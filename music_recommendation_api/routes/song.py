import os
import logging
from flask import Blueprint, request, send_file, current_app, make_response, Response
from sqlalchemy import text
from utils.response import success, error
from recommender_service import recommender_service
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
bp = Blueprint('song', __name__)

@bp.route('/<song_id>', methods=['GET'])
def get_song_detail(song_id):
    """获取歌曲详情"""
    try:
        song = recommender_service.get_song_details(song_id)
        if song:
            return success(song)
        else:
            return error(message="歌曲不存在", code=404)
    except Exception as e:
        return error(message=str(e), code=500)

@bp.route('/batch', methods=['POST'])
def get_songs_batch():
    """批量获取歌曲信息"""
    try:
        data = request.get_json()
        if not data or 'song_ids' not in data:
            return error(message="缺少song_ids参数", code=400)
            
        songs = []
        for sid in data['song_ids']:
            info = recommender_service.get_song_details(sid)
            if info:
                songs.append(info)
                
        return success(songs)
    except Exception as e:
        return error(message=str(e), code=500)

@bp.route('/hot', methods=['GET'])
def get_hot_songs():
    """获取热门歌曲（分层）"""
    try:
        tier = request.args.get('tier', 'all')
        n = request.args.get('n', 20, type=int)
        n = min(n, 100)
        
        engine = recommender_service._engine
        
        where_clause = ""
        if tier == 'hit':
            where_clause = "WHERE popularity_tier = 'hit'"
        elif tier == 'popular':
            where_clause = "WHERE popularity_tier = 'popular'"
        elif tier == 'normal':
            where_clause = "WHERE popularity_tier = 'normal'"
        
        query = text(f"""
            SELECT TOP {n}
                song_id, song_name, artists, album, 
                COALESCE(genre_clean, genre) as genre,
                COALESCE(final_popularity, popularity, 50) as popularity,  -- 使用真实流行度
                popularity_tier,
                audio_path,
                CASE 
                    WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 
                    ELSE 0 
                END as has_audio
            FROM enhanced_song_features
            {where_clause}
            ORDER BY COALESCE(final_popularity, popularity, 50) DESC  -- 按真实流行度排序
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query)
            songs = []
            for row in result:
                # 确保流行度是整数
                popularity = int(row.popularity) if row.popularity else 50
                songs.append({
                    "song_id": row.song_id,
                    "song_name": row.song_name,
                    "artists": row.artists,
                    "album": row.album,
                    "genre": row.genre,
                    "popularity": popularity,  # 真实流行度
                    "popularity_tier": row.popularity_tier,
                    "has_audio": bool(row.has_audio)
                })
        
        logger.info(f"[热门歌曲] tier={tier}, 返回 {len(songs)} 首歌曲，平均流行度: {sum(s['popularity'] for s in songs)/len(songs) if songs else 0:.1f}")
        
        return success({
            "tier": tier,
            "count": len(songs),
            "songs": songs
        })
        
    except Exception as e:
        logger.error(f"获取热门歌曲失败: {e}", exc_info=True)
        return error(message=str(e), code=500)

# 极简流派归一化映射（与前端一致）
GENRE_NORMALIZATION = {
    '流行': ['华语流行', '欧美流行', '日本流行', 'Pop', 'K-Pop'],
    '摇滚': ['Rock', 'Punk', '摇滚'],
    '电子': ['Electronic', '电子'],
    '金属': ['Metal'],
    '说唱': ['Rap', '说唱'],
    '民谣': ['Folk', '民谣', 'Country'],
    '其他': ['Jazz', 'Blues', 'Latin', 'New Age', 'World', 'Reggae', 'RnB', '翻唱', '现场', '影视原声']
}

@bp.route('/by-genre', methods=['GET'])
def get_songs_by_genre():
    """按流派查询（支持多值匹配）"""
    try:
        genre_param = request.args.get('genre', '').strip()
        if not genre_param:
            return error(message="缺少流派参数", code=400)
        
        # 解析逗号分隔的多个流派
        source_genres = [g.strip() for g in genre_param.split(',') if g.strip()]
        
        limit = min(request.args.get('limit', 50, type=int), 100)
        offset = request.args.get('offset', 0, type=int)
        
        engine = recommender_service._engine
        
        # 【关键修复】使用更灵活的查询条件
        # 构建 WHERE 子句
        conditions = []
        params = {}
        
        for i, g in enumerate(source_genres):
            param_name = f"genre_{i}"
            params[param_name] = f"%{g}%"
            # 同时在 genre 和 genre_clean 中搜索
            conditions.append(f"(genre LIKE :{param_name} OR genre_clean LIKE :{param_name})")
        
        where_clause = " OR ".join(conditions) if conditions else "1=1"
        
        # 构建查询
        query_str = f"""
        SELECT 
            song_id, song_name, artists, album, 
            COALESCE(genre_clean, genre) as genre,
            COALESCE(final_popularity, popularity, 50) as popularity,
            danceability, energy, valence, tempo,
            audio_path,
            track_id,
            CASE 
                WHEN (audio_path IS NOT NULL AND audio_path != '') OR track_id IS NOT NULL THEN 1 
                ELSE 0 
            END as has_audio,
            popularity_tier
        FROM enhanced_song_features
        WHERE {where_clause}
        ORDER BY 
            CASE 
                WHEN (audio_path IS NOT NULL AND audio_path != '') OR track_id IS NOT NULL THEN 0 
                ELSE 1 
            END,
            COALESCE(final_popularity, popularity, 50) DESC
        OFFSET :offset ROWS
        FETCH NEXT :limit ROWS ONLY
        """
        
        params["offset"] = offset
        params["limit"] = limit
        
        logger.info(f"[流派查询] 查询条件: {where_clause}, 参数: {params}")
        
        with engine.connect() as conn:
            result = conn.execute(text(query_str), params)
            songs = []
            for row in result:
                songs.append({
                    "song_id": row.song_id,
                    "song_name": row.song_name or "未知歌曲",
                    "artists": row.artists or "未知艺术家",
                    "album": row.album or "",
                    "genre": row.genre or "未知流派",
                    "popularity": int(row.popularity) if row.popularity else 50,
                    "has_audio": bool(row.has_audio),
                    "popularity_tier": row.popularity_tier,
                    "audio_features": {
                        "danceability": float(row.danceability) if row.danceability else 0.5,
                        "energy": float(row.energy) if row.energy else 0.5,
                        "valence": float(row.valence) if row.valence else 0.5,
                        "tempo": float(row.tempo) if row.tempo else 120
                    }
                })
        
        # 获取总数（使用相同的条件）
        count_query = f"""
        SELECT COUNT(*) as total
        FROM enhanced_song_features
        WHERE {where_clause}
        """
        
        with engine.connect() as conn:
            count_result = conn.execute(text(count_query), {k: v for k, v in params.items() if not k.startswith('offset') and not k.startswith('limit')})
            total = count_result.scalar() or 0
        
        audio_count = sum(1 for song in songs if song['has_audio'])
        logger.info(f"[流派查询] 返回 {len(songs)}/{total} 首歌曲，有音频: {audio_count} 首")
        
        return success({
            "songs": songs,
            "query_genres": source_genres,
            "count": len(songs),
            "total": total,
            "audio_count": audio_count,
            "has_more": (offset + len(songs)) < total
        })
        
    except Exception as e:
        logger.error(f"[流派查询错误] {e}", exc_info=True)
        # 返回空结果而不是错误，避免前端崩溃
        return success({
            "songs": [],
            "query_genres": source_genres,
            "count": 0,
            "total": 0,
            "audio_count": 0,
            "has_more": False,
            "warning": str(e)
        })
    

# ==================== 新增：音频文件服务路由 ====================

@bp.route('/<song_id>/audio', methods=['GET'])
def get_song_audio(song_id):
    try:
        logger.info(f"[音频请求] 开始处理歌曲ID: {song_id}")
        
        # 获取歌曲信息
        engine = recommender_service._engine
        query = text("""
            SELECT audio_path, track_id, song_name, 
                   CASE 
                     WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 
                     ELSE 0 
                   END as has_audio
            FROM enhanced_song_features 
            WHERE song_id = :song_id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"song_id": song_id}).fetchone()
            
            if not result:
                logger.warning(f"[音频请求] 歌曲不存在: {song_id}")
                return error(message="歌曲不存在", code=404)
            
            # 检查是否有音频
            if not result.has_audio:
                logger.info(f"[音频请求] 歌曲无音频: {song_id}")
                return error(message="该歌曲暂无音频文件", code=404)
            
            file_path = result.audio_path
            
            # 详细的路径检查和日志
            logger.info(f"[音频请求] 原始路径: {file_path}")
            
            if not file_path or file_path == '':
                logger.warning(f"[音频请求] 音频路径为空: {song_id}")
                return error(message="音频文件路径为空", code=404)
            
            # 处理路径（Windows）
            file_path = str(file_path).strip()
            
            # 检查文件是否存在
            if not os.path.exists(file_path):
                # 尝试多种路径格式
                possible_paths = [
                    file_path,
                    file_path.replace('\\', '/'),
                    file_path.replace('/', '\\'),
                    os.path.normpath(file_path),
                    os.path.abspath(file_path)
                ]
                
                for i, path in enumerate(possible_paths):
                    logger.info(f"[音频请求] 尝试路径 {i+1}: {path}")
                    if os.path.exists(path):
                        file_path = path
                        logger.info(f"[音频请求] 找到文件: {path}")
                        break
                else:
                    logger.error(f"[音频请求] 所有路径都不存在: {song_id}")
                    return error(message=f"音频文件不存在于任何路径: {file_path}", code=404)
            
            # 检查文件可读性
            if not os.access(file_path, os.R_OK):
                logger.error(f"[音频请求] 文件不可读: {file_path}")
                return error(message="音频文件不可读", code=403)
            
            file_size = os.path.getsize(file_path)
            logger.info(f"[音频请求] 文件大小: {file_size} bytes, 路径: {file_path}")
        
        # 设置mimetype
        ext = os.path.splitext(file_path)[1].lower()
        mimetypes = {
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav',
            '.ogg': 'audio/ogg',
            '.m4a': 'audio/mp4',
            '.flac': 'audio/flac'
        }
        mimetype = mimetypes.get(ext, 'application/octet-stream')
        
        # 处理Range请求（支持进度条拖动）
        range_header = request.headers.get('Range')
        
        if range_header:
            logger.info(f"[音频请求] Range请求: {range_header}")
            response = _send_file_range(file_path, range_header, mimetype)
        else:
            # 直接返回完整文件
            logger.info(f"[音频请求] 完整文件请求")
            response = make_response(send_file(
                file_path,
                mimetype=mimetype,
                as_attachment=False,
                download_name=f"{song_id}_{os.path.basename(file_path)}"
            ))
            response.headers['Content-Length'] = str(file_size)
        
        # 添加缓存头，避免重复请求
        response.headers['Cache-Control'] = 'public, max-age=3600'
        response.headers['Accept-Ranges'] = 'bytes'
        
        logger.info(f"[音频请求] 成功处理: {song_id}, 大小: {file_size}")
        return response
            
    except Exception as e:
        logger.error(f"[音频请求] 处理失败 [{song_id}]: {e}", exc_info=True)
        return error(message=f"音频服务错误: {str(e)}", code=500)


def _send_file_range(file_path, range_header, mimetype):
    """处理HTTP Range请求，支持音频拖动"""
    try:
        file_size = os.path.getsize(file_path)
        byte_range = range_header.replace('bytes=', '').split('-')
        start = int(byte_range[0]) if byte_range[0] else 0
        end = int(byte_range[1]) if byte_range[1] else file_size - 1
        
        if start >= file_size or end >= file_size or start > end:
            return error(message="Invalid Range", code=416)
        
        length = end - start + 1
        
        with open(file_path, 'rb') as f:
            f.seek(start)
            data = f.read(length)
        
        response = Response(
            data,
            206,
            mimetype=mimetype,
            direct_passthrough=False
        )
        response.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
        response.headers.add('Accept-Ranges', 'bytes')
        response.headers.add('Content-Length', str(length))
        
        # 【关键】添加CORS头
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Expose-Headers', 'Content-Range, Content-Length')
        
        return response
        
    except Exception as e:
        logger.error(f"Range请求处理失败: {e}")
        # 出错时回退到完整文件，但也要加CORS头
        response = make_response(send_file(file_path, mimetype=mimetype))
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    
@bp.route('/by-genre/stats', methods=['GET'])
def get_genre_stats():
    """获取各流派的统计信息（精简版）"""
    try:
        engine = recommender_service._engine
        
        query = """
        SELECT 
            genre,
            COUNT(*) as song_count
        FROM enhanced_song_features
        WHERE genre IS NOT NULL AND genre <> ''
        GROUP BY genre
        ORDER BY song_count DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            genres = []
            for row in result:
                genres.append({
                    "name": row.genre,
                    "count": int(row.song_count)
                })
        
        return success({
            "genres": genres,
            "total_songs": sum(g['count'] for g in genres)
        })
        
    except Exception as e:
        logger.error(f"获取流派统计失败: {e}")
        return error(message=str(e), code=500)
    
# song.py 中添加

@bp.route('/<song_id>/audio/status', methods=['GET', 'OPTIONS'])
def get_audio_status(song_id):
    """检查音频文件是否存在（简化版）"""
    try:
        engine = recommender_service._engine
        query = text("""
            SELECT 
                CASE WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 ELSE 0 END as has_audio,
                track_id, song_name
            FROM enhanced_song_features 
            WHERE song_id = :song_id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"song_id": song_id}).fetchone()
            
            if not result:
                return error(message="歌曲不存在", code=404)
            
            has_audio = bool(result.has_audio)
            file_path = None
            
            if has_audio:
                # 尝试获取实际路径
                path_query = text("SELECT audio_path FROM enhanced_song_features WHERE song_id = :song_id")
                path_result = conn.execute(path_query, {"song_id": song_id}).fetchone()
                file_path = path_result.audio_path if path_result else None
                
                # 检查文件是否存在
                if file_path and os.path.exists(str(file_path)):
                    file_size = os.path.getsize(str(file_path))
                else:
                    has_audio = False
                    file_path = None
            
            return success({
                "song_id": song_id,
                "has_audio": has_audio,
                "file_path": file_path,
                "file_exists": has_audio and file_path and os.path.exists(str(file_path))
            })
            
    except Exception as e:
        logger.error(f"检查音频状态失败 [{song_id}]: {e}", exc_info=True)
        return success({
            "song_id": song_id,
            "has_audio": False,
            "error": str(e)
        })
    
# 在song.py中添加搜索API
# 在song.py的bp路由中添加
@bp.route('/search', methods=['GET'])
def search_songs():
    """搜索歌曲（支持歌曲名、艺术家、专辑、流派）"""
    try:
        query = request.args.get('q', '').strip()
        if not query or len(query) < 2:
            return error(message="请输入至少2个字符进行搜索", code=400)
        
        limit = min(request.args.get('limit', 50, type=int), 100)
        offset = request.args.get('offset', 0, type=int)
        
        engine = recommender_service._engine
        
        # 构建搜索查询
        search_pattern = f"%{query}%"
        
        query_sql = text("""
            SELECT 
                song_id, song_name, artists, album, genre,
                final_popularity as popularity,
                danceability, energy, valence, tempo,
                audio_path,
                CASE 
                    WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 
                    ELSE 0 
                END as has_audio
            FROM enhanced_song_features
            WHERE 
                song_name LIKE :pattern OR
                artists LIKE :pattern OR
                album LIKE :pattern OR
                genre LIKE :pattern
            ORDER BY 
                final_popularity DESC
            OFFSET :offset ROWS
            FETCH NEXT :limit ROWS ONLY
        """)
        
        with engine.connect() as conn:
            result = conn.execute(
                query_sql, 
                {"pattern": search_pattern, "offset": offset, "limit": limit}
            )
            
            songs = [{
                "song_id": row.song_id,
                "song_name": row.song_name,
                "artists": row.artists,
                "album": row.album,
                "genre": row.genre,
                "popularity": int(row.popularity) if row.popularity else 50,
                "has_audio": bool(row.has_audio),
                "audio_features": {
                    "danceability": float(row.danceability) if row.danceability else 0.5,
                    "energy": float(row.energy) if row.energy else 0.5,
                    "valence": float(row.valence) if row.valence else 0.5,
                    "tempo": float(row.tempo) if row.tempo else 120
                }
            } for row in result]
        
        logger.info(f"[搜索] 查询: '{query}'，返回 {len(songs)} 个结果")
        
        return success({
            "query": query,
            "songs": songs,
            "count": len(songs)
        })
        
    except Exception as e:
        logger.error(f"[搜索错误] {e}", exc_info=True)
        return error(message=f"搜索失败: {str(e)}", code=500)
    
@bp.route('/hot/stats', methods=['GET'])
def get_hot_stats():
    """获取热门歌曲分类统计"""
    try:
        engine = recommender_service._engine
        
        query = text("""
            SELECT 
                popularity_tier,
                COUNT(*) as count,
                AVG(final_popularity) as avg_popularity
            FROM enhanced_song_features
            WHERE popularity_tier IS NOT NULL
            GROUP BY popularity_tier
            ORDER BY 
                CASE popularity_tier
                    WHEN 'hit' THEN 1
                    WHEN 'popular' THEN 2
                    WHEN 'normal' THEN 3
                    ELSE 4
                END
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query)
            stats = []
            for row in result:
                stats.append({
                    "tier": row.popularity_tier,
                    "count": row.count,
                    "avg_popularity": float(row.avg_popularity) if row.avg_popularity else 0
                })
        
        return success({
            "stats": stats,
            "total": sum(s['count'] for s in stats)
        })
        
    except Exception as e:
        logger.error(f"获取热门统计失败: {e}")
        return error(message=str(e), code=500)
    
@bp.route('/stats', methods=['GET'])
def get_system_stats():
    """获取系统统计数据"""
    try:
        engine = recommender_service._engine
        
        # 1. 活跃用户数（最近30天有交互的）
        user_count_query = text("""
            SELECT COUNT(DISTINCT user_id) as active_users
            FROM filtered_interactions
            WHERE created_at >= DATEADD(day, -30, GETDATE())
        """)
        
        # 2. 歌曲总数和音频数
        song_count_query = text("""
            SELECT 
                COUNT(*) as total_songs,
                SUM(CASE WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 ELSE 0 END) as audio_songs
            FROM enhanced_song_features
        """)
        
        # 3. 今日推荐数
        today_recommend_query = text("""
            SELECT COUNT(*) as today_recommends
            FROM recommendations
            WHERE CONVERT(DATE, created_at) = CONVERT(DATE, GETDATE())
        """)
        
        with engine.connect() as conn:
            # 活跃用户数
            user_result = conn.execute(user_count_query).fetchone()
            active_users = user_result.active_users if user_result else 43355
            
            # 歌曲总数
            song_result = conn.execute(song_count_query).fetchone()
            total_songs = song_result.total_songs if song_result else 16588
            audio_songs = song_result.audio_songs if song_result else total_songs // 2
            
            # 今日推荐数
            recommend_result = conn.execute(today_recommend_query).fetchone()
            today_recommends = recommend_result.today_recommends if recommend_result else 500
        
        return success({
            "active_users": active_users,
            "total_songs": total_songs,
            "audio_songs": audio_songs,
            "today_recommends": today_recommends,
            "last_updated": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取系统统计失败: {e}")
        return error(message=str(e), code=500)