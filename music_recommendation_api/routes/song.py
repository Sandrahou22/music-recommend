import os
import logging
from flask import Blueprint, request, send_file, current_app, make_response, Response
from sqlalchemy import text
from utils.response import success, error
from recommender_service import recommender_service

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
        
        # 构建查询
        engine = recommender_service._engine
        
        where_clause = ""
        if tier == 'hit':
            where_clause = "WHERE popularity_tier = 'hit'"
        elif tier == 'popular':
            where_clause = "WHERE popularity_tier = 'popular'"
        elif tier == 'normal':
            where_clause = "WHERE popularity_tier = 'normal'"
        # all 就不加条件
        
        query = text(f"""
            SELECT TOP {n}
                song_id, song_name, artists, album, genre,
                final_popularity as popularity,
                audio_path,
                CASE 
                    WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 
                    ELSE 0 
                END as has_audio
            FROM enhanced_song_features
            {where_clause}
            ORDER BY final_popularity DESC
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query)
            songs = [{
                "song_id": row.song_id,
                "song_name": row.song_name,
                "artists": row.artists,
                "album": row.album,
                "genre": row.genre,
                "popularity": int(row.popularity) if row.popularity else 50,
                "has_audio": bool(row.has_audio)
            } for row in result]
        
        logger.info(f"[热门歌曲] 返回 {len(songs)} 首歌曲，流派: {tier}")
        
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
    '流行': ['华语流行', '欧美流行', '日本流行', 'Pop'],
    '摇滚': ['Rock', 'Punk'],
    '电子': ['Electronic'],
    '金属': ['Metal'], 
    '说唱': ['Rap'],
    '其他': ['Country', 'Reggae', 'RnB', 'other']
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
        
        # 【新增】获取排序参数
        sort_by_audio = request.args.get('sort_by_audio', 'true').lower() == 'true'
        
        # 【修改】SQL查询，优先返回有音频的歌曲
        placeholders = ', '.join([f"'{g}'" for g in source_genres])
        
        # 构建基础查询
        base_query = f"""
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
        WHERE genre IN ({placeholders})
        """
        
        # 【新增】根据参数决定排序方式
        order_clause = ""
        if sort_by_audio:
            order_clause = "ORDER BY has_audio DESC, final_popularity DESC"
        else:
            order_clause = "ORDER BY final_popularity DESC"
        
        query = f"""
        {base_query}
        {order_clause}
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
        """
        
        logger.info(f"[流派查询] 排序方式: {'优先有音频' if sort_by_audio else '仅按流行度'}")
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
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
        
        # 【新增】统计信息
        audio_count = sum(1 for song in songs if song['has_audio'])
        logger.info(f"[流派查询] 返回 {len(songs)} 首歌曲，其中有音频: {audio_count} 首")
        
        return success({
            "songs": songs,
            "query_genres": source_genres,
            "count": len(songs),
            "audio_count": audio_count,
            "sort_by_audio": sort_by_audio
        })
        
    except Exception as e:
        logger.error(f"[流派查询错误] {e}", exc_info=True)
        return error(message=str(e), code=500)
    

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