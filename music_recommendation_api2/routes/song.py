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
        n = min(n, 50)
        
        songs = recommender_service.get_hot_songs(tier=tier, n=n)
        
        if not songs:
            return success([], message="暂无热门歌曲数据")
            
        return success({
            "tier": tier,
            "count": len(songs),
            "songs": songs
        })
    except Exception as e:
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
        
        # 【关键】解析逗号分隔的多个流派
        source_genres = [g.strip() for g in genre_param.split(',') if g.strip()]
        
        limit = min(request.args.get('limit', 50, type=int), 100)
        offset = request.args.get('offset', 0, type=int)
        
        engine = recommender_service._engine
        
        # 【关键】使用 IN 语句匹配多个值
        placeholders = ', '.join([f"'{g}'" for g in source_genres])
        
        # 【调试用】打印实际执行的 SQL
        query = f"""
        SELECT 
            song_id, song_name, artists, album, genre,
            final_popularity as popularity,
            danceability, energy, valence, tempo
        FROM enhanced_song_features
        WHERE genre IN ({placeholders})
        ORDER BY final_popularity DESC
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
        """
        
        # 【关键】在日志中输出实际 SQL，方便调试
        logger.info(f"[流派查询] SQL: WHERE genre IN ({placeholders})")
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            songs = [{
                "song_id": row.song_id,
                "song_name": row.song_name,
                "artists": row.artists,
                "album": row.album,
                "genre": row.genre,
                "popularity": int(row.popularity) if row.popularity else 50,
                "audio_features": {
                    "danceability": float(row.danceability) if row.danceability else 0.5,
                    "energy": float(row.energy) if row.energy else 0.5,
                    "valence": float(row.valence) if row.valence else 0.5,
                    "tempo": float(row.tempo) if row.tempo else 120
                }
            } for row in result]
        
        logger.info(f"[流派查询] 返回 {len(songs)} 首歌曲")
        
        return success({
            "songs": songs,
            "query_genres": source_genres,  # 返回查询条件方便核对
            "count": len(songs)
        })
        
    except Exception as e:
        logger.error(f"[流派查询错误] {e}", exc_info=True)
        return error(message=str(e), code=500)
    

# ==================== 新增：音频文件服务路由 ====================

@bp.route('/<song_id>/audio', methods=['GET'])
def get_song_audio(song_id):
    try:
        engine = recommender_service._engine
        query = text("""
            SELECT audio_path, track_id, song_name
            FROM enhanced_song_features 
            WHERE song_id = :song_id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"song_id": song_id}).fetchone()
            
            if not result:
                return error(message="歌曲不存在", code=404)
            
            file_path = result.audio_path
            
            # 【添加这段调试代码】
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"[音频调试] song_id={song_id}, 原始路径={file_path}")
            
            if not file_path:
                return error(message="该歌曲暂无音频文件(audio_path为空)", code=404)
            
            # 在 song.py 的 get_song_audio 函数中，查询结果后添加：
            if file_path:
                logger.info(f"[音频调试] 歌曲 {song_id} 路径: {file_path}, 大小: {os.path.getsize(file_path)} bytes")
                # 检查是否是真实MP3（简单检查文件头）
                with open(file_path, 'rb') as f:
                    header = f.read(4)
                    is_mp3 = header[:3] == b'ID3' or header[:2] == b'\xff\xfb' or header[:2] == b'\xff\xf3'
                    logger.info(f"[音频调试] 文件头检测: {header}, 疑似MP3: {is_mp3}")

            # Windows路径处理
            file_path = os.path.normpath(str(file_path))
            logger.info(f"[音频调试] 规范化后路径={file_path}, 存在={os.path.exists(file_path)}")
            
            # 【关键修复】确保路径能被Python正确识别（特别是含中文路径）
            if not os.path.exists(file_path):
                # 尝试用原始字符串再次检查（处理反斜杠转义问题）
                raw_path = str(result.audio_path).replace('\\', '\\\\')
                logger.info(f"[音频调试] 尝试转义路径: {raw_path}")
                if not os.path.exists(file_path):
                    logger.warning(f"音频文件不存在: {file_path}")
                    return error(message=f"音频文件不存在", code=404)
            
            # 检查文件可读性
            if not os.access(file_path, os.R_OK):
                return error(message="无法读取音频文件(权限不足)", code=403)
            
        # 获取文件扩展名决定mimetype（原有逻辑）
        ext = os.path.splitext(file_path)[1].lower()
        mimetypes = {
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav',
            '.ogg': 'audio/ogg',
            '.m4a': 'audio/mp4',
            '.flac': 'audio/flac'
        }
        mimetype = mimetypes.get(ext, 'application/octet-stream')
        
        # 处理Range请求（支持拖动进度条）
        range_header = request.headers.get('Range')
        if range_header:
            response = _send_file_range(file_path, range_header, mimetype)
        else:
            # 直接返回完整文件
            response = make_response(send_file(
                file_path,
                mimetype=mimetype,
                as_attachment=False,
                download_name=os.path.basename(file_path)
            ))
        
        return response
            
    except Exception as e:
        logger.error(f"获取音频失败 [{song_id}]: {e}", exc_info=True)
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