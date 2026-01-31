from flask import Blueprint, request
from utils.response import success, error
from config import Config
from recommender_service import recommender_service

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
        tier = request.args.get('tier', 'all')  # hit/popular/normal/all
        n = request.args.get('n', 20, type=int)
        
        # 限制最大数量
        n = min(n, 50)
        
        # 调用服务层方法
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