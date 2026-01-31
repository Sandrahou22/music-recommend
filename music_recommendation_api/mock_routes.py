# mock_routes.py - 模拟数据路由
from flask import Blueprint, jsonify, request
import random
from datetime import datetime, timedelta

mock_bp = Blueprint('mock', __name__)

# 模拟流派数据
MOCK_GENRES = ['流行', '摇滚', '民谣', '电子', '嘻哈', '爵士', '古典', 'R&B', '金属', '放克', '灵魂', '乡村']

# 模拟艺术家
MOCK_ARTISTS = ['周杰伦', '林俊杰', '邓紫棋', '五月天', 'Taylor Swift', 'Ed Sheeran', 'Adele', 'Coldplay', 'Maroon 5', 'Bruno Mars']

@mock_bp.route('/api/v1/mock/songs/hot')
def mock_hot_songs():
    """模拟热门歌曲"""
    tier = request.args.get('tier', 'all')
    limit = int(request.args.get('limit', 20))
    
    songs = []
    for i in range(min(limit, 20)):
        genre = random.choice(MOCK_GENRES)
        songs.append({
            'song_id': f'mock_hot_{i+1:03d}',
            'song_name': f'{genre}歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': genre,
            'popularity': random.randint(60, 95),
            'audio_features': {
                'danceability': round(random.uniform(0.3, 0.9), 2),
                'energy': round(random.uniform(0.4, 0.95), 2),
                'valence': round(random.uniform(0.3, 0.8), 2),
                'tempo': random.randint(80, 160)
            }
        })
    
    return jsonify({"success": True, "data": {"songs": songs}})

@mock_bp.route('/api/v1/mock/songs/by-genre')
def mock_songs_by_genre():
    """模拟按流派筛选"""
    genre = request.args.get('genre', '流行')
    limit = int(request.args.get('limit', 12))
    
    songs = []
    for i in range(min(limit, 12)):
        songs.append({
            'song_id': f'mock_genre_{genre}_{i+1:03d}',
            'song_name': f'{genre}歌曲示例 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': genre,
            'popularity': random.randint(50, 90),
            'audio_features': {
                'danceability': round(random.uniform(0.3, 0.9), 2),
                'energy': round(random.uniform(0.4, 0.95), 2),
                'valence': round(random.uniform(0.3, 0.8), 2),
                'tempo': random.randint(80, 160)
            }
        })
    
    return jsonify({
        "success": True,
        "data": {
            "songs": songs,
            "pagination": {
                "page": 1,
                "limit": limit,
                "total": 50,
                "has_more": True
            }
        }
    })

@mock_bp.route('/api/v1/mock/users/<user_id>/history')
def mock_user_history(user_id):
    """模拟用户历史"""
    limit = int(request.args.get('limit', 10))
    
    history = []
    for i in range(min(limit, 10)):
        days_ago = random.randint(0, 30)
        history.append({
            'song_id': f'mock_hist_{i+1:03d}',
            'song_name': f'历史歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': random.choice(MOCK_GENRES),
            'popularity': random.randint(40, 85),
            'behavior': random.choice(['播放', '喜欢', '收藏']),
            'time_ago': f'{days_ago}天前'
        })
    
    return jsonify({
        "success": True,
        "data": {
            "user_id": user_id,
            "history": history,
            "total": len(history)
        }
    })

@mock_bp.route('/api/v1/mock/users/<user_id>/activity')
def mock_user_activity(user_id):
    """模拟用户活动"""
    limit = int(request.args.get('limit', 8))
    
    activities = []
    for i in range(min(limit, 8)):
        hours_ago = random.randint(1, 168)  # 1-168小时前
        activity_type = random.choice(['play', 'like', 'collect'])
        
        activities.append({
            'activity_id': i+1,
            'song_id': f'mock_act_{i+1:03d}',
            'song_name': f'活动歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'behavior_type': activity_type,
            'action_text': '播放了' if activity_type == 'play' else '喜欢了' if activity_type == 'like' else '收藏了',
            'icon': 'fas fa-play' if activity_type == 'play' else 'fas fa-heart' if activity_type == 'like' else 'fas fa-star',
            'time_ago': f'{hours_ago}小时前'
        })
    
    return jsonify({
        "success": True,
        "data": {
            "user_id": user_id,
            "activities": activities,
            "summary": {
                'total_activities': len(activities),
                'last_activity': '1小时前',
                'activity_by_type': {'play': 5, 'like': 2, 'collect': 1}
            },
            "period_days": 7
        }
    })

@mock_bp.route('/api/v1/mock/songs/genres')
def mock_genres():
    """模拟流派列表"""
    genres = []
    for i, genre in enumerate(MOCK_GENRES):
        genres.append({
            'genre': genre,
            'song_count': random.randint(100, 5000)
        })
    
    return jsonify({"success": True, "data": {"genres": genres}})

@mock_bp.route('/api/v1/mock/recommend/<user_id>')
def mock_recommendations(user_id):
    """模拟推荐"""
    algorithm = request.args.get('algorithm', 'hybrid')
    n = int(request.args.get('n', 10))
    
    recommendations = []
    for i in range(n):
        genre = random.choice(MOCK_GENRES)
        recommendations.append({
            'song_id': f'mock_rec_{user_id}_{i+1:03d}',
            'song_name': f'为您推荐的{genre}歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': genre,
            'popularity': random.randint(60, 95),
            'score': round(random.uniform(0.7, 0.95), 3),
            'cold_start': random.choice([True, False])
        })
    
    return jsonify({
        "success": True,
        "data": {
            "user_id": user_id,
            "algorithm": algorithm,
            "recommendations": recommendations,
            "count": len(recommendations)
        }
    })

@mock_bp.route('/api/v1/mock/health')
def mock_health():
    """模拟健康检查"""
    return jsonify({
        "status": "healthy",
        "healthy": True,
        "ready": True,
        "timestamp": datetime.now().isoformat(),
        "service": "Mock Music Recommendation API"
    })
