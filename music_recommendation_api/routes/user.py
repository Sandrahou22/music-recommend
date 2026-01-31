from flask import Blueprint, request
from utils.response import success, error
from config import Config
from recommender_service import recommender_service

bp = Blueprint('user', __name__)

@bp.route('/<user_id>/profile', methods=['GET'])
def get_user_profile(user_id):
    """获取用户画像"""
    try:
        profile = recommender_service.get_user_profile(user_id)
        if profile:
            return success(profile)
        else:
            # 用户不存在，返回默认画像
            return success({
                "user_id": user_id,
                "n_songs": 0,
                "top_genres": [],
                "popularity_bias": 0,
                "avg_popularity": 50,
                "is_cold_start": True
            })
    except Exception as e:
        return error(message=str(e), code=500)

@bp.route('/<user_id>/history', methods=['GET'])
def get_user_history(user_id):
    """获取用户历史收听"""
    try:
        n = request.args.get('n', 10, type=int)
        # 这里可以调用推荐服务的获取历史方法
        # 暂时返回空列表
        return success([])
    except Exception as e:
        return error(message=str(e), code=500)