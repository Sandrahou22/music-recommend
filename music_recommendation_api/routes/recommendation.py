from flask import Blueprint, request, current_app, g
from typing import Optional
from config import Config
import time

from utils.response import success, error
from recommender_service import recommender_service

bp = Blueprint('recommendation', __name__)

# 支持的算法列表
VALID_ALGORITHMS = ['hybrid', 'cf', 'content', 'mf', 'cold', 'auto', 'usercf']  # 添加 usercf

@bp.before_request
def before_request():
    """请求前置处理：记录开始时间、请求ID"""
    g.start_time = time.time()
    g.request_id = request.headers.get('X-Request-ID', 
                                       f"{time.time()}-{id(request)}")

@bp.route('/recommend/<user_id>', methods=['GET'])
def get_recommendations(user_id: str):
    """
    获取用户推荐
    
    Query Params:
        n: 推荐数量 (1-50, 默认10)
        algorithm: 算法类型 (hybrid/cf/content/mf/cold/auto, 默认hybrid)
    """
    try:
        # 1. 参数解析与验证
        n = request.args.get('n', Config.DEFAULT_RECOMMEND_COUNT, type=int)
        algorithm = request.args.get('algorithm', 'hybrid').lower()
        
        # 参数校验
        if not (1 <= n <= Config.MAX_RECOMMEND_COUNT):
            return error(
                message=f"参数n超出范围(1-{Config.MAX_RECOMMEND_COUNT})", 
                code=400
            )
        
        if algorithm not in VALID_ALGORITHMS:
            return error(
                message=f"无效算法，支持: {', '.join(VALID_ALGORITHMS)}", 
                code=400
            )
        
        # 2. 获取推荐（带错误处理）
        recs = recommender_service.get_recommendations(
            user_id=user_id,
            n=n,
            algorithm=algorithm
        )
        
        # 3. 获取用户画像（冷启动标记）
        profile = recommender_service.get_user_profile_cached(user_id)
        is_cold = recs[0].get('cold_start', False) if recs else True
        
        # 4. 计算响应时间
        elapsed = time.time() - g.get('start_time', 0)
        
        # 5. 记录日志
        current_app.logger.info(
            f"[{g.request_id}] 推荐生成成功 | user={user_id}, "
            f"n={len(recs)}, cold={is_cold}, time={elapsed:.3f}s"
        )
        
        return success({
            "user_id": user_id,
            "recommendations": recs,
            "count": len(recs),
            "user_profile": {
                "is_cold_start": is_cold,
                "n_songs": profile.get('n_songs', 0) if profile else 0,
                "top_genres": profile.get('top_genres', []) if profile else []
            },
            "algorithm_used": algorithm,
            "metadata": {
                "response_time_ms": round(elapsed * 1000, 2),
                "request_id": g.request_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        })
        
    except ValueError as ve:
        # 业务逻辑错误（如参数错误）
        current_app.logger.warning(f"[{g.request_id}] 业务错误: {ve}")
        return error(message=str(ve), code=400)
        
    except RuntimeError as re:
        # 服务未就绪
        current_app.logger.error(f"[{g.request_id}] 服务错误: {re}")
        return error(message=str(re), code=503)  # Service Unavailable
        
    except Exception as e:
        # 意外错误
        current_app.logger.exception(f"[{g.request_id}] 系统异常")
        return error(message="推荐服务内部错误", code=500)


@bp.route('/recommend/<user_id>/diverse', methods=['GET'])
def get_diverse_recommendations(user_id: str):
    """
    获取多样性推荐（MMR重排序）
    """
    try:
        n = request.args.get('n', 10, type=int)
        n = min(max(n, 1), Config.MAX_RECOMMEND_COUNT)
        
        # 获取较多候选进行重排序
        candidates = recommender_service.get_recommendations(
            user_id, 
            n=min(n * 3, 50),  # 最多取50个候选
            algorithm='hybrid'
        )
        
        # 简单重排序（实际可调用mmr_rerank）
        # 这里只是示例：按流派多样性简单重排
        sorted_recs = _simple_diversity_sort(candidates, n)
        
        return success({
            "user_id": user_id,
            "recommendations": sorted_recs,
            "diversity_optimized": True,
            "algorithm": "mmr-lite"
        })
        
    except Exception as e:
        current_app.logger.error(f"多样性推荐错误: {e}")
        return error(message=str(e), code=500)


def _simple_diversity_sort(songs: list, n: int) -> list:
    """简单的多样性重排序：确保前N首不重复流派"""
    if len(songs) <= n:
        return songs
    
    selected = []
    genres_seen = set()
    remaining = songs.copy()
    
    # 第一轮：每个流派选一首
    for song in songs:
        if len(selected) >= n:
            break
        genre = song.get('genre', 'unknown')
        if genre not in genres_seen:
            selected.append(song)
            genres_seen.add(genre)
            remaining.remove(song)
    
    # 补满剩余名额
    selected.extend(remaining[:n - len(selected)])
    return selected


@bp.route('/feedback', methods=['POST'])
def record_feedback():
    """
    记录用户反馈
    
    Body:
        {
            "user_id": "xxx",
            "song_id": "xxx", 
            "action": "click|play|skip|like",
            "context": {optional}
        }
    """
    try:
        data = request.get_json()
        if not data:
            return error(message="请求体不能为空", code=400)
        
        # 字段验证
        required = ['user_id', 'song_id', 'action']
        missing = [f for f in required if f not in data]
        if missing:
            return error(message=f"缺少必填字段: {', '.join(missing)}", code=400)
        
        # 行为类型验证
        valid_actions = ['click', 'play', 'skip', 'like', 'dislike', 'share']
        if data['action'] not in valid_actions:
            return error(
                message=f"无效行为类型，支持: {', '.join(valid_actions)}", 
                code=400
            )
        
        # 异步记录（不阻塞响应）
        success_flag = recommender_service.record_feedback(
            user_id=data['user_id'],
            song_id=data['song_id'],
            action=data['action'],
            context=data.get('context', {})
        )
        
        if success_flag:
            return success(message="反馈已记录")
        else:
            return error(message="反馈记录失败", code=500)  # 次要功能允许失败
            
    except Exception as e:
        current_app.logger.error(f"记录反馈异常: {e}")
        return error(message=str(e), code=500)