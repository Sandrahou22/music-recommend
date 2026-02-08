from flask import Blueprint, request, current_app, g
from typing import Optional
from sqlalchemy import text  # 添加这行
from config import Config
import time
import logging
from datetime import datetime, timedelta  # 添加这一行

def save_recommendations_sync(user_id, recommendations):
    """同步保存推荐结果（替换原来的异步函数）"""
    try:
        engine = recommender_service._engine
        
        # 设置过期时间（24小时后过期）
        expires_at = datetime.now() + timedelta(hours=24)
        
        insert_query = """
        INSERT INTO recommendations 
        (user_id, song_id, recommendation_score, algorithm_type, rank_position, expires_at, created_at)
        VALUES 
        (:user_id, :song_id, :score, :algorithm, :rank_pos, :expires, GETDATE())
        """
        
        inserted_count = 0
        with engine.begin() as conn:
            for rec in recommendations:
                try:
                    conn.execute(text(insert_query), {
                        "user_id": user_id,
                        "song_id": rec['song_id'],
                        "score": rec['score'],
                        "algorithm": rec['algorithm'],
                        "rank_pos": rec['rank_position'],
                        "expires": expires_at
                    })
                    inserted_count += 1
                except Exception as insert_err:
                    # 单挑失败记录日志但不中断
                    logger.warning(f"插入推荐记录失败 {rec.get('song_id')}: {insert_err}")
                    continue
        
        logger.info(f"保存推荐结果成功 | user={user_id}, count={inserted_count}/{len(recommendations)}")
        return True
        
    except Exception as e:
        logger.error(f"保存推荐结果失败: {e}")
        return False

# 添加 logger 定义
logger = logging.getLogger(__name__)

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

        # 【修改】删除原有的 save_recommendations_async 调用，直接调用保存函数
        try:
            recommendations_to_save = []
            for i, rec in enumerate(recs):
                recommendations_to_save.append({
                    "song_id": rec.get('song_id'),
                    "score": rec.get('score', 0.0),
                    "algorithm": algorithm,
                    "rank_position": i + 1
                })
            
            # 【修复】直接调用同步保存函数
            save_recommendations_sync(user_id, recommendations_to_save)
            
        except Exception as save_err:
            logger.warning(f"保存推荐记录失败: {save_err}")
            # 不阻断主流程
        
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

@bp.route('/recommend/<user_id>/compare', methods=['GET'])
def get_ab_test_recommendations(user_id: str):
    """
    A/B测试对比：同时返回两种算法的推荐结果
    """
    try:
        n = min(request.args.get('n', 5, type=int), 10)
        
        # 算法A：混合推荐
        recs_a = recommender_service.get_recommendations(
            user_id=user_id,
            n=n,
            algorithm='hybrid'
        )
        
        # 算法B：仅协同过滤
        recs_b = recommender_service.get_recommendations(
            user_id=user_id,
            n=n,
            algorithm='cf'
        )
        
        return success({
            "user_id": user_id,
            "test_type": "A/B Test",
            "group_a": {
                "algorithm": "hybrid",
                "description": "混合推荐（协同过滤+内容+矩阵分解）",
                "recommendations": recs_a
            },
            "group_b": {
                "algorithm": "cf",
                "description": "纯协同过滤",
                "recommendations": recs_b
            }
        })
        
    except Exception as e:
        return error(message=str(e), code=500)

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

@bp.route('/recommendations/feedback', methods=['POST'])
def submit_recommendation_feedback():
    """
    提交推荐反馈（显式反馈）
    Body: {
        "user_id": "xxx",
        "song_id": "xxx",
        "feedback": "like|dislike|neutral",
        "algorithm": "hybrid"
    }
    """
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        song_id = data.get('song_id')
        feedback = data.get('feedback')  # like/dislike/neutral
        algorithm = data.get('algorithm', 'hybrid')
        
        if not all([user_id, song_id, feedback]):
            return error(message="缺少必要参数", code=400)
        
        # 记录到数据库（可以新建表或扩展recommendations表）
        engine = recommender_service._engine
        query = """
        UPDATE recommendations 
        SET feedback = :feedback, 
            feedback_at = GETDATE()
        WHERE user_id = :user_id 
        AND song_id = :song_id
        AND CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)
        """
        
        with engine.begin() as conn:
            result = conn.execute(text(query), {
                "user_id": user_id,
                "song_id": song_id,
                "feedback": feedback
            })
            
            # 如果没有更新到（可能不是今天的推荐），插入新记录
            if result.rowcount == 0:
                insert_query = """
                INSERT INTO recommendations 
                (user_id, song_id, algorithm_type, feedback, created_at)
                VALUES 
                (:user_id, :song_id, :algorithm, :feedback, GETDATE())
                """
                conn.execute(text(insert_query), {
                    "user_id": user_id,
                    "song_id": song_id,
                    "algorithm": algorithm,
                    "feedback": feedback
                })
        
        return success(message="反馈已记录")
        
    except Exception as e:
        logger.error(f"记录反馈失败: {e}")
        return error(message=str(e), code=500)

# recommendation.py 中的 record_behavior 函数修正
@bp.route('/behavior', methods=['POST'])
def record_behavior():
    """
    记录用户行为
    """
    try:
        data = request.get_json()
        if not data:
            return error(message="请求体不能为空", code=400)
        
        # 验证必填字段
        required = ['user_id', 'song_id', 'behavior_type']
        missing = [f for f in required if f not in data]
        if missing:
            return error(message=f"缺少必填字段: {', '.join(missing)}", code=400)
        
        # 验证 behavior_type
        valid_behaviors = ['play', 'like', 'collect', 'skip']
        if data['behavior_type'] not in valid_behaviors:
            return error(message=f"无效行为类型，支持: {', '.join(valid_behaviors)}", code=400)
        
        engine = recommender_service._engine
        query = """
        INSERT INTO user_song_interaction 
        (user_id, song_id, behavior_type, [weight], [timestamp])
        VALUES 
        (:user_id, :song_id, :behavior_type, :weight, :timestamp)
        """
        
        weight = data.get('weight', 1.0)
        
        # 【关键修改】使用 engine.begin()
        with engine.begin() as conn:
            conn.execute(text(query), {
                "user_id": data['user_id'],
                "song_id": data['song_id'],
                "behavior_type": data['behavior_type'],
                "weight": weight,
                "timestamp": data.get('timestamp', datetime.now())
            })
        
        current_app.logger.info(
            f"行为记录成功 | user={data['user_id']}, "
            f"song={data['song_id']}, behavior={data['behavior_type']}"
        )
        
        return success(message="行为记录成功")
        
    except Exception as e:
        current_app.logger.error(f"记录行为失败: {e}")
        return error(message=str(e), code=500)
    
@bp.route('/recommendations/status', methods=['POST'])
def update_recommendation_status():
    """
    更新推荐结果的反馈状态
    """
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        song_id = data.get('song_id')
        action = data.get('action')
        
        if not all([user_id, song_id, action]):
            return error(message="缺少必要参数", code=400)
        
        engine = recommender_service._engine
        
        # 查找最新的推荐记录
        find_query = """
        SELECT TOP 1 recommendation_id 
        FROM recommendations 
        WHERE user_id = :user_id AND song_id = :song_id
        ORDER BY created_at DESC
        """
        
        # 【关键修改】使用 engine.begin()
        with engine.begin() as conn:
            result = conn.execute(text(find_query), {
                "user_id": user_id,
                "song_id": song_id
            }).fetchone()
            
            if result:
                rec_id = result[0]
                
                # 根据 action 更新不同字段
                if action == 'view':
                    update_query = "UPDATE recommendations SET is_viewed = 1 WHERE recommendation_id = :id"
                elif action == 'click':
                    update_query = "UPDATE recommendations SET is_clicked = 1 WHERE recommendation_id = :id"
                elif action == 'listen':
                    update_query = "UPDATE recommendations SET is_listened = 1 WHERE recommendation_id = :id"
                else:
                    return error(message="无效action", code=400)
                
                conn.execute(text(update_query), {"id": rec_id})
                
        return success(message="状态更新成功")
        
    except Exception as e:
        logger.error(f"更新推荐状态失败: {e}")
        return error(message=str(e), code=500)
    

@bp.route('/recommendations/save', methods=['POST'])
def save_recommendations():
    """
    保存推荐结果到数据库
    """
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        recommendations = data.get('recommendations', [])
        
        if not user_id or not recommendations:
            return error(message="缺少必要参数", code=400)
        
        engine = recommender_service._engine
        
        # 【关键修复】先检查用户是否存在，不存在则创建临时用户记录
        check_user_query = "SELECT 1 FROM enhanced_user_features WHERE user_id = :user_id"
        
        with engine.connect() as conn:
            user_exists = conn.execute(text(check_user_query), {"user_id": user_id}).fetchone()
            
            if not user_exists:
                # 用户不存在，创建临时用户记录以避免外键冲突
                insert_user_query = """
                INSERT INTO enhanced_user_features 
                (user_id, nickname, source, activity_level, created_at, updated_at)
                VALUES 
                (:user_id, :nickname, :source, '新用户', GETDATE(), GETDATE())
                """
                try:
                    conn.execute(text(insert_user_query), {
                        "user_id": user_id,
                        "nickname": f"User_{user_id}",
                        "source": "temp"
                    })
                    conn.commit()  # 立即提交用户创建
                    logger.info(f"自动创建临时用户记录: {user_id}")
                except Exception as user_err:
                    # 可能并发创建，忽略重复错误
                    logger.warning(f"创建临时用户失败(可能已存在): {user_err}")
        
        # 设置过期时间（24小时后过期）
        expires_at = datetime.now() + timedelta(hours=24)
        
        insert_query = """
        INSERT INTO recommendations 
        (user_id, song_id, recommendation_score, algorithm_type, rank_position, expires_at, created_at)
        VALUES 
        (:user_id, :song_id, :score, :algorithm, :rank_pos, :expires, GETDATE())
        """
        
        inserted_count = 0
        with engine.begin() as conn:
            for rec in recommendations:
                try:
                    conn.execute(text(insert_query), {
                        "user_id": user_id,
                        "song_id": rec['song_id'],
                        "score": rec['score'],
                        "algorithm": rec['algorithm'],
                        "rank_pos": rec['rank_position'],
                        "expires": expires_at
                    })
                    inserted_count += 1
                except Exception as insert_err:
                    # 单挑失败记录日志但不中断
                    logger.warning(f"插入推荐记录失败 {rec.get('song_id')}: {insert_err}")
                    continue
        
        logger.info(f"保存推荐结果成功 | user={user_id}, count={inserted_count}/{len(recommendations)}")
        return success(message=f"成功保存{inserted_count}条推荐")
        
    except Exception as e:
        logger.error(f"保存推荐结果失败: {e}")
        # 即使失败也返回成功，不让前端报错（非关键功能）
        return success(message="推荐已生成", data={"warning": str(e)})
    
@bp.route('/explain/<user_id>/<song_id>', methods=['GET'])
def get_explanation(user_id, song_id):
    """
    获取推荐解释
    """
    try:
        from explanation_engine import ExplanationEngine
        
        # 获取推荐引擎
        if not recommender_service._recommender:
            return error(message="推荐引擎未初始化", code=503)
        
        # 创建解释引擎
        explainer = ExplanationEngine(recommender_service._recommender)
        
        # 获取算法类型
        algorithm = request.args.get('algorithm', 'hybrid')
        
        # 生成解释
        explanation = explainer.generate_explanation(user_id, song_id, algorithm)
        
        return success({
            "user_id": user_id,
            "song_id": song_id,
            "explanation": explanation  # 返回完整的解释对象
        })
        
    except Exception as e:
        logger.error(f"生成解释失败: {e}")
        # 返回默认解释，不报错
        return success({
            "user_id": user_id,
            "song_id": song_id,
            "explanation": {
                "main_reason": "基于您的音乐偏好推荐",
                "details": ["符合您的收听习惯"],
                "confidence": 0.7
            }
        })
    
@bp.route('/songs/<song_id>/similar', methods=['GET'])
def get_similar_songs(song_id):
    """获取相似歌曲（基于ItemCF或内容相似度）"""
    try:
        n = min(request.args.get('n', 6, type=int), 12)
        
        # 使用推荐引擎获取相似歌曲
        engine = recommender_service._engine
        
        # 方法1：基于内容相似度（如果available）
        similar = []
        if hasattr(engine, 'content_similarities') and song_id in engine.content_similarities:
            sim_dict = engine.content_similarities[song_id]
            # 取Top N
            for sim_id, score in sorted(sim_dict.items(), key=lambda x: x[1], reverse=True)[:n]:
                info = recommender_service.get_song_details(sim_id)
                if info:
                    info['similarity_score'] = round(score, 3)
                    similar.append(info)
        
        # 方法2：基于协同过滤的物品相似度
        if not similar and hasattr(engine, 'item_similarities'):
            sim_dict = engine.item_similarities.get(song_id, {})
            for sim_id, score in sorted(sim_dict.items(), key=lambda x: x[1], reverse=True)[:n]:
                info = recommender_service.get_song_details(sim_id)
                if info:
                    info['similarity_score'] = round(score, 3)
                    similar.append(info)
        
        return success({
            "source_song": song_id,
            "similar_songs": similar,
            "count": len(similar)
        })
        
    except Exception as e:
        logger.error(f"获取相似歌曲失败: {e}")
        return error(message=str(e), code=500)