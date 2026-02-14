from flask import Blueprint, request
from utils.response import success, error
from sqlalchemy import text  # 添加这行
from recommender_service import recommender_service
import logging
logger = logging.getLogger(__name__)

bp = Blueprint('user', __name__)

# user.py 中的 get_user_profile 函数修正
@bp.route('/<user_id>/profile', methods=['GET'])
def get_user_profile(user_id):
    """获取用户画像（包含所有统计字段）"""
    try:
        logger.info(f"获取用户画像 | user={user_id}")
        
        # 先从推荐服务获取（内存缓存）
        profile = recommender_service.get_user_profile(user_id)
        
        if profile:
            logger.info(f"命中内存缓存 | user={user_id}")
            return success({
                "user_id": profile.get('user_id', user_id),
                "n_songs": int(profile.get('n_songs', 0)),
                "total_interactions": int(profile.get('total_interactions', 0)),
                "top_genres": profile.get('top_genres', []),
                "avg_popularity": float(profile.get('avg_popularity', 50)),
                "activity_level": profile.get('activity_level', '普通用户'),
                "diversity_ratio": float(profile.get('diversity_ratio', 0.5) if profile.get('diversity_ratio') else 0.5),
                "is_cold_start": profile.get('is_cold_start', False)
            })
        
        # 如果内存中没有，从数据库查
        logger.info(f"缓存未命中，查询数据库 | user={user_id}")
        engine = recommender_service._engine
        
        # 确保SQL字段名与数据库表结构完全匹配
        query = """
        SELECT 
            user_id,
            nickname,
            ISNULL(unique_songs, 0) as n_songs,
            ISNULL(total_interactions, 0) as total_interactions,
            ISNULL(avg_popularity_pref, 50) as avg_popularity,
            ISNULL(top_genre_1, '') as top_genre_1,
            ISNULL(top_genre_2, '') as top_genre_2,
            ISNULL(top_genre_3, '') as top_genre_3,
            ISNULL(activity_level, '普通用户') as activity_level,
            ISNULL(diversity_ratio, 0.5) as diversity_ratio
        FROM enhanced_user_features 
        WHERE user_id = :user_id
        """
        
        # 【关键修复】使用正确的连接方式
        with engine.connect() as conn:
            result = conn.execute(text(query), {"user_id": user_id}).fetchone()
            
            if result:
                genres = [g for g in [result.top_genre_1, result.top_genre_2, result.top_genre_3] if g]
                
                # 【关键修复】确保所有类型转换正确处理NULL
                n_songs = int(result.n_songs) if result.n_songs is not None else 0
                total_interactions = int(result.total_interactions) if result.total_interactions is not None else 0
                avg_popularity = float(result.avg_popularity) if result.avg_popularity is not None else 50.0
                diversity_ratio = float(result.diversity_ratio) if result.diversity_ratio is not None else 0.5
                
                logger.info(f"数据库查询成功 | user={user_id}, n_songs={n_songs}")
                
                return success({
                    "user_id": str(result.user_id),
                    "n_songs": n_songs,
                    "total_interactions": total_interactions,
                    "avg_popularity": avg_popularity,
                    "top_genres": genres,
                    "activity_level": str(result.activity_level) if result.activity_level else '普通用户',
                    "diversity_ratio": diversity_ratio,
                    "is_cold_start": n_songs < 5
                })
            else:
                logger.warning(f"用户不存在 | user={user_id}")
                return error(message="用户不存在", code=404)
                
    except Exception as e:
        logger.error(f"获取用户画像失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return error(message=str(e), code=500)

def _calculate_activity_level(n_songs):
    """根据听歌数量计算活跃等级"""
    if n_songs > 100:
        return "高活跃"
    elif n_songs > 50:
        return "中活跃"
    elif n_songs > 10:
        return "普通用户"
    else:
        return "新用户"

@bp.route('/<user_id>/history', methods=['GET'])
def get_user_history(user_id):
    """获取用户历史收听记录"""
    try:
        n = request.args.get('n', 20, type=int)
        
        engine = recommender_service._engine
        
        # SQL Server参数绑定使用 :param 而不是 %s 或 ?
        query = text("""
        SELECT TOP (:n)
            fi.song_id,
            fi.total_weight,
            fi.interaction_types,
            fi.created_at,
            ef.song_name,
            ef.artists,
            ef.genre
        FROM filtered_interactions fi
        JOIN enhanced_song_features ef ON fi.song_id = ef.song_id
        WHERE fi.user_id = :user_id
        ORDER BY fi.created_at DESC
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"user_id": user_id, "n": n})
            history = []
            for row in result:
                history.append({
                    "song_id": row.song_id,
                    "song_name": row.song_name,
                    "artists": row.artists,
                    "genre": row.genre,
                    "weight": float(row.total_weight) if row.total_weight else 0,
                    "interaction_types": row.interaction_types,
                    "timestamp": row.created_at.isoformat() if row.created_at else None
                })
            
            return success(history)
            
    except Exception as e:
        logger.error(f"获取用户历史记录失败: {e}")
        return error(message=str(e), code=500)
    
@bp.route('/register', methods=['POST'])
def register_user():
    """新用户注册"""
    try:
        data = request.get_json()
        
        # 验证必填
        if not data.get('user_id') or not data.get('nickname'):
            return error(message="用户ID和昵称为必填项", code=400)
        
        # 检查用户是否存在
        engine = recommender_service._engine
        
        with engine.connect() as conn:
            check = conn.execute(
                text("SELECT 1 FROM enhanced_user_features WHERE user_id = :id"),
                {"id": data['user_id']}
            ).fetchone()
            
            if check:
                return error(message="用户ID已存在", code=400)
        
        # 【关键修复】获取当前时间
        from datetime import datetime
        current_time = datetime.now()
        
        # 【关键修复】使用 engine.begin() 进行事务，并返回 created_at
        with engine.begin() as conn:
            insert_query = """
            INSERT INTO enhanced_user_features 
            (user_id, nickname, gender, age, province, city, source, activity_level, created_at, updated_at)
            VALUES 
            (:user_id, :nickname, :gender, :age, :province, :city, :source, '新用户', :created_at, :updated_at)
            """
            
            conn.execute(text(insert_query), {
                "user_id": data['user_id'],
                "nickname": data['nickname'],
                "gender": data.get('gender'),
                "age": data.get('age'),
                "province": data.get('province', ''),
                "city": data.get('city', ''),
                "source": data.get('source', 'internal'),
                "created_at": current_time,
                "updated_at": current_time
            })
        
        logger.info(f"新用户注册成功 | user_id={data['user_id']}")
        
        # 【关键修复】返回包含创建时间的完整用户信息
        return success({
            "message": "注册成功",
            "user": {
                "user_id": data['user_id'],
                "nickname": data['nickname'],
                "gender": data.get('gender'),
                "age": data.get('age'),
                "province": data.get('province', ''),
                "city": data.get('city', ''),
                "source": data.get('source', 'internal'),
                "created_at": current_time.isoformat(),  # 返回 ISO 格式时间
                "activity_level": "新用户"
            }
        })
        
    except Exception as e:
        logger.error(f"用户注册失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return error(message=str(e), code=500)

@bp.route('/<user_id>/recent-activities', methods=['GET'])
def get_recent_activities(user_id):
    """获取用户近期活动（播放、评论、点赞、推荐生成等）"""
    try:
        limit = min(request.args.get('limit', 20, type=int), 50)
        engine = recommender_service._engine
        
        # 联合查询播放、歌曲喜欢、评论、点赞评论、推荐生成
        query = text("""
            -- 播放行为
            SELECT 
                'play' as activity_type,
                i.song_id,
                NULL as comment_id,
                NULL as content,
                i.[timestamp] as activity_time,
                s.song_name,
                s.artists,
                NULL as extra_data
            FROM user_song_interaction i
            LEFT JOIN enhanced_song_features s ON i.song_id = s.song_id
            WHERE i.user_id = :user_id AND i.behavior_type = 'play'
            
            UNION ALL
            
            -- 歌曲喜欢
            SELECT 
                'like_song' as activity_type,
                i.song_id,
                NULL as comment_id,
                NULL as content,
                i.[timestamp] as activity_time,
                s.song_name,
                s.artists,
                NULL as extra_data
            FROM user_song_interaction i
            LEFT JOIN enhanced_song_features s ON i.song_id = s.song_id
            WHERE i.user_id = :user_id AND i.behavior_type = 'like'
            
            UNION ALL
            
            -- 评论
            SELECT 
                'comment' as activity_type,
                c.unified_song_id as song_id,
                c.comment_id,
                c.content,
                c.created_at as activity_time,
                s.song_name,
                s.artists,
                NULL as extra_data
            FROM song_comments c
            LEFT JOIN enhanced_song_features s ON c.unified_song_id = s.song_id
            WHERE c.original_user_id = :user_id
            
            UNION ALL
            
            -- 点赞评论
            SELECT 
                'like_comment' as activity_type,
                sc.unified_song_id as song_id,
                cl.comment_id,
                sc.content as content,
                cl.created_at as activity_time,
                s.song_name,
                s.artists,
                NULL as extra_data
            FROM comment_likes cl
            JOIN song_comments sc ON cl.comment_id = sc.comment_id
            LEFT JOIN enhanced_song_features s ON sc.unified_song_id = s.song_id
            WHERE cl.user_id = :user_id
            
            UNION ALL
            
            -- 推荐生成（聚合计数）
            SELECT 
                'generate_recommend' as activity_type,
                NULL as song_id,
                NULL as comment_id,
                NULL as content,
                i.[timestamp] as activity_time,
                NULL as song_name,
                NULL as artists,
                i.weight as extra_data
            FROM user_song_interaction i
            WHERE i.user_id = :user_id AND i.behavior_type = 'generate_recommend'
            
            ORDER BY activity_time DESC
            OFFSET 0 ROWS
            FETCH NEXT :limit ROWS ONLY
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"user_id": user_id, "limit": limit})
            activities = []
            for row in result:
                activities.append({
                    "type": row.activity_type,
                    "song_id": row.song_id,
                    "song_name": row.song_name,
                    "artists": row.artists,
                    "comment_id": row.comment_id,
                    "content": row.content,
                    "timestamp": row.activity_time.isoformat() if row.activity_time else None,
                    "extra_data": row.extra_data
                })
        
        logger.info(f"获取用户 {user_id} 活动记录 {len(activities)} 条")
        return success(activities)
        
    except Exception as e:
        logger.error(f"获取用户活动失败: {e}")
        return error(message=str(e), code=500)