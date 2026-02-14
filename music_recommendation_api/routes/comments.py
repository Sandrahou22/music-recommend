# routes/comments.py
from flask import Blueprint, request, jsonify, current_app
from sqlalchemy import text, func, desc
from datetime import datetime, timedelta
import logging
from utils.response import success, error
from recommender_service import recommender_service
import re  # 添加正则表达式支持

logger = logging.getLogger(__name__)
bp = Blueprint('comments', __name__)

try:
    from snownlp import SnowNLP
    HAS_SNOWNLP = True
    logger.info("SnowNLP 已加载，使用SnowNLP进行情感分析")
except ImportError:
    HAS_SNOWNLP = False
    logger.warning("SnowNLP 未安装，将使用简单情感分析")

# ==================== 歌曲评论API ====================

@bp.route('/songs/<song_id>/comments', methods=['GET'])
def get_song_comments(song_id):
    """获取歌曲的所有评论（分页+排序）"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 20, type=int), 100)
        sort_by = request.args.get('sort_by', 'comment_time')  # comment_time, liked_count
        sort_order = request.args.get('order', 'desc')  # asc, desc
        
        offset = (page - 1) * per_page
        
        engine = recommender_service._engine
        
        # 验证歌曲是否存在
        song_check = engine.execute(
            text("SELECT 1 FROM enhanced_song_features WHERE song_id = :song_id"),
            {"song_id": song_id}
        ).fetchone()
        
        if not song_check:
            return error(message="歌曲不存在", code=404)
        
        # 构建排序
        order_map = {
            'comment_time': 'comment_time',
            'liked_count': 'liked_count',
            'sentiment': 'sentiment_score'
        }
        order_field = order_map.get(sort_by, 'comment_time')
        order_direction = 'DESC' if sort_order.lower() == 'desc' else 'ASC'
        
        # 获取评论数据
        query = text(f"""
            SELECT 
                comment_id,
                unified_song_id,
                original_user_id,
                user_nickname,
                content,
                liked_count,
                comment_time,
                sentiment_score,
                is_positive,
                created_at
            FROM song_comments
            WHERE unified_song_id = :song_id
            ORDER BY {order_field} {order_direction}
            OFFSET :offset ROWS
            FETCH NEXT :limit ROWS ONLY
        """)
        
        with engine.connect() as conn:
            result = conn.execute(
                query,
                {"song_id": song_id, "offset": offset, "limit": per_page}
            )
            
            comments = []
            for row in result:
                comments.append({
                    "comment_id": row.comment_id,
                    "user_id": row.original_user_id,
                    "user_nickname": row.user_nickname or "匿名用户",
                    "content": row.content,
                    "liked_count": row.liked_count or 0,
                    "comment_time": row.comment_time.isoformat() if row.comment_time else None,
                    "sentiment_score": float(row.sentiment_score) if row.sentiment_score else 0,
                    "is_positive": bool(row.is_positive) if row.is_positive is not None else None,
                    "created_at": row.created_at.isoformat() if row.created_at else None
                })
            
            # 获取总数
            count_query = text("""
                SELECT COUNT(*) as total 
                FROM song_comments 
                WHERE unified_song_id = :song_id
            """)
            total = conn.execute(count_query, {"song_id": song_id}).fetchone().total
        
        # 获取情感统计
        sentiment_stats = get_song_sentiment_stats(song_id)
        
        return success({
            "song_id": song_id,
            "comments": comments,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            },
            "sort": {
                "by": sort_by,
                "order": sort_order
            },
            "stats": sentiment_stats
        })
        
    except Exception as e:
        logger.error(f"获取歌曲评论失败 [{song_id}]: {e}", exc_info=True)
        return error(message=f"获取评论失败: {str(e)}", code=500)

@bp.route('/songs/<song_id>/comments/stats', methods=['GET'])
def get_song_comments_stats(song_id):
    """获取歌曲评论统计"""
    try:
        stats = get_song_sentiment_stats(song_id)
        return success({
            "song_id": song_id,
            "stats": stats
        })
    except Exception as e:
        logger.error(f"获取评论统计失败 [{song_id}]: {e}")
        return error(message=str(e), code=500)

def analyze_sentiment_snownlp(text):
    """使用SnowNLP进行情感分析"""
    try:
        s = SnowNLP(str(text))
        # SnowNLP返回0-1的分数，越接近1越正面
        sentiment = s.sentiments
        return round(sentiment, 3)
    except Exception as e:
        logger.warning(f"SnowNLP情感分析失败: {e}")
        return None

def analyze_sentiment_local(text):
    """本地简单情感分析（优化版）"""
    text = str(text).lower().strip()
    
    # 去除标点
    import re
    text = re.sub(r'[^\w\s\u4e00-\u9fff]', '', text)
    
    # 扩展的音乐专用正向词库
    music_positive_words = [
        '好听', '悦耳', '动听', '美妙', '优美', '悠扬', '动人', '感人',
        '旋律好', '节奏好', '音色好', '声音好', '歌声好', '唱得好',
        '喜欢这首歌', '爱这首歌', '经典', '神曲', '金曲', '必听',
        '单曲循环', '循环播放', '收藏', '推荐', '点赞', '支持',
        '太棒了', '太赞了', '太美了', '太感人了', '太动听了',
        '非常喜欢', '特别喜欢', '超级喜欢', '真心喜欢', '真的喜欢',
        '百听不厌', '百听不腻', '越听越好听', '越听越有味道',
        '治愈', '温暖', '舒服', '放松', '惬意', '享受', '陶醉'
    ]
    
    # 扩展的通用正向词
    general_positive_words = [
        '喜欢', '爱', '棒', '优秀', '完美', '赞', '支持',
        '精彩', '出色', '惊艳', '超赞', '无敌', '爱了',
        '舒服', '温暖', '感动', '美好', '快乐', '高兴',
        '满意', '惊喜', '开心', '愉悦', '迷人', '甜美',
        '清新', '阳光', '正能量', '给力', '厉害', '强大'
    ]
    
    # 负向词库
    negative_words = [
        '难听', '刺耳', '难听死了', '不好听', '不喜欢',
        '讨厌', '垃圾', '差', '烂', '失望', '无聊',
        '恶心', '反感', '受不了', '劝退', '无语',
        '拉胯', '不行', '弃了', '快进', '跳过',
        '痛苦', '难受', '烦躁', '厌恶', '遗憾',
        '后悔', '差劲', '糟糕', '失望透顶'
    ]
    
    # 强度词
    intensity_words = {
        '非常': 2.0, '特别': 2.0, '超级': 2.0, '极其': 2.0,
        '极度': 2.0, '十分': 1.8, '相当': 1.8, '很': 1.5,
        '太': 1.5, '挺': 1.3, '有点': 1.2, '略微': 1.1
    }
    
    # 否定词
    negation_words = ['不', '没', '无', '非', '未', '否', '莫', '勿']
    
    positive_score = 0
    negative_score = 0
    
    # 处理特殊模式
    special_patterns = {
        r'太.*好听了?': 3.0,  # 太好听了、太好听
        r'最.*好听了?': 2.5,  # 最好听了、最好听
        r'非常.*好听了?': 2.5,
        r'特别.*好听了?': 2.5,
        r'超级.*好听了?': 2.5,
        r'真是.*好听了?': 2.0,
        r'确实.*好听了?': 2.0,
        r'真的.*好听了?': 2.0,
        r'确实.*好听': 2.0,
        r'真的.*好听': 2.0,
    }
    
    # 先检查特殊模式
    for pattern, score in special_patterns.items():
        if re.search(pattern, text):
            positive_score += score
    
    # 检查正向词
    for word in music_positive_words + general_positive_words:
        if word in text:
            # 检查是否有否定词前缀
            has_negation = False
            for neg in negation_words:
                if f"{neg}{word}" in text:
                    has_negation = True
                    negative_score += 1.0  # 否定+正向词 = 负向
                    break
            
            if not has_negation:
                # 检查强度词修饰
                intensity = 1.0
                for intens, multiplier in intensity_words.items():
                    if f"{intens}{word}" in text:
                        intensity = multiplier
                        break
                positive_score += intensity
    
    # 检查负向词
    for word in negative_words:
        if word in text:
            # 检查是否有否定词前缀（双重否定）
            has_negation = False
            for neg in negation_words:
                if f"{neg}{word}" in text:
                    has_negation = True
                    positive_score += 1.0  # 否定+负向词 = 正向
                    break
            
            if not has_negation:
                # 检查强度词修饰
                intensity = 1.0
                for intens, multiplier in intensity_words.items():
                    if f"{intens}{word}" in text:
                        intensity = multiplier
                        break
                negative_score += intensity
    
    # 计算总分
    if positive_score + negative_score == 0:
        return 0.5  # 中性
    
    sentiment = 0.5 + (positive_score - negative_score) / (2 * (positive_score + negative_score))
    
    # 确保在0-1范围内
    return round(max(0.0, min(1.0, sentiment)), 3)

def analyze_sentiment(text):
    """主情感分析函数"""
    if HAS_SNOWNLP:
        snow_result = analyze_sentiment_snownlp(text)
        if snow_result is not None:
            return snow_result
    
    # 如果SnowNLP不可用或失败，使用本地方法
    return analyze_sentiment_local(text)

def get_song_sentiment_stats(song_id):
    """获取歌曲情感统计（内部函数）"""
    engine = recommender_service._engine
    
    query = text("""
        SELECT 
            COUNT(*) as total_comments,
            SUM(liked_count) as total_likes,
            AVG(CAST(sentiment_score as FLOAT)) as avg_sentiment,
            SUM(CASE WHEN is_positive = 1 THEN 1 ELSE 0 END) as positive_count,
            SUM(CASE WHEN is_positive = 0 THEN 1 ELSE 0 END) as negative_count,
            SUM(CASE WHEN is_positive IS NULL THEN 1 ELSE 0 END) as neutral_count
        FROM song_comments
        WHERE unified_song_id = :song_id
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"song_id": song_id}).fetchone()
        
        if not result or result.total_comments == 0:
            return {
                "total_comments": 0,
                "total_likes": 0,
                "avg_sentiment": 0.5,
                "positive_count": 0,
                "negative_count": 0,
                "neutral_count": 0,
                "positive_ratio": 0,
                "negative_ratio": 0,
                "neutral_ratio": 0
            }
        
        total = result.total_comments
        positive_ratio = round(result.positive_count / total * 100, 1) if total > 0 else 0
        negative_ratio = round(result.negative_count / total * 100, 1) if total > 0 else 0
        neutral_ratio = round(result.neutral_count / total * 100, 1) if total > 0 else 0
        
        return {
            "total_comments": result.total_comments,
            "total_likes": result.total_likes or 0,
            "avg_sentiment": float(result.avg_sentiment) if result.avg_sentiment else 0.5,
            "positive_count": result.positive_count or 0,
            "negative_count": result.negative_count or 0,
            "neutral_count": result.neutral_count or 0,
            "positive_ratio": positive_ratio,
            "negative_ratio": negative_ratio,
            "neutral_ratio": neutral_ratio
        }

@bp.route('/songs/<song_id>/comments', methods=['POST'])
def add_song_comment(song_id):
    """添加歌曲评论"""
    try:
        data = request.get_json()
        if not data:
            return error(message="请求数据为空", code=400)
        
        content = data.get('content', '').strip()
        user_id = data.get('user_id', '').strip()
        nickname = data.get('nickname', '').strip()
        
        if not content:
            return error(message="评论内容不能为空", code=400)
        
        if len(content) > 1000:
            return error(message="评论内容过长（最多1000字）", code=400)
        
        if not user_id:
            # 如果没有用户ID，可以使用匿名
            user_id = f"anonymous_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            nickname = nickname or "匿名用户"
        
        engine = recommender_service._engine
        
        # 验证歌曲是否存在
        song_check = engine.execute(
            text("SELECT song_name FROM enhanced_song_features WHERE song_id = :song_id"),
            {"song_id": song_id}
        ).fetchone()
        
        if not song_check:
            return error(message="歌曲不存在", code=404)
        
        # 简单的情感分析（实际项目应该调用NLP服务）
        sentiment_score = analyze_sentiment(content)
        is_positive = 1 if sentiment_score > 0.6 else (0 if sentiment_score < 0.4 else None)
        
        # 插入评论
        insert_query = text("""
            INSERT INTO song_comments (
                unified_song_id,
                original_user_id,
                user_nickname,
                content,
                liked_count,
                comment_time,
                sentiment_score,
                is_positive
            ) VALUES (
                :song_id, :user_id, :nickname, :content, 
                0, GETDATE(), :sentiment, :is_positive
            )
        """)
        
        with engine.begin() as conn:
            conn.execute(insert_query, {
                "song_id": song_id,
                "user_id": user_id,
                "nickname": nickname,
                "content": content,
                "sentiment": sentiment_score,
                "is_positive": is_positive
            })
        
        # 更新歌曲表的评论统计（可选）
        update_song_stats_query = text("""
            UPDATE enhanced_song_features 
            SET comment_count = COALESCE(comment_count, 0) + 1,
                avg_sentiment = ISNULL(
                    (SELECT AVG(CAST(sentiment_score as FLOAT)) 
                     FROM song_comments 
                     WHERE unified_song_id = :song_id),
                    0.5
                )
            WHERE song_id = :song_id
        """)
        
        with engine.begin() as conn:
            conn.execute(update_song_stats_query, {"song_id": song_id})
        
        # 记录用户行为
        try:
            record_user_comment_behavior(user_id, song_id)
        except Exception as behavior_error:
            logger.warning(f"记录评论行为失败: {behavior_error}")
        
        logger.info(f"用户 {user_id} 为歌曲 {song_id} 添加评论")
        
        return success({
            "message": "评论发布成功",
            "song_id": song_id,
            "sentiment_score": sentiment_score,
            "is_positive": is_positive
        })
        
    except Exception as e:
        logger.error(f"添加评论失败 [{song_id}]: {e}", exc_info=True)
        return error(message=f"评论发布失败: {str(e)}", code=500)

def analyze_sentiment(text):
    """简单的情感分析函数（改进版）"""
    text = text.lower()
    
    # 更全面的情感词库
    positive_words = ['喜欢', '好听', '爱', '棒', '优秀', '经典', '完美', '赞', '支持', '推荐',
                     '好听', '动听', '美妙', '优美', '感人', '精彩', '出色', '惊艳', '超赞',
                     '无敌', '太棒了', '爱了', '神曲', '收藏', '循环', '单曲循环', '必听']
    
    negative_words = ['讨厌', '难听', '垃圾', '差', '不好', '失望', '烂', '不喜欢', '恶心',
                     '难听', '刺耳', '无聊', '糟糕', '反感', '受不了', '劝退', '失望',
                     '无语', '拉胯', '不行', '弃了', '快进', '跳过']
    
    # 强度词
    strong_positive = ['超级', '非常', '特别', '极其', '极度', '太', '绝', '神']
    strong_negative = ['特别', '非常', '极其', '极度', '太']
    
    positive_count = 0
    negative_count = 0
    
    # 检测正向词
    for word in positive_words:
        if word in text:
            # 检查是否有强度词修饰
            for strong in strong_positive:
                if f"{strong}{word}" in text:
                    positive_count += 2  # 强度词加倍
                    break
            else:
                positive_count += 1
    
    # 检测负向词
    for word in negative_words:
        if word in text:
            # 检查是否有强度词修饰
            for strong in strong_negative:
                if f"{strong}{word}" in text:
                    negative_count += 2  # 强度词加倍
                    break
            else:
                negative_count += 1
    
    total = positive_count + negative_count
    if total == 0:
        return 0.5  # 中性
    
    sentiment = 0.5 + (positive_count - negative_count) / (2 * total)
    return round(max(0.0, min(1.0, sentiment)), 3)

def record_user_comment_behavior(user_id, song_id):
    engine = recommender_service._engine
    with engine.begin() as conn:
        # 检查用户
        user = conn.execute(
            text("SELECT 1 FROM enhanced_user_features WHERE user_id = :uid"),
            {"uid": user_id}
        ).fetchone()
        if not user:
            conn.execute(text("""
                INSERT INTO enhanced_user_features 
                (user_id, nickname, source, activity_level, created_at, updated_at)
                VALUES (:uid, '匿名用户', 'comment', '新用户', GETDATE(), GETDATE())
            """), {"uid": user_id})
        
        # 记录评论行为
        conn.execute(text("""
            INSERT INTO user_song_interaction 
            (user_id, song_id, behavior_type, weight, [timestamp])
            VALUES (:uid, :sid, 'comment', 0.8, GETDATE())
        """), {"uid": user_id, "sid": song_id})

# 创建点赞记录表（需要在数据库中添加）
def create_comment_likes_table():
    """创建评论点赞记录表（防止重复点赞）"""
    engine = recommender_service._engine
    
    # 检查表是否存在
    table_check = engine.execute(text("""
        SELECT 1 FROM sys.tables WHERE name = 'comment_likes'
    """)).fetchone()
    
    if not table_check:
        engine.execute(text("""
            CREATE TABLE comment_likes (
                like_id INT IDENTITY(1,1) PRIMARY KEY,
                comment_id INT NOT NULL,
                user_id VARCHAR(100) NOT NULL,
                created_at DATETIME DEFAULT GETDATE(),
                
                FOREIGN KEY (comment_id) REFERENCES song_comments(comment_id) ON DELETE CASCADE,
                CONSTRAINT UQ_comment_likes UNIQUE(comment_id, user_id)
            )
            
            CREATE INDEX idx_comment_likes_comment ON comment_likes(comment_id);
            CREATE INDEX idx_comment_likes_user ON comment_likes(user_id);
        """))
        logger.info("创建 comment_likes 表成功")

@bp.route('/comments/<int:comment_id>/like', methods=['POST'])
def like_comment(comment_id):
    """点赞评论"""
    try:
        data = request.get_json()
        user_id = data.get('user_id', '').strip()
        action = data.get('action', 'like')  # like 或 cancel
        
        if not user_id:
            return error(message="用户ID不能为空", code=400)
        
        engine = recommender_service._engine
        
        # 确保点赞记录表存在
        create_comment_likes_table()
        
        if action == 'like':
            try:
                # 尝试插入点赞记录
                insert_like = text("""
                    INSERT INTO comment_likes (comment_id, user_id) 
                    VALUES (:comment_id, :user_id)
                """)
                
                with engine.begin() as conn:
                    conn.execute(insert_like, {"comment_id": comment_id, "user_id": user_id})
                    
                # 更新评论点赞数
                update_query = text("""
                    UPDATE song_comments 
                    SET liked_count = ISNULL(liked_count, 0) + 1
                    WHERE comment_id = :comment_id
                """)
                
            except Exception as insert_error:
                if '违反 UNIQUE KEY 约束' in str(insert_error) or 'duplicate' in str(insert_error).lower():
                    return error(message="您已经点过赞了", code=400)
                raise
                
        else:  # cancel like
            # 先删除点赞记录
            delete_like = text("""
                DELETE FROM comment_likes 
                WHERE comment_id = :comment_id AND user_id = :user_id
            """)
            
            with engine.begin() as conn:
                result = conn.execute(delete_like, {"comment_id": comment_id, "user_id": user_id})
                
                if result.rowcount == 0:
                    return error(message="您尚未点赞", code=400)
            
            # 更新评论点赞数
            update_query = text("""
                UPDATE song_comments 
                SET liked_count = 
                    CASE 
                        WHEN ISNULL(liked_count, 0) - 1 < 0 THEN 0 
                        ELSE ISNULL(liked_count, 0) - 1 
                    END
                WHERE comment_id = :comment_id
            """)
        
        # 执行更新
        with engine.begin() as conn:
            result = conn.execute(update_query, {"comment_id": comment_id})
            
            if result.rowcount == 0:
                return error(message="评论不存在", code=404)
        
        # 获取更新后的点赞数
        get_query = text("""
            SELECT liked_count FROM song_comments WHERE comment_id = :comment_id
        """)
        
        with engine.connect() as conn:
            new_count = conn.execute(get_query, {"comment_id": comment_id}).fetchone().liked_count
        
        return success({
            "comment_id": comment_id,
            "liked_count": new_count,
            "action": action,
            "user_has_liked": action == 'like'
        })
        
    except Exception as e:
        logger.error(f"点赞评论失败 [{comment_id}]: {e}")
        return error(message=str(e), code=500)

@bp.route('/comments/<int:comment_id>', methods=['DELETE'])
def delete_comment(comment_id):
    """删除评论（管理员或用户本人）"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        is_admin = data.get('is_admin', False)
        
        if not user_id:
            return error(message="用户ID不能为空", code=400)
        
        engine = recommender_service._engine
        
        # 先获取评论信息
        get_query = text("""
            SELECT unified_song_id, original_user_id 
            FROM song_comments 
            WHERE comment_id = :comment_id
        """)
        
        with engine.connect() as conn:
            comment = conn.execute(get_query, {"comment_id": comment_id}).fetchone()
            
            if not comment:
                return error(message="评论不存在", code=404)
            
            # 检查权限
            if not is_admin and (not comment.original_user_id or comment.original_user_id != user_id):
                return error(message="无权删除此评论", code=403)
        
        # 删除评论
        delete_query = text("""
            DELETE FROM song_comments WHERE comment_id = :comment_id
        """)
        
        with engine.begin() as conn:
            conn.execute(delete_query, {"comment_id": comment_id})
        
        # 更新歌曲统计
        update_song_query = text("""
            UPDATE enhanced_song_features 
            SET comment_count = 
                CASE 
                    WHEN ISNULL(comment_count, 0) - 1 < 0 THEN 0 
                    ELSE ISNULL(comment_count, 0) - 1 
                END
            WHERE song_id = :song_id
        """)
        
        with engine.begin() as conn:
            conn.execute(update_song_query, {"song_id": comment.unified_song_id})
        
        logger.info(f"评论 {comment_id} 已删除")
        
        return success({
            "message": "评论删除成功",
            "comment_id": comment_id
        })
        
    except Exception as e:
        logger.error(f"删除评论失败 [{comment_id}]: {e}")
        return error(message=str(e), code=500)

# ==================== 热门评论API ====================

@bp.route('/comments/hot', methods=['GET'])
def get_hot_comments():
    """获取热门评论（全站）"""
    try:
        limit = min(request.args.get('limit', 10, type=int), 50)
        
        engine = recommender_service._engine
        
        query = text("""
            SELECT TOP (:limit)
                c.comment_id,
                c.unified_song_id,
                s.song_name,
                s.artists,
                c.user_nickname,
                c.content,
                c.liked_count,
                c.comment_time,
                c.sentiment_score,
                c.is_positive
            FROM song_comments c
            JOIN enhanced_song_features s ON c.unified_song_id = s.song_id
            WHERE c.liked_count > 0
            ORDER BY c.liked_count DESC, c.comment_time DESC
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"limit": limit})
            
            comments = []
            for row in result:
                comments.append({
                    "comment_id": row.comment_id,
                    "song_id": row.unified_song_id,
                    "song_name": row.song_name,
                    "artists": row.artists,
                    "user_nickname": row.user_nickname,
                    "content": row.content[:100] + "..." if len(row.content) > 100 else row.content,
                    "liked_count": row.liked_count,
                    "comment_time": row.comment_time.isoformat() if row.comment_time else None,
                    "sentiment_score": float(row.sentiment_score) if row.sentiment_score else None
                })
        
        return success({
            "comments": comments,
            "count": len(comments)
        })
        
    except Exception as e:
        logger.error(f"获取热门评论失败: {e}")
        return error(message=str(e), code=500)

# ==================== 重新计算情感分数API ====================

@bp.route('/admin/comments/recalculate-sentiment', methods=['POST'])
def recalculate_all_comments_sentiment():
    """重新计算所有评论的情感分数（管理员功能）"""
    try:
        # 简单的身份验证（生产环境应使用更安全的验证）
        data = request.get_json()
        if not data or data.get('admin_key') != 'music_rec_admin_2024':
            return error(message="无权限", code=403)
        
        engine = recommender_service._engine
        
        # 获取所有评论
        query = text("""
            SELECT comment_id, content 
            FROM song_comments 
            WHERE content IS NOT NULL AND LEN(content) > 0
            ORDER BY comment_id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query)
            comments = [(row.comment_id, row.content) for row in result]
        
        total = len(comments)
        updated = 0
        
        logger.info(f"开始重新计算 {total} 条评论的情感分数...")
        
        for i, (comment_id, content) in enumerate(comments):
            try:
                # 使用新的情感分析函数
                sentiment_score = analyze_sentiment(content)
                is_positive = 1 if sentiment_score > 0.6 else (0 if sentiment_score < 0.4 else None)
                
                # 更新数据库
                update_query = text("""
                    UPDATE song_comments 
                    SET sentiment_score = :sentiment, is_positive = :is_positive
                    WHERE comment_id = :comment_id
                """)
                
                with engine.begin() as conn:
                    conn.execute(update_query, {
                        "comment_id": comment_id,
                        "sentiment": sentiment_score,
                        "is_positive": is_positive
                    })
                
                updated += 1
                
                # 显示进度
                if (i + 1) % 100 == 0:
                    logger.info(f"进度: {i+1}/{total} ({((i+1)/total*100):.1f}%)")
                    
            except Exception as e:
                logger.error(f"重新计算评论 {comment_id} 失败: {e}")
        
        # 重新计算歌曲的平均情感分数
        recalc_song_sentiment_query = text("""
            UPDATE enhanced_song_features 
            SET avg_sentiment = s.avg_score
            FROM enhanced_song_features esf
            INNER JOIN (
                SELECT unified_song_id, AVG(CAST(sentiment_score as FLOAT)) as avg_score
                FROM song_comments 
                WHERE sentiment_score IS NOT NULL
                GROUP BY unified_song_id
            ) s ON esf.song_id = s.unified_song_id
        """)
        
        with engine.begin() as conn:
            conn.execute(recalc_song_sentiment_query)
        
        logger.info(f"情感分数重新计算完成，更新了 {updated}/{total} 条评论")
        
        return success({
            "message": "情感分数重新计算完成",
            "total_comments": total,
            "updated_comments": updated,
            "method": "SnowNLP" if HAS_SNOWNLP else "本地词库"
        })
        
    except Exception as e:
        logger.error(f"重新计算情感分数失败: {e}", exc_info=True)
        return error(message=f"重新计算失败: {str(e)}", code=500)
    
# routes/admin.py 中添加评论管理功能

# 1. 获取歌曲的评论列表
@bp.route('/songs/<song_id>/comments', methods=['GET'])
def get_song_comments_admin(song_id):
    """管理员获取歌曲评论列表"""
    try:
        # 验证管理员权限
        admin_token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not admin_token or admin_token != current_app.config.get('ADMIN_TOKEN'):
            return jsonify({"success": False, "message": "未授权"}), 401
        
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 20, type=int), 100)
        offset = (page - 1) * per_page
        
        engine = recommender_service._engine
        
        # 验证歌曲是否存在
        song_check = engine.execute(
            text("SELECT song_name FROM enhanced_song_features WHERE song_id = :song_id"),
            {"song_id": song_id}
        ).fetchone()
        
        if not song_check:
            return jsonify({"success": False, "message": "歌曲不存在"}), 404
        
        # 获取评论数据
        query = text("""
            SELECT 
                comment_id,
                unified_song_id,
                original_user_id,
                user_nickname,
                content,
                liked_count,
                comment_time,
                sentiment_score,
                is_positive,
                created_at
            FROM song_comments
            WHERE unified_song_id = :song_id
            ORDER BY comment_time DESC
            OFFSET :offset ROWS
            FETCH NEXT :limit ROWS ONLY
        """)
        
        with engine.connect() as conn:
            result = conn.execute(
                query,
                {"song_id": song_id, "offset": offset, "limit": per_page}
            )
            
            comments = []
            for row in result:
                comments.append({
                    "comment_id": row.comment_id,
                    "user_id": row.original_user_id,
                    "user_nickname": row.user_nickname or "匿名用户",
                    "content": row.content,
                    "liked_count": row.liked_count or 0,
                    "comment_time": row.comment_time.isoformat() if row.comment_time else None,
                    "sentiment_score": float(row.sentiment_score) if row.sentiment_score else 0.5,
                    "is_positive": bool(row.is_positive) if row.is_positive is not None else None,
                    "created_at": row.created_at.isoformat() if row.created_at else None
                })
            
            # 获取总数
            count_query = text("""
                SELECT COUNT(*) as total 
                FROM song_comments 
                WHERE unified_song_id = :song_id
            """)
            total = conn.execute(count_query, {"song_id": song_id}).fetchone().total
        
        return jsonify({
            "success": True,
            "data": {
                "song_id": song_id,
                "song_name": song_check.song_name,
                "comments": comments,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total,
                    "pages": (total + per_page - 1) // per_page
                }
            }
        })
        
    except Exception as e:
        logger.error(f"获取歌曲评论失败 [{song_id}]: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"获取评论失败: {str(e)}"}), 500

# 2. 删除评论（管理员）
@bp.route('/comments/<int:comment_id>', methods=['DELETE'])
def delete_comment_admin(comment_id):
    """管理员删除评论"""
    try:
        # 验证管理员权限
        admin_token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not admin_token or admin_token != current_app.config.get('ADMIN_TOKEN'):
            return jsonify({"success": False, "message": "未授权"}), 401
        
        engine = recommender_service._engine
        
        # 先获取评论信息
        get_query = text("""
            SELECT unified_song_id, original_user_id, user_nickname
            FROM song_comments 
            WHERE comment_id = :comment_id
        """)
        
        with engine.connect() as conn:
            comment = conn.execute(get_query, {"comment_id": comment_id}).fetchone()
            
            if not comment:
                return jsonify({"success": False, "message": "评论不存在"}), 404
        
        # 删除评论
        delete_query = text("""
            DELETE FROM song_comments WHERE comment_id = :comment_id
        """)
        
        with engine.begin() as conn:
            conn.execute(delete_query, {"comment_id": comment_id})
        
        # 更新歌曲统计
        update_song_query = text("""
            UPDATE enhanced_song_features 
            SET comment_count = 
                CASE 
                    WHEN ISNULL(comment_count, 0) - 1 < 0 THEN 0 
                    ELSE ISNULL(comment_count, 0) - 1 
                END,
                avg_sentiment = ISNULL(
                    (SELECT AVG(CAST(sentiment_score as FLOAT)) 
                     FROM song_comments 
                     WHERE unified_song_id = :song_id),
                    0.5
                )
            WHERE song_id = :song_id
        """)
        
        with engine.begin() as conn:
            conn.execute(update_song_query, {"song_id": comment.unified_song_id})
        
        logger.info(f"管理员删除评论 {comment_id}, 歌曲: {comment.unified_song_id}")
        
        return jsonify({
            "success": True,
            "message": "评论删除成功",
            "data": {
                "comment_id": comment_id,
                "song_id": comment.unified_song_id,
                "user_nickname": comment.user_nickname
            }
        })
        
    except Exception as e:
        logger.error(f"删除评论失败 [{comment_id}]: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"删除失败: {str(e)}"}), 500

# 3. 修改评论情感值
@bp.route('/comments/<int:comment_id>/sentiment', methods=['PUT'])
def update_comment_sentiment(comment_id):
    """修改评论情感值"""
    try:
        # 验证管理员权限
        admin_token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not admin_token or admin_token != current_app.config.get('ADMIN_TOKEN'):
            return jsonify({"success": False, "message": "未授权"}), 401
        
        data = request.get_json()
        if not data or 'sentiment_score' not in data:
            return jsonify({"success": False, "message": "缺少sentiment_score参数"}), 400
        
        sentiment_score = float(data['sentiment_score'])
        
        # 验证范围
        if sentiment_score < 0 or sentiment_score > 1:
            return jsonify({"success": False, "message": "情感值必须在0-1之间"}), 400
        
        # 计算是否为正面评论
        is_positive = 1 if sentiment_score > 0.6 else (0 if sentiment_score < 0.4 else None)
        
        engine = recommender_service._engine
        
        # 获取原评论信息
        get_query = text("""
            SELECT unified_song_id FROM song_comments WHERE comment_id = :comment_id
        """)
        
        with engine.connect() as conn:
            comment = conn.execute(get_query, {"comment_id": comment_id}).fetchone()
            
            if not comment:
                return jsonify({"success": False, "message": "评论不存在"}), 404
        
        # 更新情感值
        update_query = text("""
            UPDATE song_comments 
            SET sentiment_score = :sentiment_score, is_positive = :is_positive
            WHERE comment_id = :comment_id
        """)
        
        with engine.begin() as conn:
            conn.execute(update_query, {
                "comment_id": comment_id,
                "sentiment_score": sentiment_score,
                "is_positive": is_positive
            })
        
        # 更新歌曲的平均情感值
        update_song_sentiment_query = text("""
            UPDATE enhanced_song_features 
            SET avg_sentiment = ISNULL(
                (SELECT AVG(CAST(sentiment_score as FLOAT)) 
                 FROM song_comments 
                 WHERE unified_song_id = :song_id),
                0.5
            )
            WHERE song_id = :song_id
        """)
        
        with engine.begin() as conn:
            conn.execute(update_song_sentiment_query, {"song_id": comment.unified_song_id})
        
        logger.info(f"更新评论 {comment_id} 情感值为 {sentiment_score}")
        
        return jsonify({
            "success": True,
            "message": "情感值更新成功",
            "data": {
                "comment_id": comment_id,
                "sentiment_score": sentiment_score,
                "is_positive": is_positive
            }
        })
        
    except Exception as e:
        logger.error(f"更新情感值失败 [{comment_id}]: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"更新失败: {str(e)}"}), 500

# 4. 批量重新计算歌曲评论情感值
@bp.route('/songs/<song_id>/comments/recalculate-sentiment', methods=['POST'])
def recalculate_song_comments_sentiment(song_id):
    """重新计算歌曲所有评论的情感值"""
    try:
        # 验证管理员权限
        admin_token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not admin_token or admin_token != current_app.config.get('ADMIN_TOKEN'):
            return jsonify({"success": False, "message": "未授权"}), 401
        
        engine = recommender_service._engine
        
        # 获取歌曲所有评论
        query = text("""
            SELECT comment_id, content 
            FROM song_comments 
            WHERE unified_song_id = :song_id 
            AND content IS NOT NULL AND LEN(content) > 0
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"song_id": song_id})
            comments = [(row.comment_id, row.content) for row in result]
        
        total = len(comments)
        updated = 0
        
        logger.info(f"开始重新计算歌曲 {song_id} 的 {total} 条评论情感值...")
        
        for comment_id, content in comments:
            try:
                # 使用情感分析函数（需要从comments.py导入或定义）
                from routes.comments import analyze_sentiment
                sentiment_score = analyze_sentiment(content)
                is_positive = 1 if sentiment_score > 0.6 else (0 if sentiment_score < 0.4 else None)
                
                # 更新数据库
                update_query = text("""
                    UPDATE song_comments 
                    SET sentiment_score = :sentiment, is_positive = :is_positive
                    WHERE comment_id = :comment_id
                """)
                
                with engine.begin() as conn:
                    conn.execute(update_query, {
                        "comment_id": comment_id,
                        "sentiment": sentiment_score,
                        "is_positive": is_positive
                    })
                
                updated += 1
                
            except Exception as e:
                logger.error(f"重新计算评论 {comment_id} 失败: {e}")
        
        # 更新歌曲的平均情感值
        update_song_query = text("""
            UPDATE enhanced_song_features 
            SET avg_sentiment = ISNULL(
                (SELECT AVG(CAST(sentiment_score as FLOAT)) 
                 FROM song_comments 
                 WHERE unified_song_id = :song_id),
                0.5
            )
            WHERE song_id = :song_id
        """)
        
        with engine.begin() as conn:
            conn.execute(update_song_query, {"song_id": song_id})
        
        logger.info(f"歌曲 {song_id} 情感值重新计算完成，更新了 {updated}/{total} 条评论")
        
        return jsonify({
            "success": True,
            "message": "情感值重新计算完成",
            "data": {
                "song_id": song_id,
                "total_comments": total,
                "updated_comments": updated
            }
        })
        
    except Exception as e:
        logger.error(f"重新计算歌曲评论情感值失败 [{song_id}]: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"重新计算失败: {str(e)}"}), 500

# 5. 获取歌曲评论统计
@bp.route('/songs/<song_id>/comments/stats', methods=['GET'])
def get_song_comments_stats_admin(song_id):
    """管理员获取歌曲评论统计"""
    try:
        # 验证管理员权限
        admin_token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not admin_token or admin_token != current_app.config.get('ADMIN_TOKEN'):
            return jsonify({"success": False, "message": "未授权"}), 401
        
        engine = recommender_service._engine
        
        query = text("""
            SELECT 
                COUNT(*) as total_comments,
                SUM(liked_count) as total_likes,
                AVG(CAST(sentiment_score as FLOAT)) as avg_sentiment,
                SUM(CASE WHEN is_positive = 1 THEN 1 ELSE 0 END) as positive_count,
                SUM(CASE WHEN is_positive = 0 THEN 1 ELSE 0 END) as negative_count,
                SUM(CASE WHEN is_positive IS NULL THEN 1 ELSE 0 END) as neutral_count
            FROM song_comments
            WHERE unified_song_id = :song_id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"song_id": song_id}).fetchone()
            
            if not result or result.total_comments == 0:
                stats = {
                    "total_comments": 0,
                    "total_likes": 0,
                    "avg_sentiment": 0.5,
                    "positive_count": 0,
                    "negative_count": 0,
                    "neutral_count": 0,
                    "positive_ratio": 0,
                    "negative_ratio": 0,
                    "neutral_ratio": 0
                }
            else:
                total = result.total_comments
                positive_ratio = round(result.positive_count / total * 100, 1) if total > 0 else 0
                negative_ratio = round(result.negative_count / total * 100, 1) if total > 0 else 0
                neutral_ratio = round(result.neutral_count / total * 100, 1) if total > 0 else 0
                
                stats = {
                    "total_comments": result.total_comments,
                    "total_likes": result.total_likes or 0,
                    "avg_sentiment": float(result.avg_sentiment) if result.avg_sentiment else 0.5,
                    "positive_count": result.positive_count or 0,
                    "negative_count": result.negative_count or 0,
                    "neutral_count": result.neutral_count or 0,
                    "positive_ratio": positive_ratio,
                    "negative_ratio": negative_ratio,
                    "neutral_ratio": neutral_ratio
                }
        
        return jsonify({
            "success": True,
            "data": {
                "song_id": song_id,
                "stats": stats
            }
        })
        
    except Exception as e:
        logger.error(f"获取评论统计失败 [{song_id}]: {e}")
        return jsonify({"success": False, "message": f"获取统计失败: {str(e)}"}), 500