# recalculate_sentiment.py
import time
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

from utils.sentiment_analyzer import analyze_music_sentiment

def analyze_sentiment_local(text):
    """使用新的音乐情感分析器"""
    score, _ = analyze_music_sentiment(str(text))
    return score

def analyze_sentiment_snownlp(text):
    """同时使用SnowNLP和音乐分析器，取平均"""
    try:
        from snownlp import SnowNLP
        s = SnowNLP(str(text))
        snow_score = s.sentiments
        
        # 使用音乐分析器
        music_score, _ = analyze_music_sentiment(str(text))
        
        # 加权平均，音乐分析器权重更高
        final_score = snow_score * 0.3 + music_score * 0.7
        
        return round(final_score, 3)
    except Exception as e:
        logger.warning(f"SnowNLP情感分析失败: {e}")
        # 回退到音乐分析器
        score, _ = analyze_music_sentiment(str(text))
        return score

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_database_engine():
    """获取数据库连接"""
    connection_string = (
        "mssql+pyodbc://@localhost/MusicRecommendationDB?"
        "driver=ODBC+Driver+18+for+SQL+Server&"
        "Trusted_Connection=yes&"
        "Encrypt=no"
    )
    
    try:
        engine = create_engine(connection_string)
        # 测试连接
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("数据库连接成功")
        return engine
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        # 尝试备选连接方式
        alternatives = [
            "mssql+pyodbc://sa:123456/MusicRecommendationDB?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no",
        ]
        
        for conn_str in alternatives:
            try:
                engine = create_engine(conn_str)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info(f"使用备选连接方式成功")
                return engine
            except Exception:
                continue
        
        raise ConnectionError("所有连接方式都失败了")

def analyze_sentiment_local(text):
    """本地简单情感分析"""
    text = str(text).lower()
    
    positive_words = ['喜欢', '好听', '爱', '棒', '优秀', '经典', '完美', '赞', '支持', '推荐',
                     '舒服', '温暖', '感动', '美好', '好听', '动听', '美妙', '优美', '感人',
                     '精彩', '出色', '惊艳', '超赞', '无敌', '太棒了', '爱了', '神曲', '收藏',
                     '循环', '单曲循环', '必听', '舒适', '惬意', '愉悦', '开心', '快乐', '高兴',
                     '满意', '惊喜', '享受', '陶醉', '沉醉', '迷人', '动人', '感人', '治愈',
                     '放松', '舒缓', '轻柔', '温柔', '甜美', '清新', '阳光', '正能量']
    
    negative_words = ['讨厌', '难听', '垃圾', '差', '不好', '失望', '烂', '不喜欢', '恶心',
                     '刺耳', '无聊', '糟糕', '反感', '受不了', '劝退', '失望', '无语',
                     '拉胯', '不行', '弃了', '快进', '跳过', '痛苦', '难受', '烦躁',
                     '厌恶', '遗憾', '后悔', '差劲', '糟糕', '无语', '失望']
    
    positive_count = 0
    negative_count = 0
    
    for word in positive_words:
        if word in text:
            positive_count += 1
    
    for word in negative_words:
        if word in text:
            negative_count += 1
    
    total = positive_count + negative_count
    if total == 0:
        return 0.5  # 中性
    
    sentiment = 0.5 + (positive_count - negative_count) / (2 * total)
    return round(max(0.0, min(1.0, sentiment)), 3)

def analyze_sentiment_snownlp(text):
    """使用SnowNLP进行情感分析"""
    try:
        from snownlp import SnowNLP
        s = SnowNLP(str(text))
        sentiment = s.sentiments
        return round(sentiment, 3)
    except Exception as e:
        logger.warning(f"SnowNLP情感分析失败: {e}")
        return None

def recalculate_sentiment(engine, use_snownlp=True, batch_size=500):
    """重新计算所有评论的情感分数"""
    start_time = time.time()
    
    # 检查SnowNLP是否可用
    if use_snownlp:
        try:
            from snownlp import SnowNLP
            logger.info("使用SnowNLP进行情感分析")
        except ImportError:
            logger.warning("SnowNLP未安装，将使用本地词库分析")
            use_snownlp = False
    
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
    logger.info(f"开始重新计算 {total} 条评论的情感分数...")
    
    updated = 0
    errors = 0
    
    for i, (comment_id, content) in enumerate(comments):
        try:
            # 使用指定的情感分析方法
            if use_snownlp:
                sentiment_score = analyze_sentiment_snownlp(content)
                if sentiment_score is None:  # 如果SnowNLP失败，使用本地方法
                    sentiment_score = analyze_sentiment_local(content)
            else:
                sentiment_score = analyze_sentiment_local(content)
            
            # 确定情感极性
            if sentiment_score > 0.6:
                is_positive = 1
            elif sentiment_score < 0.4:
                is_positive = 0
            else:
                is_positive = None
            
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
            if (i + 1) % batch_size == 0:
                elapsed = time.time() - start_time
                logger.info(f"进度: {i+1}/{total} ({((i+1)/total*100):.1f}%) - 已更新 {updated} 条 - 耗时: {elapsed:.1f}秒")
                
        except Exception as e:
            errors += 1
            logger.error(f"重新计算评论 {comment_id} 失败: {e}")
    
    # 重新计算歌曲的平均情感分数
    logger.info("重新计算歌曲的平均情感分数...")
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
        result = conn.execute(recalc_song_sentiment_query)
        logger.info(f"更新了 {result.rowcount} 首歌曲的平均情感分数")
    
    # 统计信息
    elapsed = time.time() - start_time
    logger.info("="*60)
    logger.info("情感分数重新计算完成")
    logger.info(f"总评论数: {total}")
    logger.info(f"成功更新: {updated}")
    logger.info(f"失败: {errors}")
    logger.info(f"总耗时: {elapsed:.1f}秒")
    logger.info(f"平均速度: {total/elapsed:.1f} 条/秒")
    logger.info(f"使用的方法: {'SnowNLP' if use_snownlp else '本地词库'}")
    logger.info("="*60)
    
    return {
        "total_comments": total,
        "updated": updated,
        "errors": errors,
        "elapsed_time": elapsed,
        "method": "SnowNLP" if use_snownlp else "local"
    }

def get_statistics(engine):
    """获取情感分析统计信息"""
    logger.info("获取情感分析统计信息...")
    
    queries = {
        "总评论数": "SELECT COUNT(*) as count FROM song_comments",
        "有情感分数的评论": "SELECT COUNT(*) as count FROM song_comments WHERE sentiment_score IS NOT NULL",
        "正面评论数": "SELECT COUNT(*) as count FROM song_comments WHERE is_positive = 1",
        "负面评论数": "SELECT COUNT(*) as count FROM song_comments WHERE is_positive = 0",
        "中性评论数": "SELECT COUNT(*) as count FROM song_comments WHERE is_positive IS NULL",
        "平均情感分数": "SELECT AVG(CAST(sentiment_score as FLOAT)) as avg FROM song_comments WHERE sentiment_score IS NOT NULL",
        "情感分数分布": """
            SELECT 
                CASE 
                    WHEN sentiment_score < 0.4 THEN '负面 (<0.4)'
                    WHEN sentiment_score >= 0.4 AND sentiment_score <= 0.6 THEN '中性 (0.4-0.6)'
                    WHEN sentiment_score > 0.6 THEN '正面 (>0.6)'
                    ELSE '未知'
                END as sentiment_range,
                COUNT(*) as count,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM song_comments WHERE sentiment_score IS NOT NULL) as percentage
            FROM song_comments 
            WHERE sentiment_score IS NOT NULL
            GROUP BY 
                CASE 
                    WHEN sentiment_score < 0.4 THEN '负面 (<0.4)'
                    WHEN sentiment_score >= 0.4 AND sentiment_score <= 0.6 THEN '中性 (0.4-0.6)'
                    WHEN sentiment_score > 0.6 THEN '正面 (>0.6)'
                    ELSE '未知'
                END
            ORDER BY sentiment_range
        """
    }
    
    results = {}
    with engine.connect() as conn:
        for name, query in queries.items():
            try:
                result = conn.execute(text(query)).fetchone()
                if result:
                    results[name] = dict(result._mapping)
            except Exception as e:
                logger.error(f"查询 {name} 失败: {e}")
                results[name] = {"error": str(e)}
    
    return results

def main():
    """主函数"""
    print("="*80)
    print("评论情感分数重新计算工具")
    print("="*80)
    
    try:
        # 获取数据库连接
        engine = get_database_engine()
        
        # 显示当前统计信息
        print("\n📊 当前统计信息:")
        stats_before = get_statistics(engine)
        for name, data in stats_before.items():
            if isinstance(data, dict) and 'error' not in data:
                if 'avg' in data:
                    print(f"  {name}: {data['avg']:.3f}")
                elif 'count' in data:
                    print(f"  {name}: {data['count']:,}")
                elif 'percentage' in data:
                    print(f"  {name}: {data['sentiment_range']} - {data['count']:,} ({data['percentage']:.1f}%)")
        
        # 询问是否使用SnowNLP
        use_snownlp = input("\n是否使用SnowNLP进行情感分析? (y/n, 默认y): ").strip().lower() in ['y', 'yes', '']
        
        if use_snownlp:
            try:
                from snownlp import SnowNLP
                print("✅ SnowNLP 可用")
            except ImportError:
                print("❌ SnowNLP 未安装，将使用本地词库分析")
                use_snownlp = False
        
        # 确认开始
        confirm = input(f"\n确定要重新计算所有评论的情感分数吗? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("操作已取消")
            return
        
        # 开始重新计算
        print(f"\n开始重新计算情感分数...")
        print(f"使用的方法: {'SnowNLP' if use_snownlp else '本地词库'}")
        
        result = recalculate_sentiment(engine, use_snownlp=use_snownlp)
        
        # 显示重新计算后的统计信息
        print("\n📊 重新计算后的统计信息:")
        stats_after = get_statistics(engine)
        for name, data in stats_after.items():
            if isinstance(data, dict) and 'error' not in data:
                if 'avg' in data:
                    print(f"  {name}: {data['avg']:.3f}")
                elif 'count' in data:
                    print(f"  {name}: {data['count']:,}")
                elif 'percentage' in data:
                    print(f"  {name}: {data['sentiment_range']} - {data['count']:,} ({data['percentage']:.1f}%)")
        
        print(f"\n✅ 情感分数重新计算完成!")
        
    except Exception as e:
        print(f"\n❌ 程序执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()