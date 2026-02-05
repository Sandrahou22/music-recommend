import schedule
import time
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from recommender_service import recommender_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_algorithm_performance():
    """计算算法性能指标"""
    try:
        engine = recommender_service._engine
        today = datetime.now().date()
        
        # 计算最近7天的数据
        with engine.connect() as conn:
            # 1. 获取各算法的推荐数据
            result = conn.execute(text("""
                SELECT 
                    algorithm_type,
                    COUNT(*) as total_recommendations,
                    SUM(CASE WHEN is_clicked = 1 THEN 1 ELSE 0 END) as clicks,
                    SUM(CASE WHEN is_listened = 1 THEN 1 ELSE 0 END) as listens
                FROM recommendations
                WHERE created_at >= :start_date
                GROUP BY algorithm_type
            """), {"start_date": today - timedelta(days=7)})
            
            algorithm_data = {}
            for row in result:
                algorithm_data[row.algorithm_type] = {
                    'total': row.total_recommendations,
                    'clicks': row.clicks or 0,
                    'listens': row.listens or 0
                }
            
            # 2. 计算CTR和收听率
            performance_stats = []
            for algo, data in algorithm_data.items():
                if data['total'] > 0:
                    ctr = (data['clicks'] / data['total']) * 100
                    listen_rate = (data['listens'] / data['total']) * 100
                    
                    # 模拟计算召回率、准确率、多样性（实际中需要更复杂的计算）
                    # 这里可以根据你的业务逻辑实现
                    recall_rate = calculate_recall_rate(algo, today)
                    precision_rate = calculate_precision_rate(algo, today)
                    diversity_score = calculate_diversity_score(algo, today)
                    
                    performance_stats.append({
                        'algorithm_type': algo,
                        'metric_date': today,
                        'recall_rate': recall_rate,
                        'precision_rate': precision_rate,
                        'diversity_score': diversity_score,
                        'ctr_rate': ctr,
                        'listen_rate': listen_rate,
                        'total_recommendations': data['total'],
                        'clicks': data['clicks'],
                        'listens': data['listens']
                    })
            
            # 3. 保存到数据库
            for stat in performance_stats:
                conn.execute(text("""
                    MERGE algorithm_performance_stats AS target
                    USING (SELECT :algorithm_type as algo, :metric_date as date) AS source
                    ON target.algorithm_type = source.algo AND target.metric_date = source.date
                    WHEN MATCHED THEN
                        UPDATE SET 
                            recall_rate = :recall_rate,
                            precision_rate = :precision_rate,
                            diversity_score = :diversity_score,
                            ctr_rate = :ctr_rate,
                            listen_rate = :listen_rate,
                            total_recommendations = :total_recommendations,
                            clicks = :clicks,
                            listens = :listens,
                            created_at = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (algorithm_type, metric_date, recall_rate, precision_rate,
                                diversity_score, ctr_rate, listen_rate, total_recommendations,
                                clicks, listens)
                        VALUES (:algorithm_type, :metric_date, :recall_rate, :precision_rate,
                                :diversity_score, :ctr_rate, :listen_rate, :total_recommendations,
                                :clicks, :listens);
                """), stat)
            
            conn.commit()
            logger.info(f"算法性能统计更新完成: {len(performance_stats)}条记录")
            
    except Exception as e:
        logger.error(f"计算算法性能失败: {e}")

def calculate_recall_rate(algorithm, date):
    """计算召回率（需要根据你的业务逻辑实现）"""
    # 这里实现召回率计算逻辑
    # 可以基于测试集和推荐结果计算
    return 75.0 + (hash(algorithm) % 20)  # 临时示例

def calculate_precision_rate(algorithm, date):
    """计算准确率"""
    return 70.0 + (hash(algorithm) % 15)

def calculate_diversity_score(algorithm, date):
    """计算多样性得分"""
    return 65.0 + (hash(algorithm) % 20)

if __name__ == "__main__":
    # 立即运行一次
    calculate_algorithm_performance()
    
    # 设置定时任务（每天凌晨2点运行）
    schedule.every().day.at("02:00").do(calculate_algorithm_performance)
    
    logger.info("算法性能监控服务启动...")
    while True:
        schedule.run_pending()
        time.sleep(60)