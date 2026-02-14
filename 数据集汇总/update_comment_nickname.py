# file: update_comment_nickname.py
"""
更新 song_comments 表中的 user_nickname
通过 original_user_id 关联内部用户数据获取昵称
"""

import pandas as pd
import os
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_comment_nickname():
    """从原始用户数据中读取昵称，更新 song_comments 表"""
    
    # ---------- 1. 数据库配置 ----------
    db_config = {
        'server': 'localhost',
        'database': 'MusicRecommendationDB',
        'username': 'sa',
        'password': '123456',   # ← 请修改为您的密码
        'driver': 'ODBC Driver 18 for SQL Server'
    }
    conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                f"@{db_config['server']}/{db_config['database']}"
                f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
    engine = create_engine(conn_str)
    
    # ---------- 2. 读取原始用户数据（获取 user_id -> nickname 映射）----------
    user_file = "用户数据_20260124_200012.csv"
    if not os.path.exists(user_file):
        logger.error(f"用户文件不存在: {user_file}")
        return
    
    logger.info(f"读取用户文件: {user_file}")
    user_df = pd.read_csv(user_file)
    user_df = user_df[['user_id', 'nickname']].dropna()
    user_df['user_id'] = user_df['user_id'].astype(str)
    user_df['nickname'] = user_df['nickname'].astype(str)
    
    # 构建映射字典
    nickname_map = dict(zip(user_df['user_id'], user_df['nickname']))
    logger.info(f"共加载 {len(nickname_map)} 个用户昵称")
    
    # ---------- 3. 获取所有需要更新昵称的评论 ----------
    with engine.connect() as conn:
        query = text("""
            SELECT comment_id, original_user_id 
            FROM song_comments 
            WHERE original_user_id IS NOT NULL AND (user_nickname IS NULL OR user_nickname = '')
        """)
        result = conn.execute(query)
        comments = [(row.comment_id, str(row.original_user_id)) for row in result]
    
    logger.info(f"需要更新昵称的评论: {len(comments)} 条")
    
    # ---------- 4. 批量更新 ----------
    update_count = 0
    batch_size = 1000
    updates = []
    
    for comment_id, original_user_id in comments:
        nickname = nickname_map.get(original_user_id)
        if nickname and pd.notna(nickname):
            updates.append({'comment_id': comment_id, 'nickname': nickname})
            update_count += 1
        
        # 批量执行
        if len(updates) >= batch_size:
            _execute_updates(engine, updates)
            updates = []
    
    if updates:
        _execute_updates(engine, updates)
    
    logger.info(f"昵称更新完成: 成功 {update_count} 条")
    
    # ---------- 5. 统计验证 ----------
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) as cnt 
            FROM song_comments 
            WHERE user_nickname IS NOT NULL AND user_nickname != ''
        """))
        filled = result.fetchone()[0]
        logger.info(f"现在共有 {filled} 条评论拥有昵称")


def _execute_updates(engine, updates):
    """批量执行更新"""
    with engine.begin() as conn:
        for upd in updates:
            conn.execute(
                text("UPDATE song_comments SET user_nickname = :nickname WHERE comment_id = :comment_id"),
                {"comment_id": upd['comment_id'], "nickname": upd['nickname']}
            )
    logger.info(f"  批量更新 {len(updates)} 条")


if __name__ == "__main__":
    update_comment_nickname()