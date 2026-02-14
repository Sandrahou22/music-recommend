# file: import_song_comments.py
"""
将原始歌曲评论数据导入到 song_comments 表
- 读取 song_comments_20260124_001212.csv
- 通过 song_id_mapping 表或 enhanced_song_features 表获取 unified_song_id
- 批量插入数据库
"""

import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def import_song_comments():
    print("="*80)
    print("导入原始评论数据到 song_comments 表")
    print("="*80)
    
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
    engine = create_engine(conn_str, echo=False)
    
    # ---------- 2. 读取原始评论文件 ----------
    csv_file = "song_comments_20260124_001212.csv"
    if not os.path.exists(csv_file):
        print(f"❌ 文件不存在: {csv_file}")
        return
    
    print(f"📄 读取评论文件: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"   原始记录数: {len(df)}")
    
    # ---------- 3. 获取歌曲ID映射 ----------
    with engine.connect() as conn:
        # 方法1：从 song_id_mapping 表获取（优先）
        mapping_query = """
            SELECT original_song_id, unified_song_id 
            FROM song_id_mapping 
            WHERE original_song_id IS NOT NULL
        """
        mapping_df = pd.read_sql(mapping_query, conn)

        if len(mapping_df) == 0:
            # 方法2：直接从 enhanced_song_features 表获取（注意列名已改为 original_song_id）
            song_query = "SELECT song_id, original_song_id FROM enhanced_song_features WHERE original_song_id IS NOT NULL"
            song_df = pd.read_sql(song_query, conn)
            mapping_dict = dict(zip(song_df['original_song_id'].astype(str), song_df['song_id']))
        else:
            mapping_dict = dict(zip(mapping_df['original_song_id'].astype(str), mapping_df['unified_song_id']))
    
    print(f"✅ 获取歌曲映射: {len(mapping_dict)} 条")
    
    # ---------- 4. 数据预处理 ----------
    df = df.dropna(subset=['song_id', 'content'])
    df['song_id'] = df['song_id'].astype(str)
    df['unified_song_id'] = df['song_id'].map(mapping_dict)
    
    # 过滤无效映射
    original_count = len(df)
    df = df[df['unified_song_id'].notna()]
    print(f"✅ 有效映射: {len(df)} 条 (过滤 {original_count - len(df)} 条)")
    
    if len(df) == 0:
        print("❌ 无有效映射数据，请检查歌曲映射表")
        return
    
    # ---------- 5. 准备插入数据 ----------
    # 重命名列以匹配数据库表
    df = df.rename(columns={
        'comment_id': 'original_comment_id',
        'user_id': 'original_user_id',
        'nickname': 'user_nickname',  # CSV中可能没有，需要确认
        'content': 'content',
        'liked_count': 'liked_count',
        'time': 'comment_time'
    })
    
    # 处理时间字段
    if 'comment_time' in df.columns:
        df['comment_time'] = pd.to_datetime(df['comment_time'], errors='coerce')
    
    # 添加统一歌曲ID列
    df['unified_song_id'] = df['unified_song_id']
    
    # 选择需要插入的列
    insert_cols = [
        'unified_song_id', 'original_comment_id', 'original_user_id',
        'user_nickname', 'content', 'liked_count', 'comment_time'
    ]
    insert_cols = [col for col in insert_cols if col in df.columns]
    
    insert_df = df[insert_cols].copy()
    
    # ---------- 6. 分批导入 ----------
    batch_size = 1000
    total = len(insert_df)
    success = 0
    
    print(f"\n⏳ 开始导入评论数据，共 {total} 条...")
    
    for i in range(0, total, batch_size):
        batch = insert_df.iloc[i:i+batch_size]
        try:
            batch.to_sql('song_comments', engine, if_exists='append', index=False)
            success += len(batch)
            print(f"   进度: {success}/{total} ({success/total*100:.1f}%)")
        except Exception as e:
            print(f"   ❌ 批次导入失败，尝试逐条插入...")
            # 逐条插入，跳过失败的单条
            for _, row in batch.iterrows():
                try:
                    row.to_frame().T.to_sql('song_comments', engine, if_exists='append', index=False)
                    success += 1
                except Exception as e2:
                    print(f"     跳过评论: {e2}")
            print(f"   进度: {success}/{total} ({success/total*100:.1f}%)")
    
    print(f"\n✅ 评论数据导入完成! 成功: {success} 条")
    
    # ---------- 7. 统计验证 ----------
    with engine.connect() as conn:
        count = pd.read_sql("SELECT COUNT(*) as cnt FROM song_comments", conn)
        print(f"\n📊 song_comments 表现在有 {count.iloc[0]['cnt']} 条评论")
        
        song_count = pd.read_sql("""
            SELECT COUNT(DISTINCT unified_song_id) as cnt 
            FROM song_comments
        """, conn)
        print(f"   覆盖歌曲数: {song_count.iloc[0]['cnt']}")


if __name__ == "__main__":
    import_song_comments()