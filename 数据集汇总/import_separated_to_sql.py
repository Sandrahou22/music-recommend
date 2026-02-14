# file: import_separated_to_sql.py
"""
分离数据导入SQL Server（简化版）
功能：
1. 按依赖顺序导入歌曲、用户、交互数据
2. 自动过滤外键约束失败的记录
3. 适配短ID格式（U000001, S000001等）
"""

import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text, inspect
import warnings
warnings.filterwarnings('ignore')

def clean_old_data(engine):
    """按依赖顺序清理旧数据"""
    print("\n1. 清理旧数据...")
    
    delete_order = [
        "DELETE FROM comment_likes",
        "DELETE FROM song_comments",
        "DELETE FROM recommendations",
        "DELETE FROM user_song_interaction",
        "DELETE FROM test_interactions",
        "DELETE FROM train_interactions",
        "DELETE FROM filtered_interactions",
        "DELETE FROM song_id_mapping",
        "DELETE FROM enhanced_song_features",
        "DELETE FROM enhanced_user_features",
        "DELETE FROM algorithm_performance_stats",
        "DELETE FROM system_config"
    ]
    
    with engine.begin() as conn:
        for sql in delete_order:
            try:
                result = conn.execute(text(sql))
                print(f"   ✅ {sql:50} 影响行数: {result.rowcount}")
            except Exception as e:
                if "doesn't exist" in str(e) or "对象名" in str(e):
                    print(f"   ⚠️ {sql:50} 表不存在，跳过")
                else:
                    print(f"   ❌ {sql:50} 失败: {e}")
                    raise

def import_song_features(engine, data_dir):
    """导入歌曲特征（直接读取CSV全部列）"""
    print("\n2. 导入歌曲特征...")
    song_file = os.path.join(data_dir, "all_song_features.csv")
    
    if not os.path.exists(song_file):
        print(f"   ❌ 歌曲文件不存在: {song_file}")
        return False
    
    song_df = pd.read_csv(song_file)
    print(f"   读取歌曲数据: {song_df.shape}")
    
    # 直接使用CSV全部列（列名已与数据库一致）
    try:
        song_df.to_sql('enhanced_song_features', engine, if_exists='append', index=False)
        print(f"   ✅ 歌曲特征导入成功: {len(song_df)} 条记录")
        return True
    except Exception as e:
        print(f"   ❌ 歌曲特征导入失败: {e}")
        return False

def import_user_features(engine, data_dir):
    """导入用户特征（分别处理internal/external）"""
    print("\n3. 导入用户特征...")
    
    for source_type in ['internal', 'external']:
        user_file = os.path.join(data_dir, source_type, "user_features.csv")
        if os.path.exists(user_file):
            user_df = pd.read_csv(user_file)
            # 确保source字段存在（兼容旧数据）
            if 'source' not in user_df.columns:
                user_df['source'] = source_type
            # 确保role字段存在
            if 'role' not in user_df.columns:
                user_df['role'] = 'user'
            try:
                user_df.to_sql('enhanced_user_features', engine, if_exists='append', index=False)
                print(f"   ✅ {source_type} 用户特征导入成功: {len(user_df)} 条")
            except Exception as e:
                print(f"   ❌ {source_type} 用户特征导入失败: {e}")
        else:
            print(f"   ⚠️ 文件不存在: {user_file}")

def import_interaction_data(engine, data_dir, source_type, table_name, file_name):
    """通用导入交互数据（自动跳过外键约束失败的记录）"""
    file_path = os.path.join(data_dir, source_type, file_name)
    if not os.path.exists(file_path):
        print(f"   ⚠️ 文件不存在: {file_path}")
        return
    
    df = pd.read_csv(file_path)
    print(f"   {source_type} {file_name}: {df.shape}")
    
    # 获取数据库中已存在的song_id和user_id
    with engine.connect() as conn:
        existing_songs = pd.read_sql("SELECT song_id FROM enhanced_song_features", conn)
        existing_users = pd.read_sql("SELECT user_id FROM enhanced_user_features", conn)
    
    valid_song_ids = set(existing_songs['song_id'].astype(str))
    valid_user_ids = set(existing_users['user_id'].astype(str))
    
    # 确保列类型为字符串
    df['song_id'] = df['song_id'].astype(str)
    df['user_id'] = df['user_id'].astype(str)
    
    # 过滤无效的外键
    original_count = len(df)
    df = df[df['song_id'].isin(valid_song_ids) & df['user_id'].isin(valid_user_ids)]
    filtered_count = len(df)
    
    if filtered_count == 0:
        print(f"   ⚠️ 所有记录均因外键约束被过滤，跳过导入")
        return
    
    if filtered_count < original_count:
        print(f"   ⚠️ 过滤了 {original_count - filtered_count} 条无效song_id/user_id记录")
    
    # 分批导入
    batch_size = 5000
    success_count = 0
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        try:
            batch.to_sql(table_name, engine, if_exists='append', index=False)
            success_count += len(batch)
            print(f"     进度: {success_count}/{filtered_count}")
        except Exception as e:
            print(f"     批次导入失败，尝试逐条插入...")
            for _, row in batch.iterrows():
                try:
                    row.to_frame().T.to_sql(table_name, engine, if_exists='append', index=False)
                    success_count += 1
                except:
                    continue
    
    print(f"   ✅ {source_type} {table_name} 导入成功: {success_count}/{filtered_count} 条")

def import_separated_data_to_sql():
    """主函数"""
    print("="*80)
    print("分离数据导入SQL Server（简化版）")
    print("="*80)
    
    # 数据库配置（请根据实际情况修改）
    db_config = {
        'server': 'localhost',
        'database': 'MusicRecommendationDB',
        'username': 'sa',
        'password': '123456',   # ← 改成您的密码
        'driver': 'ODBC Driver 18 for SQL Server'
    }
    
    conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                f"@{db_config['server']}/{db_config['database']}"
                f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
    
    engine = create_engine(conn_str, echo=False)
    data_dir = "separated_processed_data"
    
    if not os.path.exists(data_dir):
        print(f"错误: 数据目录不存在: {data_dir}")
        return
    
    # 1. 清理旧数据
    clean_old_data(engine)
    
    # 2. 导入歌曲特征（必须成功）
    song_success = import_song_features(engine, data_dir)
    if not song_success:
        print("\n❌ 歌曲特征导入失败，终止后续导入")
        return
    
    # 3. 导入用户特征
    import_user_features(engine, data_dir)
    
    # 4. 导入交互数据（按依赖顺序）
    print("\n4. 导入交互数据...")
    # filtered_interactions
    import_interaction_data(engine, data_dir, 'internal', 'filtered_interactions', 'interaction_matrix.csv')
    import_interaction_data(engine, data_dir, 'external', 'filtered_interactions', 'interaction_matrix.csv')
    
    # 5. 导入训练集
    print("\n5. 导入训练集...")
    import_interaction_data(engine, data_dir, 'internal', 'train_interactions', 'train_interactions.csv')
    import_interaction_data(engine, data_dir, 'external', 'train_interactions', 'train_interactions.csv')
    
    # 6. 导入测试集
    print("\n6. 导入测试集...")
    import_interaction_data(engine, data_dir, 'internal', 'test_interactions', 'test_interactions.csv')
    import_interaction_data(engine, data_dir, 'external', 'test_interactions', 'test_interactions.csv')
    
    # 7. 统计验证
    print("\n7. 导入统计验证...")
    try:
        with engine.connect() as conn:
            # 歌曲统计
            song_stats = pd.read_sql("""
                SELECT source, COUNT(*) as count 
                FROM enhanced_song_features 
                GROUP BY source
            """, conn)
            print("\n   📊 歌曲统计:")
            for _, row in song_stats.iterrows():
                print(f"     {row['source']}: {row['count']:,} 首")
            
            # 用户统计
            user_stats = pd.read_sql("""
                SELECT source, COUNT(*) as count 
                FROM enhanced_user_features 
                GROUP BY source
            """, conn)
            print("\n   📊 用户统计:")
            for _, row in user_stats.iterrows():
                print(f"     {row['source']}: {row['count']:,} 用户")
            
            # 交互统计（过滤后）
            interaction_stats = pd.read_sql("""
                SELECT u.source, COUNT(*) as count 
                FROM filtered_interactions i
                JOIN enhanced_user_features u ON i.user_id = u.user_id
                GROUP BY u.source
            """, conn)
            print("\n   📊 交互统计:")
            for _, row in interaction_stats.iterrows():
                print(f"     {row['source']}: {row['count']:,} 交互")
    except Exception as e:
        print(f"   ⚠️ 统计验证失败: {e}")
    
    print("\n" + "="*80)
    print("🎉 分离数据导入完成！")
    print("="*80)

if __name__ == "__main__":
    import_separated_data_to_sql()