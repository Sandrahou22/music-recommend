import pandas as pd
from sqlalchemy import create_engine, inspect, text
import os
import numpy as np

# ==================== 配置区 ====================
DB_CONFIG = {
    'server': 'localhost',
    'database': 'MusicRecommendationDB',
    'username': 'sa',
    'password': '123456',  # ← 改成您的密码
    'driver': 'ODBC Driver 18 for SQL Server'
}

DATA_DIR = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\aligned_data_optimized"

IMPORT_ORDER = [
    ('enhanced_song_features.csv', 'enhanced_song_features'),
    ('enhanced_user_features.csv', 'enhanced_user_features'),
    ('filtered_interactions.csv', 'filtered_interactions'),
    ('train_interactions.csv', 'train_interactions'),
    ('test_interactions.csv', 'test_interactions')
]
# ==================== 配置结束 ====================

conn_str = (f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
            f"?driver={DB_CONFIG['driver'].replace(' ', '+')}&Encrypt=no")
engine = create_engine(conn_str)

def get_sql_columns(table_name):
    """获取SQL表的列名"""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return [col['name'] for col in columns]

def clean_duplicate_columns(df, table_name):
    """清理重复列名"""
    print(f"  清理 {table_name} 的重复列...")
    
    # 记录原始列
    original_cols = set(df.columns)
    
    # 1. 删除所有以 _x 或 _y 结尾的列
    duplicate_cols = [col for col in df.columns if col.endswith('_x') or col.endswith('_y')]
    if duplicate_cols:
        print(f"    删除重复列: {duplicate_cols}")
        df = df.drop(columns=duplicate_cols)
    
    # 2. 处理其他可能的重复
    # 如果有同名但大小写不同的列，保留第一个
    lower_cols = {}
    cols_to_drop = []
    
    for col in df.columns:
        lower = col.lower()
        if lower in lower_cols:
            # 已存在同名列（忽略大小写），删除当前列
            cols_to_drop.append(col)
        else:
            lower_cols[lower] = col
    
    if cols_to_drop:
        print(f"    删除大小写重复列: {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)
    
    # 3. 验证清理结果
    final_cols = set(df.columns)
    removed = original_cols - final_cols
    if removed:
        print(f"    共删除 {len(removed)} 个重复列")
    
    return df

def verify_columns(df, table_name, sql_columns):
    """验证CSV列与SQL列是否匹配"""
    csv_cols = set(df.columns)
    sql_cols_set = set(sql_columns)
    
    # 检查缺失的列
    missing_in_sql = csv_cols - sql_cols_set
    extra_in_sql = sql_cols_set - csv_cols
    
    if missing_in_sql:
        print(f"   警告: CSV中有但SQL表中没有的列: {missing_in_sql}")
        print(f"   请运行以下SQL添加这些列:")
        for col in missing_in_sql:
            # 猜测数据类型
            if col in ['weight_sum', 'weight_mean', 'weight_std', 'total_weight_mean', 'avg_weight', 
                      'final_popularity_norm', 'recency_score', 'danceability', 'energy', 'valence',
                      'mood_score', 'energy_dance', 'acousticness', 'instrumentalness', 'liveness',
                      'speechiness']:
                dtype = 'FLOAT'
            elif col in ['unique_users', 'total_interactions', 'song_age', 'duration_ms', 'playlist_count',
                        'similar_songs_count', 'comment_count', 'total_likes', 'tag_count']:
                dtype = 'INT'
            elif col in ['popularity_tier', 'genre_clean', 'age_group', 'activity_level', 'popularity_group']:
                dtype = 'NVARCHAR(100)'
            elif col in ['song_name', 'artists', 'album']:
                dtype = 'NVARCHAR(500)'
            elif col in ['genre', 'language', 'province', 'city']:
                dtype = 'NVARCHAR(200)'
            elif col in ['interaction_types']:
                dtype = 'NVARCHAR(300)'
            else:
                dtype = 'NVARCHAR(200)'
            
            print(f"   ALTER TABLE {table_name} ADD {col} {dtype} NULL;")
    
    if extra_in_sql:
        print(f"   注意: SQL表中有但CSV中没有的列: {extra_in_sql}")
        print(f"   这些列将保持NULL或默认值")
    
    return len(missing_in_sql) == 0

def validate_and_fix_data(df, table_name):
    """验证数据并修复常见问题"""
    print(f"  验证 {table_name} 数据...")
    
    # 1. 处理无限大/无限小值
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            # 对于浮点数列，用中位数填充
            if df[col].dtype in ['float64', 'float32']:
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)
            # 对于整数列，用0填充
            else:
                df[col].fillna(0, inplace=True)
    
    # 2. 处理字符串列的空值
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        if col in df.columns:
            df[col].fillna('', inplace=True)
    
    # 3. 截断过长的字符串（避免SQL Server截断错误）
    max_lengths = {
        'song_name': 500,
        'artists': 450,
        'album': 500,
        'genre': 200,
        'language': 50,
        'province': 50,
        'city': 50,
        'interaction_types': 200,
        'nickname': 100,
        'genre_clean': 200,
        'popularity_tier': 50,
        'age_group': 50,
        'activity_level': 50
    }
    
    for col, max_len in max_lengths.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str[:max_len]
    
    return df

print("开始导入数据...\n")

# 按顺序导入：先主表，后从表（避免外键冲突）
for csv_file, table_name in IMPORT_ORDER:
    file_path = os.path.join(DATA_DIR, csv_file)
    
    if not os.path.exists(file_path):
        print(f"⚠️ 跳过：找不到 {csv_file}")
        continue
    
    try:
        print(f"正在导入 {csv_file}")
        
        # 1. 读CSV
        df = pd.read_csv(file_path, low_memory=False)
        original_rows = len(df)
        original_cols = len(df.columns)
        
        print(f"  读取: {original_rows}行 × {original_cols}列")
        
        # 2. 清理重复列名
        df = clean_duplicate_columns(df, table_name)
        
        # 3. 数据验证和修复
        df = validate_and_fix_data(df, table_name)
        
        # 4. 获取SQL列
        sql_columns = get_sql_columns(table_name)
        
        # 5. 验证列匹配
        columns_match = verify_columns(df, table_name, sql_columns)
        
        # 对于歌曲特征表，如果列不匹配需要特别注意
        if not columns_match and table_name == 'enhanced_song_features':
            print(f"   重要: 歌曲特征列不匹配，请先更新数据库表结构")
            print(f"   是否继续导入? (输入 'y' 继续，其他键跳过): ")
            user_input = input().strip().lower()
            if user_input != 'y':
                print(f"   跳过 {csv_file}\n")
                continue
        
        # 6. 过滤列，只保留SQL中有的
        common_cols = [col for col in df.columns if col in sql_columns]
        df = df[common_cols]
        
        print(f"   列匹配: {original_cols} → {len(df.columns)} (过滤{original_cols-len(df.columns)}列)")
        
        # 7. 处理空值
        df = df.where(pd.notnull(df), None)
        
        # 8. 清空表（关键修复：用text()包装SQL）
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table_name}"))
        print(f"   已清空表")
        
        # 9. 导入数据
        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
        
        print(f"✅ 成功导入 {len(df)} 行\n")
        
    except Exception as e:
        print(f"❌ 失败：{str(e)}")
        import traceback
        print(traceback.format_exc())
        print()

# 验证
print("="*50)
print("验证数据量：")
for _, table_name in IMPORT_ORDER:
    try:
        result = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", engine)
        count = result.iloc[0]['cnt']
        print(f"  {table_name}: {count} 条")
    except Exception as e:
        print(f"  {table_name}: 查询失败 - {e}")

# 额外的验证查询
print("\n" + "="*50)
print("数据质量检查：")

try:
    # 检查歌曲表
    song_stats = pd.read_sql("""
        SELECT 
            COUNT(*) as total_songs,
            COUNT(DISTINCT song_id) as unique_songs,
            AVG(final_popularity) as avg_popularity,
            MIN(final_popularity) as min_popularity,
            MAX(final_popularity) as max_popularity
        FROM enhanced_song_features
    """, engine)
    
    if not song_stats.empty:
        row = song_stats.iloc[0]
        print(f"歌曲表:")
        print(f"  总数: {row['total_songs']:,}")
        print(f"  唯一ID: {row['unique_songs']:,}")
        print(f"  流行度: 平均{row['avg_popularity']:.1f}, 范围[{row['min_popularity']:.1f}, {row['max_popularity']:.1f}]")
    
    # 检查用户表
    user_stats = pd.read_sql("""
        SELECT 
            COUNT(*) as total_users,
            COUNT(DISTINCT user_id) as unique_users,
            AVG(total_interactions) as avg_interactions,
            SUM(total_interactions) as total_interactions
        FROM enhanced_user_features
    """, engine)
    
    if not user_stats.empty:
        row = user_stats.iloc[0]
        print(f"用户表:")
        print(f"  总数: {row['total_users']:,}")
        print(f"  唯一ID: {row['unique_users']:,}")
        print(f"  平均交互数: {row['avg_interactions']:.1f}")
        print(f"  总交互数: {row['total_interactions']:,}")
    
except Exception as e:
    print(f"数据质量检查失败: {e}")