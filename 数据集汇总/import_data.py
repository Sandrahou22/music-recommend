import pandas as pd
from sqlalchemy import create_engine, inspect, text  # ← 添加 text
import os

# ==================== 配置区 ====================
DB_CONFIG = {
    'server': 'localhost',
    'database': 'MusicRecommendationDB',
    'username': 'sa',
    'password': '123456',  # ← 改成您的密码
    'driver': 'ODBC Driver 18 for SQL Server'
}

DATA_DIR = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\aligned_data_final_optimized"

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
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return [col['name'] for col in columns]

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
        df = pd.read_csv(file_path, low_memory=False)  # 加low_memory=False避免警告
        original_cols = len(df.columns)
        
        # 2. 获取SQL列并过滤
        sql_columns = get_sql_columns(table_name)
        common_cols = [col for col in df.columns if col in sql_columns]
        df = df[common_cols]
        
        print(f"   CSV列: {original_cols} → SQL匹配: {len(common_cols)} (忽略{original_cols-len(common_cols)}列)")
        
        # 3. 处理空值
        df = df.where(pd.notnull(df), None)
        
        # 4. 清空表（关键修复：用text()包装SQL）
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table_name}"))
        print(f"   已清空表")
        
        # 5. 导入数据
        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000)
        
        print(f"✅ 成功导入 {len(df)} 行\n")
        
    except Exception as e:
        print(f"❌ 失败：{e}\n")

# 验证
print("="*50)
print("验证数据量：")
for _, table_name in IMPORT_ORDER:
    try:
        result = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", engine)
        count = result.iloc[0]['cnt']
        print(f"  {table_name}: {count} 条")
    except Exception as e:
        print(f"  {table_name}: 查询失败")