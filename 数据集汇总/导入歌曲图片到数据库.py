import pandas as pd
import pyodbc
import os

# ========== 配置 ==========
SERVER = 'localhost'           # 如果是命名实例，改成 localhost\SQLEXPRESS
DATABASE = 'MusicRecommendationDB'
# 常用驱动列表
DRIVERS = [
    'ODBC Driver 17 for SQL Server',
    'ODBC Driver 13 for SQL Server',
    'ODBC Driver 11 for SQL Server',
    'SQL Server Native Client 11.0',
    'SQL Server'
]
BASE_URL = 'http://127.0.0.1:5000'
STATIC_PATH = '/static/song_covers/'
CSV_FILE = 'song_cover_success.csv'

# ========== 测试连接 ==========
conn = None
for driver in DRIVERS:
    try:
        conn_str = f'DRIVER={{{driver}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        print(f"成功使用驱动: {driver}")
        break
    except Exception as e:
        print(f"驱动 {driver} 连接失败: {e}")
        continue

if conn is None:
    print("错误：无法找到可用的 SQL Server ODBC 驱动。")
    print("请安装 ODBC Driver 17 for SQL Server 或其他兼容驱动。")
    print("可运行以下代码查看已安装驱动：")
    print("import pyodbc; print(pyodbc.drivers())")
    exit(1)

# ========== 读取 CSV ==========
if not os.path.exists(CSV_FILE):
    print(f"错误：找不到 {CSV_FILE}，请先运行爬虫脚本生成该文件。")
    exit(1)

df = pd.read_csv(CSV_FILE)
cursor = conn.cursor()

success_count = 0
for _, row in df.iterrows():
    song_id = row['song_id']
    file_path = row['file_path']
    filename = os.path.basename(file_path)
    cover_url = f"{BASE_URL}{STATIC_PATH}{filename}"
    try:
        cursor.execute(
            "UPDATE enhanced_song_features SET cover_path = ? WHERE song_id = ?",
            (cover_url, song_id)
        )
        if cursor.rowcount > 0:
            print(f"✓ 更新 {song_id} -> {cover_url}")
            success_count += 1
        else:
            print(f"⚠ 未找到歌曲 {song_id}，跳过")
    except Exception as e:
        print(f"✗ 更新 {song_id} 失败: {e}")

conn.commit()
cursor.close()
conn.close()
print(f"\n数据库更新完成！成功更新 {success_count} 条记录。")