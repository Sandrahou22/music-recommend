import os
import pyodbc

AVATAR_DIR = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\avatars'
SERVER = 'localhost'
DATABASE = 'MusicRecommendationDB'

# 尝试的驱动列表（按优先级排序）
DRIVERS = [
    'ODBC Driver 17 for SQL Server',
    'ODBC Driver 13 for SQL Server',
    'ODBC Driver 11 for SQL Server',
    'SQL Server Native Client 11.0',
    'SQL Server'
]

# 测试连接
conn = None
for driver in DRIVERS:
    try:
        conn_str = f'DRIVER={{{driver}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        print(f"成功使用驱动: {driver}")
        break
    except:
        continue

if conn is None:
    print("错误：无法找到可用的 SQL Server ODBC 驱动。")
    print("请安装 ODBC Driver 17 for SQL Server 或其他兼容驱动。")
    exit(1)

cursor = conn.cursor()
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif'}

updated_count = 0
not_found_count = 0
error_count = 0

for filename in os.listdir(AVATAR_DIR):
    ext = os.path.splitext(filename)[1].lower()
    if ext not in IMAGE_EXTS:
        continue
    user_id = os.path.splitext(filename)[0]
    full_path = os.path.join(AVATAR_DIR, filename)

    try:
        cursor.execute(
            "UPDATE enhanced_user_features SET avatar_path = ? WHERE user_id = ?",
            (full_path, user_id)
        )
        if cursor.rowcount > 0:
            print(f"✓ 已更新 user_id: {user_id}")
            updated_count += 1
        else:
            print(f"⚠ 未找到 user_id: {user_id}")
            not_found_count += 1
    except Exception as e:
        print(f"✗ 更新 user_id {user_id} 出错: {e}")
        error_count += 1

conn.commit()
cursor.close()
conn.close()

print("\n========== 完成 ==========")
print(f"成功更新: {updated_count}")
print(f"未匹配用户: {not_found_count}")
print(f"错误: {error_count}")