import pyodbc

# 查看实际安装的驱动（先运行这个确认）
print("已安装驱动:", pyodbc.drivers())

# 修改这里：Driver 18 而不是 17
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"  # ← 改成18
    "SERVER=localhost;"
    "DATABASE=MusicRecommendationDB;"
    "UID=sa;"
    "PWD=123456;"
    "TrustServerCertificate=yes;"  # Driver 18需要加这个
)

try:
    conn = pyodbc.connect(conn_str)
    print("✓ 连接成功！")
except Exception as e:
    print(f"✗ 连接失败: {e}")