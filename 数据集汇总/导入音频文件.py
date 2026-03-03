import os
import pyodbc
import logging

# ========== 配置 ==========
AUDIO_DIR = r"C:\Users\小侯\Desktop\学校作业\毕业设计\music_recommendation_api\static\audio_files"
SERVER = 'localhost'
DATABASE = 'MusicRecommendationDB'

# 常用驱动列表（按优先级排序）
DRIVERS = [
    'ODBC Driver 17 for SQL Server',
    'ODBC Driver 13 for SQL Server',
    'ODBC Driver 11 for SQL Server',
    'SQL Server Native Client 11.0',
    'SQL Server'
]

# 日志设置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_connection():
    """尝试使用不同驱动连接数据库，返回第一个成功的连接"""
    for driver in DRIVERS:
        try:
            conn_str = f'DRIVER={{{driver}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            logger.info(f"成功使用驱动: {driver}")
            return conn
        except Exception as e:
            logger.debug(f"驱动 {driver} 连接失败: {e}")
            continue
    raise Exception("无法找到可用的 ODBC 驱动，请安装 SQL Server ODBC 驱动（例如 ODBC Driver 17 for SQL Server）。")

def update_audio_paths():
    if not os.path.exists(AUDIO_DIR):
        logger.error(f"目录不存在: {AUDIO_DIR}")
        return

    files = [f for f in os.listdir(AUDIO_DIR) if f.lower().endswith('.mp3')]
    logger.info(f"找到 {len(files)} 个 MP3 文件")

    if not files:
        logger.info("没有 MP3 文件需要处理")
        return

    conn = get_connection()
    cursor = conn.cursor()

    updated = 0
    not_found = 0
    error = 0

    for filename in files:
        song_id = os.path.splitext(filename)[0]          # 去掉扩展名得到 song_id
        # 构造绝对路径（使用 os.path.join 自动处理分隔符）
        audio_path = os.path.join(AUDIO_DIR, filename)   # 例如：C:\Users\小侯\Desktop\...\E200001.mp3

        try:
            cursor.execute(
                "UPDATE enhanced_song_features SET audio_path = ? WHERE song_id = ?",
                (audio_path, song_id)
            )
            if cursor.rowcount > 0:
                logger.info(f"更新成功: {song_id} -> {audio_path}")
                updated += 1
            else:
                logger.warning(f"未找到歌曲: {song_id}，跳过")
                not_found += 1
        except Exception as e:
            logger.error(f"更新 {song_id} 失败: {e}")
            error += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"完成！更新 {updated} 条，未找到 {not_found} 条，错误 {error} 条")

if __name__ == "__main__":
    update_audio_paths()