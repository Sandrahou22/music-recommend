# config.py
# 网易云音乐爬虫配置

class Config:
    # 网络请求配置
    REQUEST_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://music.163.com/',
        'Accept': 'application/json, text/plain, */*',
    }
    
    # API端点
    BASE_URL = "https://music.163.com"
    API_URL = "https://music.163.com/api"
    
    # 爬虫控制
    MAX_RETRIES = 3
    REQUEST_DELAY = 1.5  # 基本延迟
    BATCH_DELAY = 5.0    # 批次间延迟
    
    # 数据存储
    OUTPUT_DIR = "./data"
    LOG_FILE = "./logs/crawler.log"
    
    # 数据库配置（如果需要）
    DATABASE = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'password',
        'database': 'music_recommendation'
    }