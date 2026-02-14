import os
import sys
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# 加载.env文件
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

class Config:
    """基础配置类 - 包含所有必需属性"""
    
    # Flask核心配置
    SECRET_KEY: str = os.getenv('SECRET_KEY', 'music-recommendation-secret-key-dev-only')
    DEBUG: bool = os.getenv('DEBUG', 'True').lower() in ('true', '1', 'yes')
    FLASK_ENV: str = os.getenv('FLASK_ENV', 'development')

    # 管理员配置 - 【新增】
    ADMIN_TOKEN: str = os.getenv('ADMIN_TOKEN', 'admin-secret-token-2024-change-in-production')
    
    # 路径配置（使用Path处理跨平台）
    BASE_DIR: Path = Path(__file__).parent.resolve()
    PROJECT_ROOT: Path = BASE_DIR.parent
    
    # 数据集目录（支持环境变量覆盖）
    DATASET_DIR: Path = Path(os.getenv(
        'DATASET_DIR', 
        PROJECT_ROOT / '数据集' / '数据集汇总'
    ))
    
    # 原代码文件路径
    RECOMMENDER_CODE_PATH: Path = Path(os.getenv(
        'RECOMMENDER_CODE_PATH',
        DATASET_DIR / 'separated_music_recommender.py'   # ← 新文件名
    ))
    
    # API限流配置（recommender_service.py需要）
    RATELIMIT_ENABLED: bool = os.getenv('RATELIMIT_ENABLED', 'True').lower() == 'true'
    RATELIMIT_STORAGE_URI: str = os.getenv('RATELIMIT_STORAGE_URI', 'memory://')
    DEFAULT_RATE_LIMIT: str = "100 per minute"
    
    # 推荐系统配置（recommender_service.py需要）
    MAX_RECOMMEND_COUNT: int = min(int(os.getenv('MAX_RECOMMEND_COUNT', 50)), 100)
    DEFAULT_RECOMMEND_COUNT: int = min(int(os.getenv('DEFAULT_RECOMMEND_COUNT', 10)), MAX_RECOMMEND_COUNT)
    CACHE_RECOMMENDATIONS_TTL: int = int(os.getenv('CACHE_TTL', 1800))  # 30分钟
    
    # 熔断器配置（recommender_service.py需要）- 关键修复
    CIRCUIT_BREAKER_THRESHOLD: int = int(os.getenv('CIRCUIT_BREAKER_THRESHOLD', 5))
    CIRCUIT_BREAKER_TIMEOUT: int = int(os.getenv('CIRCUIT_BREAKER_TIMEOUT', 60))
    
    # SQL Server配置
    # DB_CONFIG: dict = {
       # 'server': os.getenv('DB_SERVER', 'localhost'),
       # 'database': os.getenv('DB_NAME', 'MusicRecommendationDB'),
       # 'username': os.getenv('DB_USER', 'sa'),
       # 'password': os.getenv('DB_PASSWORD', '123456'),  # 建议.env覆盖
       # 'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
       # 'encrypt': os.getenv('DB_ENCRYPT', 'no'),
       # 'pool_size': int(os.getenv('DB_POOL_SIZE', 5)),
       # 'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', 10)),
       # 'pool_recycle': int(os.getenv('DB_POOL_RECYCLE', 3600))
    # }
    
     # SQL Server配置 - 使用Windows身份验证
    # SQL Server配置 - 使用Windows身份验证（无需密码）
    DB_CONFIG: dict = {
        'server': os.getenv('DB_SERVER', 'localhost'),  # 或 localhost\\SQLEXPRESS
        'database': os.getenv('DB_NAME', 'MusicRecommendationDB'),
        'username': '',  # Windows验证留空
        'password': '',  # Windows验证留空
        'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
        'trusted_connection': 'yes',  # 关键：使用Windows身份验证
        'pool_size': int(os.getenv('DB_POOL_SIZE', 5)),
        'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', 10)),
        'pool_recycle': int(os.getenv('DB_POOL_RECYCLE', 3600))
    }

    # 缓存配置 - 【新增】
    CACHE_TYPE: str = os.getenv('CACHE_TYPE', 'SimpleCache')
    CACHE_DEFAULT_TIMEOUT: int = int(os.getenv('CACHE_DEFAULT_TIMEOUT', 300))
    
    # 文件上传配置 - 【新增】
    MAX_CONTENT_LENGTH: int = 16 * 1024 * 1024  # 16MB
    UPLOAD_FOLDER: Path = BASE_DIR / 'uploads'
    ALLOWED_EXTENSIONS: set = {'mp3', 'wav', 'ogg', 'flac', 'm4a'}
    
    @classmethod
    def get_db_connection_string(cls) -> str:
        """生成SQL Server连接字符串 - Windows身份验证"""
        cfg = cls.DB_CONFIG
        driver = cfg['driver'].replace(' ', '+')
        
        # Windows身份验证连接字符串（无需用户名密码）
        return (f"mssql+pyodbc://@{cfg['server']}/{cfg['database']}"
                f"?driver={driver}&Trusted_Connection=yes&Encrypt=no&TrustServerCertificate=yes")

    @classmethod
    def validate(cls) -> None:
        """启动时验证配置"""
        errors = []
        
        # 如果不是 Windows 身份验证，才检查密码
        if cls.DB_CONFIG.get('trusted_connection') != 'yes':
            if not cls.DB_CONFIG.get('password'):
                errors.append("DB_PASSWORD 环境变量未设置（或使用Windows身份验证）")
        
        if not cls.RECOMMENDER_CODE_PATH.exists():
            errors.append(f"推荐系统代码文件不存在: {cls.RECOMMENDER_CODE_PATH}")
        
        if errors:
            raise ValueError(f"配置验证失败:\n" + "\n".join(f"  - {e}" for e in errors))
    
    # @classmethod
    # def get_db_connection_string(cls) -> str:
    #    """生成SQL Server连接字符串"""
    #    cfg = cls.DB_CONFIG
    #    driver = cfg['driver'].replace(' ', '+')
    #    # 添加 TrustServerCertificate=yes 解决驱动18的连接问题
    #    return (f"mssql+pyodbc://{cfg['username']}:{cfg['password']}"
    #            f"@{cfg['server']}/{cfg['database']}"
    #            f"?driver={driver}&Encrypt={cfg['encrypt']}&TrustServerCertificate=yes")

class DevelopmentConfig(Config):
    """开发环境配置"""
    DEBUG = True
    # 开发环境使用简单缓存
    CACHE_TYPE = "SimpleCache"
    CACHE_DEFAULT_TIMEOUT = 300
    # 开发环境管理员令牌（用于测试）
    ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', 'dev-admin-token-test-only')

class ProductionConfig(Config):
    """生产环境配置"""
    DEBUG = False
    # 生产环境推荐Redis（如未安装则使用SimpleCache）
    CACHE_TYPE = os.getenv('CACHE_TYPE', 'SimpleCache')
    CACHE_REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    # 生产环境必须设置强密码
    ADMIN_TOKEN = os.getenv('ADMIN_TOKEN')
    if not ADMIN_TOKEN:
        raise ValueError("生产环境必须设置ADMIN_TOKEN环境变量")

class TestingConfig(Config):
    """测试环境配置"""
    TESTING = True
    DEBUG = True
    DB_CONFIG = {**Config.DB_CONFIG, 'database': 'MusicRecommendationDB_Test'}
    ADMIN_TOKEN = 'test-admin-token'

# 配置映射（app.py需要）
config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}