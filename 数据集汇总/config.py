"""
配置文件 - 数据库连接配置和路径配置
"""
import os
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).parent

# 数据文件路径
DATA_DIR = Path(r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\processed_data_complete")

# 文件列表
DATA_FILES = {
    'song_features': DATA_DIR / 'song_features.csv',
    'user_features': DATA_DIR / 'user_features.csv',
    'interaction_matrix': DATA_DIR / 'interaction_matrix.csv',
    'train_interactions': DATA_DIR / 'train_interactions.csv',
    'test_interactions': DATA_DIR / 'test_interactions.csv'
}

# 输出目录
OUTPUT_DIR = BASE_DIR / 'analysis_output'
OUTPUT_DIR.mkdir(exist_ok=True)

# SQL Server数据库配置
DB_CONFIG = {
    'server': 'localhost',  # SQL Server地址
    'database': 'MusicRecommendationDB',  # 数据库名称
    'username': 'sa',  # 用户名
    'password': 'YourPassword123',  # 密码，请修改为你的密码
    'driver': 'ODBC Driver 17 for SQL Server',  # ODBC驱动
    'port': 1433,  # 端口
    'trusted_connection': 'no',  # 是否使用Windows身份验证
}

# 数据类型映射配置
DATA_TYPE_MAPPING = {
    # Pandas到SQL Server的数据类型映射
    'int64': 'INT',
    'int32': 'INT',
    'float64': 'FLOAT',
    'float32': 'FLOAT',
    'bool': 'BIT',
    'datetime64[ns]': 'DATETIME',
    'object': 'NVARCHAR(255)',  # 默认字符串类型
}

# 列名特殊映射
COLUMN_TYPE_SPECIAL_MAPPING = {
    # 特定列名的数据类型映射
    'id$': 'VARCHAR(50)',  # 以id结尾的列
    '_id$': 'VARCHAR(50)',  # 以_id结尾的列
    'name$': 'NVARCHAR(500)',  # 以name结尾的列
    'title$': 'NVARCHAR(500)',  # 以title结尾的列
    'description$': 'NVARCHAR(1000)',  # 以description结尾的列
    'date$': 'DATETIME',  # 以date结尾的列
    'time$': 'DATETIME',  # 以time结尾的列
    'count$': 'INT',  # 以count结尾的列
    'total$': 'INT',  # 以total结尾的列
    'score$': 'FLOAT',  # 以score结尾的列
    'ratio$': 'FLOAT',  # 以ratio结尾的列
    'percentage$': 'DECIMAL(5,2)',  # 以percentage结尾的列
    'price$': 'DECIMAL(10,2)',  # 以price结尾的列
    'amount$': 'DECIMAL(10,2)',  # 以amount结尾的列
}

def get_file_info():
    """获取文件信息"""
    file_info = {}
    for key, file_path in DATA_FILES.items():
        if file_path.exists():
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            file_info[key] = {
                'path': str(file_path),
                'exists': True,
                'size_mb': size_mb
            }
        else:
            file_info[key] = {
                'path': str(file_path),
                'exists': False,
                'size_mb': 0
            }
    return file_info

def check_data_files():
    """检查数据文件是否存在"""
    print("检查数据文件...")
    print("-" * 60)
    
    file_info = get_file_info()
    missing_files = []
    
    for key, info in file_info.items():
        if info['exists']:
            print(f"✓ {key}: {info['size_mb']:.2f} MB")
        else:
            print(f"✗ {key}: 文件不存在")
            missing_files.append(key)
    
    if missing_files:
        print(f"\n警告: 缺少 {len(missing_files)} 个文件: {missing_files}")
        return False
    else:
        print(f"\n✅ 所有数据文件都存在")
        return True

if __name__ == "__main__":
    # 检查配置文件
    print("配置文件检查:")
    print(f"数据目录: {DATA_DIR}")
    print(f"数据库配置: {DB_CONFIG['server']}:{DB_CONFIG['port']}")
    
    # 检查数据文件
    check_data_files()