"""
极简版 - 只获取字段和列数
"""
import pandas as pd
from pathlib import Path

# 文件路径 - 使用原始字符串并确保路径正确
# 方法1: 使用原始字符串（在字符串前加r）
data_dir = Path(r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\processed_data_complete")

# 方法2: 使用正斜杠（推荐，跨平台兼容）
# data_dir = Path("C:/Users/小侯/Desktop/学校作业/毕业设计/数据集/数据集汇总/processed_data_complete")

# 方法3: 使用双反斜杠
# data_dir = Path("C:\\Users\\小侯\\Desktop\\学校作业\\毕业设计\\数据集\\数据集汇总\\processed_data_complete")

# 检查目录是否存在
if not data_dir.exists():
    print(f"❌ 目录不存在: {data_dir}")
    print("请检查路径是否正确")
    exit(1)

files = {
    '歌曲特征': 'song_features.csv',
    '用户特征': 'user_features.csv',
    '交互矩阵': 'interaction_matrix.csv',
    '训练集': 'train_interactions.csv',
    '测试集': 'test_interactions.csv'
}

print("📋 文件结构分析")
print("="*50)

results = {}

for name, filename in files.items():
    filepath = data_dir / filename
    if filepath.exists():
        try:
            # 只读取列名，不加载数据
            df = pd.read_csv(filepath, nrows=0, encoding='utf-8')
            columns = list(df.columns)
            results[name] = {
                'filename': filename,
                'column_count': len(columns),
                'columns': columns
            }
            
            print(f"\n{name} ({filename}):")
            print(f"  列数: {len(columns)}")
            print(f"  字段: {', '.join(columns[:8])}" + 
                  (f" ... (共{len(columns)}个字段)" if len(columns) > 8 else ""))
            
            # 如果有时间，显示一些示例
            if len(columns) > 0:
                try:
                    # 读取第一行查看数据类型
                    df_sample = pd.read_csv(filepath, nrows=1, encoding='utf-8')
                    print(f"  示例数据类型:")
                    for col in columns[:5]:  # 只显示前5列的数据类型
                        dtype = df_sample[col].dtype
                        sample = str(df_sample[col].iloc[0])[:30] if not pd.isna(df_sample[col].iloc[0]) else "空值"
                        print(f"    {col}: {dtype} (示例: {sample})")
                except:
                    pass
                
        except Exception as e:
            print(f"\n{name}: 读取失败 - {e}")
    else:
        print(f"\n{name}: 文件不存在 - {filepath}")

print("\n" + "="*50)
print("📊 汇总统计:")
print("="*50)

# 汇总统计
for name, data in results.items():
    print(f"{name}: {data['column_count']}个字段")

print("\n" + "="*50)
print("💡 数据库设计建议:")
print("="*50)

# 数据库设计建议
if '歌曲特征' in results:
    print("\n1. 歌曲表 (Songs):")
    print(f"   主键: song_id")
    print(f"   字段数: {results['歌曲特征']['column_count']}")
    print(f"   关键字段: {', '.join([c for c in results['歌曲特征']['columns'] if 'id' in c.lower() or 'name' in c.lower()][:5])}")

if '用户特征' in results:
    print("\n2. 用户表 (Users):")
    print(f"   主键: user_id")
    print(f"   字段数: {results['用户特征']['column_count']}")
    print(f"   关键字段: {', '.join([c for c in results['用户特征']['columns'] if 'id' in c.lower() or 'name' in c.lower() or 'age' in c][:5])}")

if '交互矩阵' in results:
    print("\n3. 交互表 (UserSongInteractions):")
    print(f"   主键: interaction_id (自增)")
    print(f"   外键: user_id, song_id")
    print(f"   字段数: {results['交互矩阵']['column_count']}")
    print(f"   关键字段: {', '.join(results['交互矩阵']['columns'])}")

# 保存结果
with open('文件结构分析结果.txt', 'w', encoding='utf-8') as f:
    f.write("文件结构分析结果\n")
    f.write("="*60 + "\n\n")
    
    for name, data in results.items():
        f.write(f"{name} ({data['filename']}):\n")
        f.write(f"列数: {data['column_count']}\n")
        f.write("字段列表:\n")
        for i, col in enumerate(data['columns'], 1):
            f.write(f"  {i:2d}. {col}\n")
        f.write("\n" + "-"*40 + "\n\n")

print("\n✅ 分析完成! 结果已保存到: 文件结构分析结果.txt")
print("="*50)