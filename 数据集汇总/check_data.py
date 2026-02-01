import pandas as pd

# 检查原始CSV
df = pd.read_csv(r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\aligned_data_final_optimized\enhanced_song_features.csv")

print("=== 歌曲特征数据检查 ===")
print(f"总行数: {len(df)}")

# 检查popularity字段
if 'popularity' in df.columns:
    print(f"\npopularity字段:")
    print(f"  - 非空数: {df['popularity'].notna().sum()}")
    print(f"  - 唯一值数: {df['popularity'].nunique()}")
    print(f"  - 范围: {df['popularity'].min()} ~ {df['popularity'].max()}")
    print(f"  - 前5个值: {df['popularity'].head().tolist()}")
else:
    print("\n❌ CSV里没有popularity字段！可用字段:", df.columns.tolist())

# 检查genre字段
if 'genre' in df.columns:
    print(f"\ngenre字段:")
    print(f"  - 非空数: {df['genre'].notna().sum()}")
    print(f"  - 示例值: {df['genre'].dropna().head().tolist()}")
else:
    print("\n❌ CSV里没有genre字段")

# 检查final_popularity（如果存在）
if 'final_popularity' in df.columns:
    print(f"\nfinal_popularity字段（对齐后）:")
    print(f"  - 范围: {df['final_popularity'].min()} ~ {df['final_popularity'].max()}")