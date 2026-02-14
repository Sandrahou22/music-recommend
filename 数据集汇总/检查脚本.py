# 运行这个检查脚本
import pandas as pd
import os

data_dir = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\aligned_data_optimized"
files = ['enhanced_song_features.csv', 'enhanced_user_features.csv', 
         'filtered_interactions.csv', 'train_interactions.csv', 'test_interactions.csv']

for file in files:
    file_path = os.path.join(data_dir, file)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, nrows=5)
        print(f"\n=== {file} ===")
        print(f"形状: {df.shape}")
        print(f"列名: {list(df.columns)}")