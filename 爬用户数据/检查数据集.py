import os
import glob

# 检查当前目录下的所有CSV文件
print("当前目录下的所有CSV文件：")
csv_files = glob.glob("*.csv")
for file in csv_files:
    size = os.path.getsize(file) / 1024  # KB
    print(f"  - {file} ({size:.2f} KB)")

# 检查所有文件（包括子目录）
print("\n所有文件（包括子目录）：")
all_files = []
for root, dirs, files in os.walk("."):
    for file in files:
        if file.endswith(".csv"):
            full_path = os.path.join(root, file)
            size = os.path.getsize(full_path) / 1024
            all_files.append(full_path)
            print(f"  - {full_path} ({size:.2f} KB)")