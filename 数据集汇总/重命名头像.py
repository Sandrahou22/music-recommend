import os
import pandas as pd

# ========== 配置路径 ==========
USER_DATA_PATH = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\用户数据_20260124_200012.csv'   # 原始数据，包含原始user_id
FEATURES_PATH = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\separated_processed_data\internal\user_features.csv'  # 处理后数据，包含最终user_id
AVATAR_DIR = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\avatars'   # 头像存放文件夹

# ========== 读取数据，确保ID为字符串 ==========
print("正在读取用户数据...")
user_data = pd.read_csv(USER_DATA_PATH, dtype={'user_id': str})      # 原始ID
user_features = pd.read_csv(FEATURES_PATH, dtype={'user_id': str})   # 最终ID（假设列名为'user_id'）

# 重命名列以便区分
user_data = user_data.rename(columns={'user_id': 'user_id_original'})
user_features = user_features.rename(columns={'user_id': 'user_id_final'})

# 确保nickname为字符串（防止类型不一致）
user_data['nickname'] = user_data['nickname'].astype(str)
user_features['nickname'] = user_features['nickname'].astype(str)

# ========== 通过昵称连接两个表，得到原始ID与最终ID的对应关系 ==========
merged = pd.merge(
    user_features[['nickname', 'user_id_final']],
    user_data[['nickname', 'user_id_original']],
    on='nickname',
    how='inner'          # 只保留两边都有的昵称
).drop_duplicates(subset=['user_id_original', 'user_id_final'])   # 避免重复

print(f"共匹配到 {len(merged)} 条对应关系")

# ========== 模拟之前因int转换导致的错误ID ==========
def to_int32_overflow(val):
    """将字符串数字转换为可能溢出的32位有符号整数（模拟之前错误的int转换）"""
    try:
        num = int(val)
        # 模拟32位有符号溢出
        num = num & 0xFFFFFFFF          # 取低32位
        if num >= 2**31:                # 如果超过2^31-1，则为负数
            num -= 2**32
        return str(num)                  # 返回字符串形式，便于与文件名比较
    except:
        return None

# 构建错误ID → 最终ID的映射
error_to_final = {}
for _, row in merged.iterrows():
    orig_id = row['user_id_original']
    final_id = row['user_id_final']
    error_id = to_int32_overflow(orig_id)
    if error_id:
        if error_id in error_to_final:
            print(f"警告：错误ID {error_id} 同时对应最终ID {error_to_final[error_id]} 和 {final_id}，将覆盖为后者")
        error_to_final[error_id] = final_id

print(f"生成映射 {len(error_to_final)} 条")

# ========== 遍历头像文件夹，重命名文件 ==========
rename_count = 0
skip_count = 0
for filename in os.listdir(AVATAR_DIR):
    file_path = os.path.join(AVATAR_DIR, filename)
    if not os.path.isfile(file_path):
        continue

    # 分离文件名和扩展名
    name, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']:
        continue   # 非图片文件跳过

    # 如果文件名（错误ID）在映射中，则重命名为最终ID
    if name in error_to_final:
        final_id = error_to_final[name]
        new_filename = final_id + ext
        new_path = os.path.join(AVATAR_DIR, new_filename)

        # 避免覆盖现有文件
        if os.path.exists(new_path):
            print(f"跳过：目标文件已存在 {new_filename}")
            skip_count += 1
            continue

        os.rename(file_path, new_path)
        print(f"重命名：{filename} -> {new_filename}")
        rename_count += 1
    else:
        # 如果文件名不在映射中，可能是已经正确的ID或无关文件
        # 尝试检查它是否已经是最终ID
        if name in user_features['user_id_final'].values:
            print(f"跳过：{filename} 可能是正确的最终ID")
            skip_count += 1
        else:
            print(f"警告：未找到映射，跳过 {filename}")
            skip_count += 1

print(f"\n完成！共重命名 {rename_count} 个文件，跳过 {skip_count} 个文件。")