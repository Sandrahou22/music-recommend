import pandas as pd
import requests
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from bs4 import BeautifulSoup

# ========== 配置 ==========
USER_DATA_PATH = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\用户数据_20260124_200012.csv'   # 原始数据，含原始user_id和nickname
FEATURES_PATH = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\separated_processed_data\internal\user_features.csv'  # 处理后数据，含最终user_id和nickname
SAVE_DIR = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\avatars'   # 头像保存目录

# 请求头（确保只包含ASCII字符）
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://music.163.com/'
}
# 如果需要登录，请填写Cookie字典（键值都应是ASCII）
COOKIES = {
    # 'MUSIC_U': 'your_cookie_value',
    # '__csrf': 'your_csrf_value'
}

# 并发控制
MAX_WORKERS = 10          # 同时运行的线程数（可根据网络和服务器限制调整）
REQUEST_DELAY = (0.5, 1.5)  # 随机延时范围（秒），避免请求过快

# 重试次数
MAX_RETRIES = 3

# ========== 读取数据并构建映射 ==========
print("正在读取数据并构建映射...")
user_data = pd.read_csv(USER_DATA_PATH, dtype={'user_id': str})
user_features = pd.read_csv(FEATURES_PATH, dtype={'user_id': str})

# 确保nickname为字符串
user_data['nickname'] = user_data['nickname'].astype(str)
user_features['nickname'] = user_features['nickname'].astype(str)

# 通过nickname内连接，得到最终user_id与原始user_id的对应关系
merged = pd.merge(
    user_features[['nickname', 'user_id']].rename(columns={'user_id': 'final_id'}),
    user_data[['nickname', 'user_id']].rename(columns={'user_id': 'orig_id'}),
    on='nickname',
    how='inner'
).drop_duplicates(subset=['final_id', 'orig_id'])

print(f"共匹配到 {len(merged)} 个用户需要爬取头像")

# 转换为字典列表，方便多线程处理
user_list = merged[['final_id', 'orig_id']].to_dict('records')

# 创建保存目录
os.makedirs(SAVE_DIR, exist_ok=True)

# ========== 头像获取函数（API优先） ==========
def get_avatar_url(orig_id):
    """根据原始user_id获取头像URL（返回URL字符串，失败返回None）"""
    # 尝试API
    api_url = f'https://music.163.com/api/v1/user/detail/{orig_id}'
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(api_url, headers=HEADERS, cookies=COOKIES, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data['code'] == 200:
                    return data['profile']['avatarUrl']
            # 如果API失败，尝试解析HTML
            html_url = f'https://music.163.com/user/home?id={orig_id}'
            resp = requests.get(html_url, headers=HEADERS, cookies=COOKIES, timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                # 根据实际页面选择器调整
                img = soup.find('img', class_='d-avatar') or soup.find('img', attrs={'data-original': True})
                if img:
                    return img.get('src') or img.get('data-original')
        except Exception as e:
            print(f"尝试获取 orig_id={orig_id} 失败 (尝试 {attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(1)  # 重试前等待
    return None

# ========== 下载头像并保存 ==========
def download_avatar(final_id, avatar_url):
    """下载头像并保存为 final_id.jpg"""
    try:
        resp = requests.get(avatar_url, headers=HEADERS, cookies=COOKIES, stream=True, timeout=15)
        if resp.status_code == 200:
            # 确定扩展名（从URL中获取，默认为jpg）
            ext = avatar_url.split('.')[-1].split('?')[0].lower()
            if ext not in ['jpg', 'jpeg', 'png', 'gif']:
                ext = 'jpg'
            file_path = os.path.join(SAVE_DIR, f"{final_id}.{ext}")
            with open(file_path, 'wb') as f:
                for chunk in resp.iter_content(1024):
                    f.write(chunk)
            return True
    except Exception as e:
        print(f"下载头像失败 final_id={final_id}: {e}")
    return False

# ========== 单个用户处理任务 ==========
def process_user(user):
    final_id = user['final_id']
    orig_id = user['orig_id']
    
    # 检查是否已存在（避免重复下载）
    for ext in ['.jpg', '.jpeg', '.png', '.gif']:
        if os.path.exists(os.path.join(SAVE_DIR, f"{final_id}{ext}")):
            print(f"已存在：final_id={final_id}，跳过")
            return final_id, "exists"
    
    # 获取头像URL
    avatar_url = get_avatar_url(orig_id)
    if not avatar_url:
        print(f"无法获取头像URL final_id={final_id}, orig_id={orig_id}")
        return final_id, "failed"
    
    # 下载
    success = download_avatar(final_id, avatar_url)
    if success:
        print(f"成功下载 final_id={final_id}")
        return final_id, "success"
    else:
        print(f"下载失败 final_id={final_id}")
        return final_id, "failed"

# ========== 多线程主程序 ==========
def main():
    print(f"开始爬取，共 {len(user_list)} 个用户，并发线程数 {MAX_WORKERS}")
    results = {"success": 0, "failed": 0, "exists": 0}
    
    # 使用线程池
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_user = {executor.submit(process_user, user): user for user in user_list}
        
        # 处理完成的任务
        for future in as_completed(future_to_user):
            final_id, status = future.result()
            results[status] += 1
            
            # 随机延时（即使多线程，也可以在任务内部控制，但这里整体也可添加，但没必要）
            # 实际上每个任务内部已有重试和可能的延时，但为了整体平缓，可以在任务之间加延时
            # 但多线程时，任务间延时意义不大，因为线程是并行的。
            # 我们可以在process_user内每次请求后加随机延时（已包含在get_avatar_url的循环中？未包含，可以添加）
            # 为了安全，在process_user末尾加一个短暂随机休眠
            time.sleep(random.uniform(0.2, 0.5))
    
    print(f"\n爬取完成！成功：{results['success']}，失败：{results['failed']}，已存在：{results['exists']}")

if __name__ == '__main__':
    main()