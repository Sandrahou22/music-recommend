# collect_user_ids.py - 方案A：从歌曲评论中提取用户ID
import pandas as pd
import requests
import time
import random
import sys
import os
from datetime import datetime

# ============== 配置区域（已根据你的路径修改） ==============
# ⚠️ 注意：这里使用你提供的绝对路径
# Windows路径中的反斜杠\在Python字符串中需要用\\或者使用原始字符串r""
DATA_DIR = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\爬歌单数据\daily_data"
OUTPUT_DIR = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\爬歌单数据\data"

# 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://music.163.com/',
    'Accept': 'application/json, text/plain, */*'
}

# 性能配置
REQUEST_DELAY_MIN = 1.5      # 最小延迟（秒）
REQUEST_DELAY_MAX = 3.0      # 最大延迟（秒）
MAX_RETRIES = 3               # 最大重试次数
MAX_USERS_PER_SONG = 30       # 每首歌最多提取30个用户
MAX_SONGS_TO_PROCESS = 500    # 最多处理500首歌（可调整）

# ============== 核心功能代码 ==============

def safe_request(url, retries=MAX_RETRIES):
    """带重试的安全请求"""
    for attempt in range(retries):
        try:
            # 随机延迟
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            time.sleep(delay)
            
            response = requests.get(url, headers=HEADERS, timeout=15)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 403:
                print(f"  ⚠️  请求被拒绝(403)，等待重试... ({attempt+1}/{retries})")
                time.sleep(delay * 3)
            else:
                print(f"  ⚠️  请求失败，状态码: {response.status_code}")
                
        except Exception as e:
            print(f"  ⚠️  请求异常: {e} (尝试 {attempt+1}/{retries})")
            if attempt < retries - 1:
                time.sleep(delay)
    
    return None

def get_comments_from_song(song_id, max_users=30):
    """
    获取一首歌曲的评论，提取用户ID
    """
    url = f"https://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}?limit={max_users}"
    response = safe_request(url)
    
    if not response:
        return []  # 请求失败返回空列表
    
    try:
        data = response.json()
        if data.get('code') != 200:
            print(f"  ❌ API返回错误码: {data.get('code')}")
            return []
        
        # 合并普通评论和热评
        all_comments = data.get('comments', []) + data.get('hotComments', [])
        
        user_list = []
        for comment in all_comments:
            user_info = comment.get('user', {})
            user_id = user_info.get('userId')
            nickname = user_info.get('nickname', '未知')
            
            if user_id:
                user_list.append({
                    'user_id': user_id,
                    'nickname': nickname,
                    'song_id': song_id
                })
        
        return user_list
        
    except Exception as e:
        print(f"  ❌ 解析JSON失败: {e}")
        return []

def load_song_ids_from_csv(csv_path, max_songs=500):
    """
    从all_songs.csv中加载歌曲ID
    """
    try:
        print(f"📂 正在读取歌曲数据: {csv_path}")
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        
        # 检查是否有song_id列
        if 'song_id' not in df.columns:
            print("❌ CSV文件中没有 'song_id' 列！")
            return []
        
        # 去重并限制数量
        song_ids = df['song_id'].drop_duplicates().tolist()
        
        # 限制最大数量
        if len(song_ids) > max_songs:
            print(f"⚠️  歌曲数量过多 ({len(song_ids)} 首)，只处理前 {max_songs} 首")
            song_ids = song_ids[:max_songs]
        
        print(f"✅ 成功加载 {len(song_ids)} 首歌曲")
        return song_ids
        
    except FileNotFoundError:
        print(f"❌ 找不到文件: {csv_path}")
        print(f"   请检查路径是否正确: {os.path.abspath(csv_path)}")
        return []
    except Exception as e:
        print(f"❌ 读取CSV失败: {e}")
        return []

def collect_user_ids_from_songs(song_ids):
    """
    主函数：从歌曲评论中收集用户ID
    """
    print("\n" + "="*70)
    print("🚀 开始收集用户ID")
    print("="*70)
    print(f"📊 目标歌曲数: {len(song_ids)} 首")
    print(f"👥 每首歌最多提取: {MAX_USERS_PER_SONG} 个用户")
    print("="*70)
    
    all_users = []  # 存储所有用户
    collected_user_ids = set()  # 去重集合
    
    # 进度统计
    success_count = 0
    fail_count = 0
    
    for i, song_id in enumerate(song_ids, 1):
        print(f"\n[{i:>4}/{len(song_ids)}] 正在处理歌曲: {song_id}")
        
        # 检查是否已处理过（去重）
        users = get_comments_from_song(song_id, max_users=MAX_USERS_PER_SONG)
        
        if not users:
            fail_count += 1
            print(f"  ⚠️  未获取到用户")
            continue
        
        # 添加到总列表
        new_users = 0
        for user in users:
            if user['user_id'] not in collected_user_ids:
                all_users.append(user)
                collected_user_ids.add(user['user_id'])
                new_users += 1
        
        success_count += 1
        print(f"  ✅ 成功: {len(users)} 个用户，新增: {new_users} 个")
        
        # 每处理10首显示一次统计
        if i % 10 == 0:
            print("\n" + "-"*50)
            print(f"📈 进度统计:")
            print(f"  已处理: {i} 首歌曲")
            print(f"  成功: {success_count} 首")
            print(f"  失败: {fail_count} 首")
            print(f"  累计用户: {len(collected_user_ids)} 个")
            print("-"*50)
    
    print("\n" + "="*70)
    print("🏁 收集完成！")
    print("="*70)
    print(f"📊 最终统计:")
    print(f"  成功歌曲: {success_count} 首")
    print(f"  失败歌曲: {fail_count} 首")
    print(f"  去重后用户总数: {len(collected_user_ids)} 个")
    
    return all_users

def save_user_ids_to_csv(users, output_dir="data"):
    """
    保存用户ID到CSV文件
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_dir}/collected_user_ids_{timestamp}.csv"
    
    # 转换为DataFrame
    df = pd.DataFrame(users)
    
    # 去重（保留第一次出现）
    df_unique = df.drop_duplicates(subset=['user_id'], keep='first')
    
    # 保存
    df_unique.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"💾 数据已保存至: {output_file}")
    print(f"📦 文件大小: {os.path.getsize(output_file) / 1024:.2f} KB")
    
    return output_file

def show_sample_users(csv_file, n=10):
    """
    显示前N个用户样本
    """
    try:
        df = pd.read_csv(csv_file)
        print("\n" + "="*70)
        print(f"👥 用户样本（前{n}个）")
        print("="*70)
        for i, row in df.head(n).iterrows():
            print(f"  {i+1:>3}. 用户ID: {row['user_id']:>10} | 昵称: {row['nickname'][:15]:<15} | 来源歌曲: {row['song_id']}")
        print("="*70)
    except Exception as e:
        print(f"显示样本失败: {e}")

def main():
    """
    主函数 - 一键运行
    """
    print("🎵 网易云音乐用户ID收集工具 - 方案A")
    print("="*70)
    
    # 1. 加载歌曲数据
    csv_path = f"{DATA_DIR}\\all_songs.csv"  # Windows路径用\\或/
    song_ids = load_song_ids_from_csv(csv_path, max_songs=MAX_SONGS_TO_PROCESS)
    
    if not song_ids:
        print("❌ 没有歌曲数据可处理，程序退出")
        sys.exit(1)
    
    # 2. 开始收集用户ID
    users = collect_user_ids_from_songs(song_ids)
    
    if not users:
        print("❌ 未收集到任何用户ID，程序退出")
        sys.exit(1)
    
    # 3. 保存结果
    output_file = save_user_ids_to_csv(users, output_dir=OUTPUT_DIR)
    
    # 4. 显示样本
    show_sample_users(output_file, n=20)
    
    print("\n✅ 所有步骤完成！")
    print(f"🎯 你现在可以用这个文件里的用户ID去爬取用户画像和行为数据了")
    print(f"📂 文件路径: {os.path.abspath(output_file)}")

# ============== 运行入口 ==============
if __name__ == "__main__":
    main()