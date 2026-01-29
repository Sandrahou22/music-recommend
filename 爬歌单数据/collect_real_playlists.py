# collect_real_playlists.py
import requests
from bs4 import BeautifulSoup
import time
import random

def collect_real_playlist_ids():
    """收集真实的网易云音乐歌单ID"""
    base_url = "https://music.163.com/discover/playlist/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://music.163.com/'
    }
    
    categories = ['华语', '流行', '摇滚', '民谣', '电子', '说唱', '轻音乐', 
                  '影视原声', 'ACG', '欧美', '日语', '韩语']
    
    playlist_ids = []
    
    for cat in categories:
        print(f"正在收集分类: {cat}")
        
        # 收集多页数据
        for page in range(0, 5):  # 每类收集5页
            params = {
                'cat': cat,
                'order': 'hot',  # 按热度排序
                'limit': 35,     # 每页35个
                'offset': page * 35
            }
            
            try:
                response = requests.get(base_url, params=params, headers=headers, timeout=10)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 查找歌单链接
                for link in soup.find_all('a', {'class': 'msk'}):
                    href = link.get('href', '')
                    if '/playlist?id=' in href:
                        playlist_id = href.split('=')[1]
                        if playlist_id not in playlist_ids:
                            playlist_ids.append(playlist_id)
                            print(f"  发现歌单ID: {playlist_id}")
                
                # 随机延迟，避免被封
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                print(f"收集分类 {cat} 第 {page+1} 页时出错: {e}")
                continue
    
    # 保存到文件
    with open('real_playlist_ids.txt', 'w', encoding='utf-8') as f:
        for pid in playlist_ids:
            f.write(f"'{pid}',\n")
    
    print(f"\n✅ 共收集到 {len(playlist_ids)} 个真实歌单ID")
    print("💾 已保存到 real_playlist_ids.txt")
    
    return playlist_ids

if __name__ == "__main__":
    collect_real_playlist_ids()