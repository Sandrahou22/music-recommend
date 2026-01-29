"""
网易云音乐用户行为数据爬虫 - 支持分批爬取全部用户
获取用户播放历史、喜欢的歌曲和收藏数据
"""

import requests
import json
import time
import csv
import os
import sys
from datetime import datetime
import random
import pandas as pd
import threading
from queue import Queue

class NetEaseBehaviorCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://music.163.com/',
        })
        
        # 使用cookie
        self.cookies = {
            'NMTID': '00O7NExLq8UckSWGknpgjQffduQxR0AAAGb1D9htQ',
            'WM_TID': '24RrnhZfrCtEQEVAQVLQGjL7VSAb0A46',
        }
        
        for key, value in self.cookies.items():
            self.session.cookies.set(key, value)
        
        # 请求统计
        self.request_count = 0
        self.success_count = 0
        self.fail_count = 0
        
    def safe_request(self, url, params=None, method='GET', max_retries=3):
        """安全的请求函数，带有重试机制"""
        for attempt in range(max_retries):
            try:
                self.request_count += 1
                
                if method == 'GET':
                    response = self.session.get(url, params=params, timeout=15)
                else:
                    response = self.session.post(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data.get('code') == 200 or data.get('code') == 0:
                            self.success_count += 1
                            return data
                        else:
                            print(f"API返回错误码: {data.get('code')}, 消息: {data.get('msg', '未知错误')}")
                    except:
                        self.success_count += 1
                        return response.text
                elif response.status_code == 404:
                    print(f"请求的资源不存在: {url}")
                    return None
                elif response.status_code == 403:
                    print(f"访问被拒绝: {url}")
                    time.sleep(10)  # 等待更长时间
                    continue
                else:
                    print(f"HTTP错误码: {response.status_code}")
            
            except requests.exceptions.Timeout:
                print(f"请求超时，尝试 {attempt+1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
            except requests.exceptions.ConnectionError:
                print(f"连接错误，尝试 {attempt+1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(10 * (attempt + 1))
            except Exception as e:
                print(f"请求异常: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
        
        self.fail_count += 1
        return None
    
    def get_user_play_history(self, user_id, limit=50):
        """获取用户播放历史"""
        url = f"https://music.163.com/api/v1/play/record"
        params = {
            'uid': user_id,
            'type': 1,  # 1=最近一周，0=所有时间
            'limit': limit,
            'offset': 0
        }
        
        data = self.safe_request(url, params)
        if data:
            return data.get('weekData', [])  # 最近一周播放记录
        return []
    
    def get_user_like_songs(self, user_id, limit=100):
        """获取用户喜欢的歌曲"""
        # 先获取用户歌单列表，找到"我喜欢的音乐"歌单
        playlist_url = f"https://music.163.com/api/user/playlist"
        params = {
            'uid': user_id,
            'limit': 30,
            'offset': 0
        }
        
        data = self.safe_request(playlist_url, params)
        if not data:
            return []
        
        playlists = data.get('playlist', [])
        
        # 查找"我喜欢的音乐"歌单
        for playlist in playlists:
            if playlist.get('specialType') == 5:  # 5表示"我喜欢的音乐"
                playlist_id = playlist.get('id')
                
                # 获取歌单详情
                detail_url = f"https://music.163.com/api/v6/playlist/detail"
                detail_params = {
                    'id': playlist_id,
                    'limit': limit,
                    'offset': 0,
                    'n': 1000
                }
                
                detail_data = self.safe_request(detail_url, detail_params)
                if detail_data:
                    playlist_detail = detail_data.get('playlist', {})
                    track_ids = playlist_detail.get('trackIds', [])
                    
                    # 只返回歌曲ID列表
                    return [track['id'] for track in track_ids[:limit]]
        
        return []
    
    def get_song_details_batch(self, song_ids, batch_size=50):
        """批量获取歌曲详情"""
        if not song_ids:
            return []
        
        all_songs = []
        
        # 分批处理
        for i in range(0, len(song_ids), batch_size):
            batch = song_ids[i:i+batch_size]
            
            url = "https://music.163.com/api/v3/song/detail"
            params = {
                'c': json.dumps([{'id': sid} for sid in batch]),
                'ids': f'[{",".join([str(sid) for sid in batch])}]'
            }
            
            data = self.safe_request(url, params)
            if data:
                songs = data.get('songs', [])
                all_songs.extend(songs)
            
            # 延迟避免被封
            time.sleep(0.3 + random.random())
        
        return all_songs
    
    def crawl_single_user(self, user_id):
        """爬取单个用户的行为数据"""
        user_data = {
            'user_id': user_id,
            'play_history': [],
            'like_songs': []
        }
        
        # 获取播放历史
        play_records = self.get_user_play_history(user_id, limit=30)
        user_data['play_history'] = play_records
        
        # 获取喜欢的歌曲
        like_song_ids = self.get_user_like_songs(user_id, limit=50)
        if like_song_ids:
            # 获取歌曲详情
            songs_details = self.get_song_details_batch(like_song_ids[:30], batch_size=20)
            user_data['like_songs'] = songs_details
        
        return user_data
    
    def process_user_data(self, user_data):
        """处理用户数据，提取有用信息"""
        if not user_data:
            return [], []
        
        user_id = user_data['user_id']
        play_history_data = []
        like_songs_data = []
        
        # 处理播放历史
        for record in user_data['play_history']:
            if record and record.get('song'):
                song = record.get('song', {})
                play_history_data.append({
                    'user_id': user_id,
                    'song_id': song.get('id'),
                    'play_count': record.get('playCount', 1),
                    'score': record.get('score', 0),
                    'song_name': song.get('name', ''),
                    'artists': ','.join([a.get('name', '') for a in song.get('ar', [])]),
                    'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        # 处理喜欢的歌曲
        for song in user_data['like_songs']:
            if song:
                like_songs_data.append({
                    'user_id': user_id,
                    'song_id': song.get('id'),
                    'like_time': datetime.fromtimestamp(song.get('publishTime', 0)/1000).strftime('%Y-%m-%d') if song.get('publishTime') else '',
                    'song_name': song.get('name', ''),
                    'artists': ','.join([a.get('name', '') for a in song.get('ar', [])]),
                    'album': song.get('al', {}).get('name', ''),
                    'duration': song.get('dt', 0),
                    'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        return play_history_data, like_songs_data
    
    def crawl_users_batch(self, user_ids, batch_size=10, output_dir="behavior_data", resume=False):
        """分批爬取用户行为数据"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 如果启用断点续爬，读取已处理的用户
        processed_users = set()
        if resume:
            processed_file = os.path.join(output_dir, "processed_users.txt")
            if os.path.exists(processed_file):
                with open(processed_file, 'r', encoding='utf-8') as f:
                    processed_users = set([line.strip() for line in f])
                print(f"找到 {len(processed_users)} 个已处理的用户")
        
        # 过滤掉已处理的用户
        remaining_users = [uid for uid in user_ids if uid not in processed_users]
        print(f"需要爬取 {len(remaining_users)} 个用户")
        
        # 分批处理
        total_batches = (len(remaining_users) + batch_size - 1) // batch_size
        
        # 创建输出文件
        play_history_file = os.path.join(output_dir, f"user_play_history_{timestamp}.csv")
        like_songs_file = os.path.join(output_dir, f"user_like_songs_{timestamp}.csv")
        
        play_history_fields = [
            'user_id', 'song_id', 'play_count', 'score', 
            'song_name', 'artists', 'crawl_time'
        ]
        
        like_song_fields = [
            'user_id', 'song_id', 'like_time', 'song_name', 
            'artists', 'album', 'duration', 'crawl_time'
        ]
        
        # 初始化CSV文件（如果不是断点续爬）
        if not resume or not os.path.exists(play_history_file):
            with open(play_history_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=play_history_fields)
                writer.writeheader()
        
        if not resume or not os.path.exists(like_songs_file):
            with open(like_songs_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=like_song_fields)
                writer.writeheader()
        
        # 分批爬取
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(remaining_users))
            batch_users = remaining_users[start_idx:end_idx]
            
            print(f"\n{'='*60}")
            print(f"批次 {batch_num+1}/{total_batches} (用户 {start_idx+1}-{end_idx})")
            print(f"{'='*60}")
            
            batch_play_data = []
            batch_like_data = []
            
            for i, user_id in enumerate(batch_users, 1):
                print(f"[{i}/{len(batch_users)}] 处理用户 {user_id}")
                
                try:
                    # 爬取用户数据
                    user_data = self.crawl_single_user(user_id)
                    
                    # 处理数据
                    play_data, like_data = self.process_user_data(user_data)
                    
                    if play_data:
                        batch_play_data.extend(play_data)
                        print(f"  ✓ 播放记录: {len(play_data)} 条")
                    
                    if like_data:
                        batch_like_data.extend(like_data)
                        print(f"  ✓ 喜欢歌曲: {len(like_data)} 首")
                    
                    # 记录已处理的用户
                    processed_users.add(user_id)
                    
                    # 保存已处理的用户列表
                    with open(os.path.join(output_dir, "processed_users.txt"), 'a', encoding='utf-8') as f:
                        f.write(f"{user_id}\n")
                    
                except Exception as e:
                    print(f"  ✗ 处理用户 {user_id} 时出错: {e}")
                    continue
                
                # 用户间延迟
                if i < len(batch_users):
                    delay = 2 + random.random() * 3  # 2-5秒延迟
                    time.sleep(delay)
            
            # 保存本批次数据
            if batch_play_data:
                with open(play_history_file, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=play_history_fields)
                    writer.writerows(batch_play_data)
            
            if batch_like_data:
                with open(like_songs_file, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=like_song_fields)
                    writer.writerows(batch_like_data)
            
            print(f"\n批次 {batch_num+1} 完成!")
            print(f"  播放记录: {len(batch_play_data)} 条")
            print(f"  喜欢歌曲: {len(batch_like_data)} 条")
            print(f"  累计请求: {self.request_count}, 成功: {self.success_count}, 失败: {self.fail_count}")
            
            # 批次间延迟
            if batch_num < total_batches - 1:
                batch_delay = 10 + random.random() * 10  # 10-20秒延迟
                print(f"\n等待 {batch_delay:.1f} 秒后继续下一批次...")
                time.sleep(batch_delay)
        
        print(f"\n{'='*60}")
        print("爬取完成!")
        print(f"{'='*60}")
        print(f"总用户数: {len(remaining_users)}")
        print(f"成功处理: {len(processed_users)}")
        print(f"请求统计: 总请求 {self.request_count}, 成功 {self.success_count}, 失败 {self.fail_count}")
        print(f"\n数据文件:")
        print(f"  播放历史: {play_history_file}")
        print(f"  喜欢歌曲: {like_songs_file}")
        
        return play_history_file, like_songs_file

def main():
    print("=" * 60)
    print("网易云音乐用户行为数据爬虫 - 批量版")
    print("=" * 60)
    
    # 初始化爬虫
    crawler = NetEaseBehaviorCrawler()
    
    # 读取用户ID
    try:
        # 读取用户ID文件
        user_ids_df = pd.read_csv("collected_user_ids_20260119_173402.csv")
        user_ids = user_ids_df['user_id'].astype(str).unique().tolist()
        
        print(f"读取到 {len(user_ids)} 个用户ID")
        print(f"前5个用户ID: {user_ids[:5]}")
        
        # 配置参数
        print(f"\n配置爬取参数:")
        
        batch_size = input(f"请输入每批处理用户数 (默认10): ").strip()
        batch_size = int(batch_size) if batch_size.isdigit() else 10
        
        resume = input("是否启用断点续爬? (y/n, 默认n): ").strip().lower()
        resume = resume == 'y'
        
        # 确认开始
        print(f"\n开始爬取配置:")
        print(f"  总用户数: {len(user_ids)}")
        print(f"  批次大小: {batch_size}")
        print(f"  断点续爬: {'是' if resume else '否'}")
        
        confirm = input("\n确认开始爬取? (y/n): ").strip().lower()
        if confirm != 'y':
            print("取消爬取")
            return
        
        # 开始爬取
        start_time = time.time()
        
        play_history_file, like_songs_file = crawler.crawl_users_batch(
            user_ids, 
            batch_size=batch_size,
            resume=resume
        )
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n{'='*60}")
        print("爬取完成!")
        print(f"{'='*60}")
        print(f"总耗时: {total_time:.1f} 秒 ({total_time/3600:.2f} 小时)")
        print(f"平均每个用户: {total_time/len(user_ids):.1f} 秒")
        
        # 显示文件大小
        if os.path.exists(play_history_file):
            size = os.path.getsize(play_history_file) / 1024 / 1024
            print(f"播放历史文件大小: {size:.2f} MB")
        
        if os.path.exists(like_songs_file):
            size = os.path.getsize(like_songs_file) / 1024 / 1024
            print(f"喜欢歌曲文件大小: {size:.2f} MB")
        
    except FileNotFoundError:
        print("错误: 找不到 collected_user_ids_20260119_173402.csv 文件")
        print("请确保文件在当前目录")
    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n程序执行完成!")
    input("按Enter键退出...")

if __name__ == "__main__":
    main()