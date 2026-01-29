"""
歌单详细数据爬虫
获取歌单的详细信息和歌单中的歌曲
"""

import requests
import json
import time
import csv
import pandas as pd
from datetime import datetime
import random
import os

class PlaylistDetailCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://music.163.com/',
        })
        
        self.cookies = {
            'NMTID': '00O7NExLq8UckSWGknpgjQffduQxR0AAAGb1D9htQ',
        }
        
        for key, value in self.cookies.items():
            self.session.cookies.set(key, value)
    
    def get_playlist_detail(self, playlist_id):
        """获取歌单详情"""
        url = f"https://music.163.com/api/v6/playlist/detail"
        params = {
            'id': playlist_id,
            'n': 1000,  # 获取歌单所有歌曲
            'limit': 1000
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 200:
                    return data.get('playlist', {})
        except Exception as e:
            print(f"获取歌单详情失败: {e}")
        return {}
    
    def extract_playlist_info(self, playlist_data):
        """提取歌单信息"""
        if not playlist_data:
            return None
        
        playlist_id = playlist_data.get('id')
        
        # 获取创建者信息，处理空值情况
        creator = playlist_data.get('creator', {})
        if creator is None:
            creator = {}
        
        # 获取标签
        tags = playlist_data.get('tags', [])
        if tags is None:
            tags = []
        tags_str = ','.join(tags)
        
        # 计算歌单的平均播放量、收藏量等
        tracks = playlist_data.get('tracks', [])
        
        # 提取歌曲特征（简化版）
        song_features = []
        for track in tracks[:50]:  # 只分析前50首
            if track:  # 确保track不是None
                song_features.append({
                    'song_id': track.get('id', ''),
                    'duration': track.get('dt', 0),
                    'pop': track.get('pop', 0),  # 流行度
                })
        
        # 计算平均特征
        if song_features:
            avg_duration = sum([s['duration'] for s in song_features]) / len(song_features)
            avg_pop = sum([s['pop'] for s in song_features]) / len(song_features)
        else:
            avg_duration = 0
            avg_pop = 0
        
        # 处理时间戳
        create_time = playlist_data.get('createTime', 0)
        update_time = playlist_data.get('updateTime', 0)
        
        create_time_str = ''
        if create_time:
            try:
                create_time_str = datetime.fromtimestamp(create_time/1000).strftime('%Y-%m-%d')
            except:
                create_time_str = ''
        
        update_time_str = ''
        if update_time:
            try:
                update_time_str = datetime.fromtimestamp(update_time/1000).strftime('%Y-%m-%d')
            except:
                update_time_str = ''
        
        return {
            'playlist_id': playlist_id,
            'playlist_name': playlist_data.get('name', ''),
            'creator_id': creator.get('userId', ''),
            'creator_name': creator.get('nickname', ''),
            'create_time': create_time_str,
            'update_time': update_time_str,
            'track_count': playlist_data.get('trackCount', 0),
            'play_count': playlist_data.get('playCount', 0),
            'subscribed_count': playlist_data.get('subscribedCount', 0),
            'share_count': playlist_data.get('shareCount', 0),
            'comment_count': playlist_data.get('commentCount', 0),
            'tags': tags_str,
            'description': playlist_data.get('description', ''),
            'avg_duration': avg_duration,
            'avg_popularity': avg_pop,
            'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def get_playlist_songs(self, playlist_data):
        """提取歌单中的歌曲"""
        if not playlist_data:
            return []
        
        playlist_id = playlist_data.get('id')
        tracks = playlist_data.get('tracks', [])
        
        songs = []
        for i, track in enumerate(tracks):
            if track:  # 确保track不是None
                # 处理艺术家信息
                artists = []
                if track.get('ar'):
                    for artist in track.get('ar'):
                        if artist and artist.get('name'):
                            artists.append(artist.get('name'))
                
                # 处理专辑信息
                album_info = track.get('al', {})
                album_name = ''
                if album_info:
                    album_name = album_info.get('name', '')
                
                songs.append({
                    'playlist_id': playlist_id,
                    'song_id': track.get('id', ''),
                    'song_name': track.get('name', ''),
                    'artists': ','.join(artists),
                    'album': album_name,
                    'duration': track.get('dt', 0),
                    'popularity': track.get('pop', 0),
                    'order': i + 1  # 在歌单中的顺序
                })
        
        return songs
    
    def crawl_playlist_details(self, playlist_ids, output_dir="playlist_details"):
        """爬取歌单详细数据"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 创建输出文件
        playlist_info_file = os.path.join(output_dir, f"playlist_info_{timestamp}.csv")
        playlist_songs_file = os.path.join(output_dir, f"playlist_songs_{timestamp}.csv")
        
        all_playlist_info = []
        all_playlist_songs = []
        
        print(f"开始爬取 {len(playlist_ids)} 个歌单的详细数据...")
        
        for i, playlist_id in enumerate(playlist_ids, 1):
            print(f"[{i}/{len(playlist_ids)}] 处理歌单 {playlist_id}")
            
            try:
                # 获取歌单详情
                playlist_data = self.get_playlist_detail(playlist_id)
                
                if playlist_data and playlist_data.get('id'):
                    # 提取歌单信息
                    playlist_info = self.extract_playlist_info(playlist_data)
                    if playlist_info:
                        all_playlist_info.append(playlist_info)
                        print(f"  ✓ 歌单: {playlist_info['playlist_name']}")
                        print(f"      歌曲数: {playlist_info['track_count']}, 播放量: {playlist_info['play_count']}")
                    
                    # 提取歌单歌曲
                    songs = self.get_playlist_songs(playlist_data)
                    if songs:
                        all_playlist_songs.extend(songs)
                        print(f"      获取到 {len(songs)} 首歌曲")
                else:
                    print(f"  ✗ 歌单获取失败或不存在")
            except Exception as e:
                print(f"  ✗ 处理歌单时出错: {e}")
                continue
            
            # 延迟
            if i < len(playlist_ids):
                time.sleep(1 + random.random())
        
        # 保存数据
        if all_playlist_info:
            df_info = pd.DataFrame(all_playlist_info)
            df_info.to_csv(playlist_info_file, index=False, encoding='utf-8-sig')
            print(f"\n歌单信息已保存: {playlist_info_file}")
            print(f"歌单数量: {len(df_info)}")
        
        if all_playlist_songs:
            df_songs = pd.DataFrame(all_playlist_songs)
            df_songs.to_csv(playlist_songs_file, index=False, encoding='utf-8-sig')
            print(f"歌单歌曲关系已保存: {playlist_songs_file}")
            print(f"歌曲记录数: {len(df_songs)}")
        
        return all_playlist_info, all_playlist_songs

def main():
    print("=" * 60)
    print("歌单详细数据爬虫")
    print("=" * 60)
    
    # 读取现有数据
    try:
        # 读取歌单ID（从歌曲数据中提取）
        songs_df = pd.read_csv("all_songs.csv")
        playlist_ids = songs_df['playlist_id'].astype(str).unique().tolist()
        
        print(f"读取到 {len(playlist_ids)} 个歌单ID")
        
        # 显示前几个歌单
        print(f"前5个歌单ID: {playlist_ids[:5]}")
        
        # 选择爬取数量
        print(f"\n请选择:")
        print(f"1. 爬取全部 {len(playlist_ids)} 个歌单")
        print(f"2. 爬取前50个歌单")
        print(f"3. 自定义数量")
        
        choice = input("请输入选择 (1-3): ").strip()
        
        if choice == "1":
            selected_ids = playlist_ids
        elif choice == "2":
            selected_ids = playlist_ids[:50]
        elif choice == "3":
            try:
                n = int(input(f"请输入要爬取的数量 (1-{len(playlist_ids)}): "))
                selected_ids = playlist_ids[:n]
            except:
                selected_ids = playlist_ids[:30]
        else:
            selected_ids = playlist_ids[:30]
        
        # 初始化爬虫
        crawler = PlaylistDetailCrawler()
        
        # 开始爬取
        crawler.crawl_playlist_details(selected_ids)
        
    except FileNotFoundError:
        print("找不到 all_songs.csv 文件")
        print("请确保 all_songs.csv 文件在当前目录")
    except Exception as e:
        print(f"程序执行出错: {e}")
    
    print("\n程序执行完成!")

if __name__ == "__main__":
    main()