"""
歌曲标签和音频特征爬虫
"""

import requests
import json
import time
import csv
import pandas as pd
from datetime import datetime
import random
import os  # 添加缺失的 os 模块导入

class SongFeatureCrawler:
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
    
    def get_song_tags(self, song_id):
        """获取歌曲标签"""
        url = f"https://music.163.com/api/v1/song/detail/"
        params = {
            'id': song_id,
            'ids': f'[{song_id}]'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 200:
                    song = data.get('songs', [{}])[0]
                    return {
                        'song_id': song_id,
                        'song_name': song.get('name', ''),
                        'artists': ','.join([a.get('name', '') for a in song.get('ar', [])]),
                        'album': song.get('al', {}).get('name', ''),
                        'popularity': song.get('pop', 0),  # 流行度
                        'score': song.get('score', 0),    # 评分
                    }
        except:
            pass
        return None
    
    def get_similar_songs(self, song_id, limit=20):
        """获取相似歌曲"""
        url = f"https://music.163.com/api/v1/discovery/simiSong"
        params = {
            'songid': song_id,
            'limit': limit,
            'offset': 0
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 200:
                    songs = data.get('songs', [])
                    similar_songs = []
                    for song in songs:
                        similar_songs.append({
                            'song_id': song.get('id'),
                            'song_name': song.get('name', ''),
                            'similarity_score': song.get('score', 0),
                            'artists': ','.join([a.get('name', '') for a in song.get('artists', [])])
                        })
                    return similar_songs
        except:
            pass
        return []
    
    def get_song_comments(self, song_id, limit=30):
        """获取歌曲评论（用于情感分析）"""
        url = f"https://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}"
        params = {
            'limit': limit,
            'offset': 0,
            'rid': f'R_SO_4_{song_id}'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 200:
                    comments = data.get('comments', [])
                    comment_data = []
                    for comment in comments:
                        comment_data.append({
                            'song_id': song_id,
                            'comment_id': comment.get('commentId'),
                            'user_id': comment.get('user', {}).get('userId'),
                            'content': comment.get('content', ''),
                            'liked_count': comment.get('likedCount', 0),
                            'time': datetime.fromtimestamp(comment.get('time', 0)/1000).strftime('%Y-%m-%d %H:%M:%S'),
                            'sentiment_score': self.analyze_sentiment(comment.get('content', ''))
                        })
                    return comment_data
        except:
            pass
        return []
    
    def analyze_sentiment(self, text):
        """简单的情感分析（实际应用中应该使用更复杂的模型）"""
        positive_words = ['喜欢', '爱', '好听', '棒', '赞', '经典', '美', '感动']
        negative_words = ['难听', '讨厌', '差', '垃圾', '无聊', '失望']
        
        if not text:
            return 0
        
        score = 0
        for word in positive_words:
            if word in text:
                score += 1
        for word in negative_words:
            if word in text:
                score -= 1
        
        return max(min(score, 3), -3)  # 限制在-3到3之间
    
    def crawl_song_features(self, song_ids, output_dir="song_features"):
        """爬取歌曲特征数据"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 创建输出文件
        tags_file = os.path.join(output_dir, f"song_tags_{timestamp}.csv")
        similar_file = os.path.join(output_dir, f"song_similarity_{timestamp}.csv")
        comments_file = os.path.join(output_dir, f"song_comments_{timestamp}.csv")
        
        all_tags = []
        all_similar = []
        all_comments = []
        
        print(f"开始爬取 {len(song_ids)} 首歌曲的特征数据...")
        
        for i, song_id in enumerate(song_ids, 1):
            print(f"[{i}/{len(song_ids)}] 处理歌曲 {song_id}")
            
            # 获取标签
            tags = self.get_song_tags(song_id)
            if tags:
                all_tags.append(tags)
                print(f"  ✓ 获取标签成功")
            
            # 获取相似歌曲
            similar_songs = self.get_similar_songs(song_id, limit=10)
            if similar_songs:
                for sim_song in similar_songs:
                    sim_song['source_song_id'] = song_id
                    all_similar.append(sim_song)
                print(f"  ✓ 获取到 {len(similar_songs)} 首相似歌曲")
            
            # 获取评论（每隔5首歌获取一次，避免请求过多）
            if i % 5 == 0:
                comments = self.get_song_comments(song_id, limit=20)
                if comments:
                    all_comments.extend(comments)
                    print(f"  ✓ 获取到 {len(comments)} 条评论")
            
            # 延迟
            if i < len(song_ids):
                time.sleep(0.5 + random.random())
        
        # 保存数据
        if all_tags:
            df_tags = pd.DataFrame(all_tags)
            df_tags.to_csv(tags_file, index=False, encoding='utf-8-sig')
            print(f"\n歌曲标签数据已保存: {tags_file}")
        
        if all_similar:
            df_similar = pd.DataFrame(all_similar)
            df_similar.to_csv(similar_file, index=False, encoding='utf-8-sig')
            print(f"歌曲相似度数据已保存: {similar_file}")
        
        if all_comments:
            df_comments = pd.DataFrame(all_comments)
            df_comments.to_csv(comments_file, index=False, encoding='utf-8-sig')
            print(f"歌曲评论数据已保存: {comments_file}")
        
        return all_tags, all_similar, all_comments

def main():
    print("=" * 60)
    print("歌曲特征数据爬虫")
    print("=" * 60)
    
    # 读取现有歌曲数据
    try:
        songs_df = pd.read_csv("all_songs.csv")
        song_ids = songs_df['song_id'].astype(str).unique().tolist()
        
        print(f"读取到 {len(song_ids)} 首歌曲")
        
        # 选择爬取数量
        print(f"\n请选择:")
        print(f"1. 爬取全部 {len(song_ids)} 首歌曲的特征")
        print(f"2. 爬取前100首歌曲的特征")
        print(f"3. 自定义数量")
        
        choice = input("请输入选择 (1-3): ").strip()
        
        if choice == "1":
            selected_ids = song_ids
        elif choice == "2":
            selected_ids = song_ids[:100]
        elif choice == "3":
            try:
                n = int(input(f"请输入要爬取的数量 (1-{len(song_ids)}): "))
                selected_ids = song_ids[:n]
            except:
                selected_ids = song_ids[:50]
        else:
            selected_ids = song_ids[:50]
        
        # 初始化爬虫
        crawler = SongFeatureCrawler()
        
        # 开始爬取
        crawler.crawl_song_features(selected_ids)
        
    except FileNotFoundError:
        print("找不到 all_songs.csv 文件")
        print("请确保 all_songs.csv 文件在当前目录")
    
    print("\n程序执行完成!")

if __name__ == "__main__":
    main()