import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
import gc

class CompleteMusicDataLoader:
    """完整的音乐数据加载器"""
    
    def __init__(self, data_path="."):
        self.data_path = data_path
        self.data = {}
        self.summary = {}
        
    def load_all_data_with_progress(self):
        """加载所有数据并显示进度"""
        print("="*80)
        print("开始加载所有数据文件...")
        print("="*80)
        
        # 1. 歌曲基本信息 (8,569条)
        print("1. 加载歌曲基本信息...")
        self.data['songs'] = pd.read_csv(os.path.join(self.data_path, 'all_songs.csv'))
        self.summary['songs'] = len(self.data['songs'])
        print(f"   ✓ 加载完成: {self.summary['songs']:,} 条记录")
        
        # 2. 用户基本信息 (17,604条)
        print("2. 加载用户基本信息...")
        self.data['users'] = pd.read_csv(os.path.join(self.data_path, '用户数据_20260124_200012.csv'))
        self.summary['users'] = len(self.data['users'])
        print(f"   ✓ 加载完成: {self.summary['users']:,} 条记录")
        
        # 3. 用户-歌曲关联 (17,726条)
        print("3. 加载用户-歌曲关联数据...")
        self.data['collected'] = pd.read_csv(os.path.join(self.data_path, 'collected_user_ids_20260119_173402.csv'))
        self.summary['collected'] = len(self.data['collected'])
        print(f"   ✓ 加载完成: {self.summary['collected']:,} 条记录")
        
        # 4. 用户喜欢歌曲数据 (42433KB)
        print("4. 加载用户喜欢歌曲数据...")
        self.data['likes'] = pd.read_csv(os.path.join(self.data_path, 'user_like_songs_20260120_132245.csv'))
        self.summary['likes'] = len(self.data['likes'])
        print(f"   ✓ 加载完成: {self.summary['likes']:,} 条记录")
        
        # 5. 用户播放历史 (56202KB)
        print("5. 加载用户播放历史数据...")
        self.data['plays'] = pd.read_csv(os.path.join(self.data_path, 'user_play_history_20260120_132245.csv'))
        self.summary['plays'] = len(self.data['plays'])
        print(f"   ✓ 加载完成: {self.summary['plays']:,} 条记录")
        
        # 6. 歌单信息
        print("6. 加载歌单信息...")
        self.data['playlist_info'] = pd.read_csv(os.path.join(self.data_path, 'playlist_info_20260124_144712.csv'))
        self.summary['playlist_info'] = len(self.data['playlist_info'])
        print(f"   ✓ 加载完成: {self.summary['playlist_info']:,} 条记录")
        
        # 7. 歌单歌曲
        print("7. 加载歌单歌曲数据...")
        self.data['playlist_songs'] = pd.read_csv(os.path.join(self.data_path, 'playlist_songs_20260124_144712.csv'))
        self.summary['playlist_songs'] = len(self.data['playlist_songs'])
        print(f"   ✓ 加载完成: {self.summary['playlist_songs']:,} 条记录")
        
        # 8. 歌曲评论
        print("8. 加载歌曲评论数据...")
        self.data['comments'] = pd.read_csv(os.path.join(self.data_path, 'song_comments_20260124_001212.csv'))
        self.summary['comments'] = len(self.data['comments'])
        print(f"   ✓ 加载完成: {self.summary['comments']:,} 条记录")
        
        # 9. 歌曲相似度
        print("9. 加载歌曲相似度数据...")
        self.data['similarity'] = pd.read_csv(os.path.join(self.data_path, 'song_similarity_20260124_001212.csv'))
        self.summary['similarity'] = len(self.data['similarity'])
        print(f"   ✓ 加载完成: {self.summary['similarity']:,} 条记录")
        
        # 10. 歌曲标签
        print("10. 加载歌曲标签数据...")
        self.data['tags'] = pd.read_csv(os.path.join(self.data_path, 'song_tags_20260124_001212.csv'))
        self.summary['tags'] = len(self.data['tags'])
        print(f"   ✓ 加载完成: {self.summary['tags']:,} 条记录")
        
        # 11. 外部用户收听历史 (588MB - 需要分块处理)
        print("11. 加载外部用户收听历史数据 (可能需要一些时间)...")
        # 先查看文件大小和前几行
        try:
            # 分块读取，只读取前10000行用于分析
            self.data['external_history'] = pd.read_csv(
                os.path.join(self.data_path, 'User Listening History.csv'),
                nrows=200000 
            )
            self.summary['external_history'] = len(self.data['external_history'])
            print(f"   ✓ 部分加载完成: {self.summary['external_history']:,} 条记录 (前10,000行)")
            
            # 获取总行数
            total_rows = sum(1 for _ in open(os.path.join(self.data_path, 'User Listening History.csv'))) - 1
            print(f"   ! 总行数约: {total_rows:,} 条记录")
        except Exception as e:
            print(f"   ✗ 加载失败: {str(e)}")
            self.data['external_history'] = pd.DataFrame()
        
        # 12. 外部音乐信息 (14.6MB)
        print("12. 加载外部音乐信息...")
        try:
            self.data['external_music'] = pd.read_csv(os.path.join(self.data_path, 'Music Info.csv'))
            self.summary['external_music'] = len(self.data['external_music'])
            print(f"   ✓ 加载完成: {self.summary['external_music']:,} 条记录")
        except Exception as e:
            print(f"   ✗ 加载失败: {str(e)}")
            self.data['external_music'] = pd.DataFrame()
        
        print("\n" + "="*80)
        print("所有数据加载完成!")
        print("="*80)
        
        # 显示汇总信息
        self.display_summary()
        
        return self.data
    
    def display_summary(self):
        """显示数据汇总信息"""
        print("\n📊 数据汇总报告")
        print("="*80)
        
        for key, count in self.summary.items():
            readable_name = {
                'songs': '歌曲基本信息',
                'users': '用户基本信息',
                'collected': '用户-歌曲关联',
                'likes': '用户喜欢歌曲',
                'plays': '用户播放历史',
                'playlist_info': '歌单信息',
                'playlist_songs': '歌单歌曲',
                'comments': '歌曲评论',
                'similarity': '歌曲相似度',
                'tags': '歌曲标签',
                'external_history': '外部收听历史',
                'external_music': '外部音乐信息'
            }.get(key, key)
            
            print(f"{readable_name:20s}: {count:>12,} 条记录")
    
    def analyze_data_quality(self):
        """分析数据质量"""
        print("\n🔍 数据质量分析")
        print("="*80)
        
        for name, df in self.data.items():
            if not df.empty:
                print(f"\n{name}:")
                print(f"  形状: {df.shape}")
                print(f"  列名: {list(df.columns)[:10]}...")  # 只显示前10列
                print(f"  缺失值比例: {df.isnull().sum().sum() / (df.shape[0] * df.shape[1]):.2%}")
                
                # 显示前几行
                if len(df) > 0:
                    print(f"  示例数据:")
                    for col in df.columns[:3]:  # 显示前3列
                        if col in df.columns:
                            sample = df[col].iloc[0] if len(df) > 0 else "N/A"
                            print(f"    {col}: {str(sample)[:50]}...")

# 运行数据加载
print("正在初始化数据加载器...")
loader = CompleteMusicDataLoader()
data = loader.load_all_data_with_progress()
loader.analyze_data_quality()