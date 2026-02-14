# file: music_data_processor_separated.py (完整修改版)
"""
分离式音乐数据处理器（分别处理内部和外部数据）
修改说明：
1. 生成短ID：内部歌曲 S000001...，外部歌曲 E200001...；内部用户 U000001...，外部用户 U1000001...
2. 补全所有缺失特征字段，确保导出CSV包含数据库所需全部列
3. 增加原始ID到统一ID的映射字典，便于快速查找
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import json
from difflib import SequenceMatcher

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class SeparatedMusicDataProcessor:
    """分离式音乐数据处理器（分别处理内部和外部数据）"""
    
    def __init__(self, data_path="."):
        self.data_path = data_path
        self.data = {}
        self.song_info = {}
        self.user_info = {}
        
        # ---------- 新增：ID计数器 ----------
        self._internal_song_counter = 1      # 内部歌曲起始编号
        self._external_song_counter = 200000 # 外部歌曲起始编号（区间隔离）
        self._internal_user_counter = 1      # 内部用户起始编号
        self._external_user_counter = 1000000 # 外部用户起始编号
        
        # ---------- 新增：原始ID -> 统一ID 映射 ----------
        self.original_to_song_id = {}        # 原始歌曲ID -> 统一歌曲ID
        self.original_to_user_id = {}        # 原始用户ID -> 统一用户ID
        
        # ---------- 保留：外部音频特征缓存 ----------
        self.external_audio_features = {}
        
    def load_all_data_systematic(self):
        """系统化加载所有数据（与原代码完全相同）"""
        print("="*80)
        print("系统化加载所有数据文件...")
        print("="*80)
        
        data_files = {
            'songs': ('all_songs.csv', '内部歌曲基本信息'),
            'users': ('用户数据_20260124_200012.csv', '内部用户基本信息'),
            'collected': ('collected_user_ids_20260119_173402.csv', '内部用户-歌曲关联'),
            'likes': ('user_like_songs_20260120_132245.csv', '内部用户喜欢歌曲'),
            'plays': ('user_play_history_20260120_132245.csv', '内部用户播放历史'),
            'playlist_info': ('playlist_info_20260124_144712.csv', '内部歌单信息'),
            'playlist_songs': ('playlist_songs_20260124_144712.csv', '内部歌单歌曲'),
            'comments': ('song_comments_20260124_001212.csv', '内部歌曲评论'),
            'similarity': ('song_similarity_20260124_001212.csv', '内部歌曲相似度'),
            'tags': ('song_tags_20260124_001212.csv', '内部歌曲标签'),
            'external_history': ('User Listening History.csv', '外部收听历史'),
            'external_music': ('Music Info.csv', '外部音乐信息')
        }
        
        self.data = {}
        loaded_counts = {}
        
        for key, (filename, description) in data_files.items():
            try:
                filepath = os.path.join(self.data_path, filename)
                if os.path.exists(filepath):
                    if filename == 'User Listening History.csv':
                        print(f"加载 {description}...")
                        self.data[key] = pd.read_csv(filepath, nrows=500000)
                    elif filename == 'Music Info.csv':
                        self.data[key] = pd.read_csv(filepath)
                    else:
                        self.data[key] = pd.read_csv(filepath)
                    
                    loaded_counts[key] = len(self.data[key])
                    print(f"  ✓ {description}: {loaded_counts[key]:,} 条记录")
                    print(f"    列: {list(self.data[key].columns[:3])}...")
                else:
                    print(f"  ✗ 文件未找到: {filename}")
            except Exception as e:
                print(f"  ✗ 加载 {description} 失败: {str(e)}")
                self.data[key] = pd.DataFrame()
        
        print("\n" + "="*80)
        print(f"成功加载 {len([k for k in loaded_counts if loaded_counts[k] > 0])}/{len(data_files)} 个数据文件")
        return self.data
    
    def create_separated_song_mapping(self):
        """
        创建分离的歌曲ID映射
        修改点：生成短ID（S000001 / E200001），并填充 original_to_song_id 映射
        """
        print("\n" + "="*80)
        print("创建分离的歌曲ID映射...")
        
        internal_song_mapping = {}   # key: 歌曲名||艺术家 -> 统一ID
        external_song_mapping = {}
        song_info = {}
        
        # 重置计数器（防止多次调用时递增）
        self._internal_song_counter = 1
        self._external_song_counter = 200000
        self.original_to_song_id = {}
        
        # ---------- 1. 处理内部歌曲 ----------
        if 'songs' in self.data and not self.data['songs'].empty:
            print("处理内部歌曲数据...")
            songs_df = self.data['songs']
            
            for idx, row in songs_df.iterrows():
                original_id = str(row['song_id'])
                song_name = str(row['song_name']).strip().lower() if pd.notna(row['song_name']) else ""
                artists = str(row['artists']).strip().lower() if pd.notna(row['artists']) else ""
                
                if song_name and artists:
                    # ---------- 生成短ID：S + 6位数字 ----------
                    internal_id = f"S{self._internal_song_counter:06d}"
                    self._internal_song_counter += 1
                    
                    key = f"{song_name}||{artists}"
                    internal_song_mapping[key] = internal_id
                    # 保存原始ID映射
                    self.original_to_song_id[original_id] = internal_id
                    
                    # ---------- 尝试从外部歌曲匹配音频特征 ----------
                    audio_defaults = {
                        'danceability': 0.5, 'energy': 0.5, 'valence': 0.5,
                        'tempo': 120, 'loudness': -10, 'speechiness': 0,
                        'acousticness': 0, 'instrumentalness': 0, 'liveness': 0
                    }
                    
                    if hasattr(self, 'external_audio_features') and key in self.external_audio_features:
                        audio_features = self.external_audio_features[key]
                    else:
                        matched = False
                        if hasattr(self, 'external_audio_features'):
                            for ext_key, feat in self.external_audio_features.items():
                                if song_name in ext_key:
                                    audio_features = feat
                                    matched = True
                                    break
                        if not matched:
                            audio_features = audio_defaults
                    
                    song_info[internal_id] = {
                        'song_name': row['song_name'] if pd.notna(row['song_name']) else "",
                        'artists': row['artists'] if pd.notna(row['artists']) else "",
                        'album': row.get('album', '') if pd.notna(row.get('album')) else "",
                        'duration': row.get('duration', 0),
                        'genre': row.get('genre', '') if pd.notna(row.get('genre')) else "",
                        'popularity': row.get('popularity', 0),
                        'language': row.get('language', '') if pd.notna(row.get('language')) else "",
                        'publish_date': row.get('publish_date', '') if pd.notna(row.get('publish_date')) else '',
                        'source': 'internal',
                        'original_id': original_id,   # 保留原始ID
                        # 音频特征
                        'danceability': audio_features.get('danceability', 0.5),
                        'energy': audio_features.get('energy', 0.5),
                        'valence': audio_features.get('valence', 0.5),
                        'tempo': audio_features.get('tempo', 120),
                        'loudness': audio_features.get('loudness', -10),
                        'speechiness': audio_features.get('speechiness', 0),
                        'acousticness': audio_features.get('acousticness', 0),
                        'instrumentalness': audio_features.get('instrumentalness', 0),
                        'liveness': audio_features.get('liveness', 0)
                    }
            
            print(f"  内部歌曲数: {len(internal_song_mapping)}")
        
        # ---------- 2. 处理外部歌曲 ----------
        if 'external_music' in self.data and not self.data['external_music'].empty:
            print("处理外部歌曲数据...")
            external_df = self.data['external_music']
            
            # 统计外部歌曲播放次数
            if 'external_history' in self.data and not self.data['external_history'].empty:
                ext_hist = self.data['external_history'].copy()
                ext_hist['playcount'] = pd.to_numeric(ext_hist['playcount'], errors='coerce').fillna(1)
                song_plays = ext_hist.groupby('track_id')['playcount'].sum().to_dict()
            else:
                song_plays = {}
            
            for idx, row in external_df.iterrows():
                original_id = str(row['track_id'])
                song_name = str(row.get('name', '')).strip().lower() if pd.notna(row.get('name')) else ""
                artists = str(row.get('artist', '')).strip().lower() if pd.notna(row.get('artist')) else ""
                
                if song_name and artists:
                    # ---------- 生成短ID：E + 6位数字 ----------
                    external_id = f"E{self._external_song_counter:06d}"
                    self._external_song_counter += 1
                    
                    key = f"{song_name}||{artists}"
                    external_song_mapping[key] = external_id
                    self.original_to_song_id[original_id] = external_id
                    
                    # 计算流行度（基于播放次数）
                    play_count = song_plays.get(original_id, 0)
                    if play_count > 0:
                        log_play = np.log1p(play_count)
                        norm_pop = min(100, 30 + 70 * (log_play / 10))
                    else:
                        norm_pop = 30
                    
                    # 流派处理
                    genre_val = row.get('genre', '')
                    if pd.isna(genre_val) or genre_val == '':
                        tags_val = row.get('tags', '')
                        if pd.notna(tags_val) and tags_val != '':
                            genre_val = tags_val.split(',')[0].strip()
                        else:
                            genre_val = 'Unknown'
                    else:
                        genre_val = str(genre_val).strip()
                    
                    song_info[external_id] = {
                        'song_name': row.get('name', '') if pd.notna(row.get('name')) else "",
                        'artists': row.get('artist', '') if pd.notna(row.get('artist')) else "",
                        'album': '',
                        'duration': row.get('duration_ms', 0),
                        'genre': genre_val,
                        'popularity': norm_pop,
                        'language': '',
                        'publish_date': str(row.get('year', '')) if pd.notna(row.get('year')) else '',
                        'source': 'external',
                        'original_id': original_id,
                        'danceability': row.get('danceability', 0.5),
                        'energy': row.get('energy', 0.5),
                        'valence': row.get('valence', 0.5),
                        'sentiment': row.get('valence', 0.5),
                        'tempo': row.get('tempo', 120),
                        'loudness': row.get('loudness', -10),
                        'speechiness': row.get('speechiness', 0),
                        'acousticness': row.get('acousticness', 0),
                        'instrumentalness': row.get('instrumentalness', 0),
                        'liveness': row.get('liveness', 0)
                    }
            
            print(f"  外部歌曲数: {len(external_song_mapping)}")
            
            # 缓存外部音频特征（用于内部歌曲匹配）
            self.external_audio_features = {}
            ext_df = self.data['external_music']
            for _, row in ext_df.iterrows():
                track_id = str(row['track_id'])
                song_name = str(row.get('name', '')).strip().lower()
                artists = str(row.get('artist', '')).strip().lower()
                if song_name and artists:
                    key = f"{song_name}||{artists}"
                    self.external_audio_features[key] = {
                        'danceability': row.get('danceability', 0.5),
                        'energy': row.get('energy', 0.5),
                        'valence': row.get('valence', 0.5),
                        'tempo': row.get('tempo', 120),
                        'loudness': row.get('loudness', -10),
                        'speechiness': row.get('speechiness', 0),
                        'acousticness': row.get('acousticness', 0),
                        'instrumentalness': row.get('instrumentalness', 0),
                        'liveness': row.get('liveness', 0)
                    }
        
        # ---------- 3. 构建映射结构 ----------
        self.song_id_mapping = {
            'internal': internal_song_mapping,
            'external': external_song_mapping
        }
        self.song_info = song_info
        
        # ---------- 4. 按流派填充内部歌曲缺失的音频特征 ----------
        print("\n按流派填充未匹配内部歌曲的音频特征...")
        # 收集外部歌曲按流派的音频特征均值
        genre_audio_stats = {}
        for song_id, info in song_info.items():
            if info.get('source') == 'external':
                genre = info.get('genre', 'Unknown')
                if genre not in genre_audio_stats:
                    genre_audio_stats[genre] = {
                        'danceability': [], 'energy': [], 'valence': [], 'tempo': [],
                        'loudness': [], 'speechiness': [], 'acousticness': [],
                        'instrumentalness': [], 'liveness': []
                    }
                for feat in genre_audio_stats[genre].keys():
                    val = info.get(feat)
                    if isinstance(val, (int, float)):
                        genre_audio_stats[genre][feat].append(val)
        
        # 计算均值
        genre_audio_avg = {}
        for genre, stats in genre_audio_stats.items():
            genre_audio_avg[genre] = {}
            for feat, values in stats.items():
                if values:
                    genre_audio_avg[genre][feat] = sum(values) / len(values)
                else:
                    genre_audio_avg[genre][feat] = 0.5
        
        # 全局均值
        global_avg = {}
        for feat in ['danceability', 'energy', 'valence', 'tempo', 
                     'loudness', 'speechiness', 'acousticness', 
                     'instrumentalness', 'liveness']:
            all_vals = []
            for g, stats in genre_audio_stats.items():
                all_vals.extend(stats[feat])
            if all_vals:
                global_avg[feat] = sum(all_vals) / len(all_vals)
            else:
                global_avg[feat] = 0.5
        
        # 填充内部歌曲
        for song_id, info in song_info.items():
            if info.get('source') == 'internal':
                genre = info.get('genre', 'Unknown')
                if info.get('danceability', 0.5) == 0.5 and info.get('valence', 0.5) == 0.5:
                    if genre in genre_audio_avg:
                        for feat, avg_val in genre_audio_avg[genre].items():
                            info[feat] = avg_val
                    else:
                        for feat, avg_val in global_avg.items():
                            info[feat] = avg_val
        
        print(f"总歌曲数: {len(self.song_info)}")
        print(f"  内部: {len(internal_song_mapping)}")
        print(f"  外部: {len(external_song_mapping)}")
        print(f"  已为内部歌曲填充音频特征（匹配或按流派均值）")
        
        return self.song_id_mapping
    
    def create_separated_user_mapping(self):
        """
        创建分离的用户ID映射
        修改点：生成短ID（U000001 / U1000001），并填充 original_to_user_id 映射
        """
        print("\n" + "="*80)
        print("创建分离的用户ID映射...")
        
        internal_user_mapping = {}   # 原始ID -> 统一ID
        external_user_mapping = {}
        user_info = {}
        
        # 重置计数器
        self._internal_user_counter = 1
        self._external_user_counter = 1000000
        self.original_to_user_id = {}
        
        # ---------- 1. 内部用户 ----------
        if 'users' in self.data and not self.data['users'].empty:
            print("处理内部用户数据...")
            users_df = self.data['users']
            
            for idx, row in users_df.iterrows():
                original_id = str(row['user_id'])
                # 生成短ID：U + 6位数字
                internal_id = f"U{self._internal_user_counter:06d}"
                self._internal_user_counter += 1
                
                internal_user_mapping[original_id] = internal_id
                self.original_to_user_id[original_id] = internal_id
                
                user_info[internal_id] = {
                    'original_id': original_id,
                    'nickname': row.get('nickname', '') if pd.notna(row.get('nickname')) else "",
                    'gender': row.get('gender', 0),
                    'age': row.get('age', 25),
                    'province': row.get('province', '') if pd.notna(row.get('province')) else "",
                    'city': row.get('city', '') if pd.notna(row.get('city')) else "",
                    'listen_songs': row.get('listen_songs', 0),
                    'create_time': row.get('create_time', '') if pd.notna(row.get('create_time')) else "",
                    'source': 'internal'
                }
            
            print(f"  内部用户数: {len(internal_user_mapping)}")
        
        # ---------- 2. 外部用户 ----------
        if 'external_history' in self.data and not self.data['external_history'].empty:
            print("处理外部用户数据...")
            external_df = self.data['external_history']
            
            # 取前10万用户
            external_users = external_df['user_id'].unique()[:100000]
            
            for user_id in external_users:
                original_id = str(user_id)
                # 生成短ID：U + 7位数字（从1000000开始）
                external_id = f"U{self._external_user_counter:07d}"
                self._external_user_counter += 1
                
                external_user_mapping[original_id] = external_id
                self.original_to_user_id[original_id] = external_id
                
                user_info[external_id] = {
                    'original_id': original_id,
                    'nickname': f'外部用户_{user_id}',
                    'gender': 0,
                    'age': 25,
                    'province': '',
                    'city': '',
                    'listen_songs': 0,
                    'create_time': '',
                    'source': 'external'
                }
            
            print(f"  外部用户数: {len(external_user_mapping)}")
        
        self.user_id_mapping = {
            'internal': internal_user_mapping,
            'external': external_user_mapping
        }
        self.user_info = user_info
        
        print(f"总用户数: {len(user_info)}")
        print(f"  内部: {len(internal_user_mapping)}")
        print(f"  外部: {len(external_user_mapping)}")
        
        return self.user_id_mapping
    
    def _get_internal_song_id(self, original_id, song_name='', artists=''):
        """
        获取内部歌曲ID（简化版）
        优先通过原始ID精确查找，找不到则返回None
        """
        if not original_id:
            return None
        original_id_str = str(original_id).strip()
        # 从映射字典直接查找
        if hasattr(self, 'original_to_song_id') and original_id_str in self.original_to_song_id:
            return self.original_to_song_id[original_id_str]
        # 降级：通过歌曲名和艺术家模糊匹配（可选，保持兼容）
        if song_name and artists:
            song_name_clean = str(song_name).strip().lower()
            artists_clean = str(artists).strip().lower()
            key = f"{song_name_clean}||{artists_clean}"
            if key in self.song_id_mapping.get('internal', {}):
                return self.song_id_mapping['internal'][key]
        return None
    
    def build_internal_interaction_matrix(self):
        """构建内部用户交互矩阵（与原代码基本相同，仅ID获取方式使用 _get_internal_song_id）"""
        print("\n" + "="*80)
        print("构建内部用户交互矩阵...")
        
        all_interactions = []
        
        # 1. 内部播放历史
        if 'plays' in self.data and not self.data['plays'].empty:
            print("1. 处理内部播放历史...")
            plays_df = self.data['plays'].copy()
            plays_df['user_id'] = plays_df['user_id'].astype(str)
            
            def get_song_id_safe(row):
                try:
                    song_id = row['song_id']
                    song_name = row.get('song_name', '')
                    artists = row.get('artists', '')
                    return self._get_internal_song_id(song_id, song_name, artists)
                except Exception:
                    return None
            
            plays_df['song_id'] = plays_df.apply(get_song_id_safe, axis=1)
            plays_df = plays_df[plays_df['song_id'].notna()]
            
            if len(plays_df) > 0:
                # 用户ID直接使用映射后的统一ID（已在 _get_internal_song_id 中转换？不，需将原始user_id也转换）
                # 注意：plays_df['user_id'] 还是原始ID，需要转换为统一ID
                # 我们还没有为内部用户建立原始->统一的映射？在create_separated_user_mapping中已建立 self.original_to_user_id
                # 这里需要转换用户ID
                def map_user_id(uid):
                    uid_str = str(uid)
                    return self.original_to_user_id.get(uid_str, None)
                
                plays_df['user_id'] = plays_df['user_id'].apply(map_user_id)
                plays_df = plays_df[plays_df['user_id'].notna()]
                
                if 'play_count' in plays_df.columns:
                    plays_df['play_count'] = pd.to_numeric(plays_df['play_count'], errors='coerce').fillna(1)
                    plays_df['weight'] = np.log1p(plays_df['play_count'])
                else:
                    plays_df['weight'] = 1.0
                
                plays_df['interaction_type'] = 'play'
                all_interactions.append(plays_df[['user_id', 'song_id', 'weight', 'interaction_type']])
                print(f"   播放历史: {len(plays_df):,} 条")
        
        # 2. 内部喜欢歌曲
        if 'likes' in self.data and not self.data['likes'].empty:
            print("2. 处理内部喜欢歌曲...")
            likes_df = self.data['likes'].copy()
            likes_df['user_id'] = likes_df['user_id'].astype(str)
            
            def get_like_song_id(row):
                try:
                    song_id = row['song_id']
                    song_name = row.get('song_name', '')
                    artists = row.get('artists', '')
                    return self._get_internal_song_id(song_id, song_name, artists)
                except:
                    return None
            
            likes_df['song_id'] = likes_df.apply(get_like_song_id, axis=1)
            likes_df = likes_df[likes_df['song_id'].notna()]
            
            if len(likes_df) > 0:
                likes_df['user_id'] = likes_df['user_id'].apply(
                    lambda x: self.original_to_user_id.get(str(x), None)
                )
                likes_df = likes_df[likes_df['user_id'].notna()]
                likes_df['weight'] = 5.0
                likes_df['interaction_type'] = 'like'
                all_interactions.append(likes_df[['user_id', 'song_id', 'weight', 'interaction_type']])
                print(f"   喜欢歌曲: {len(likes_df):,} 条")
        
        # 3. 内部收藏关联
        if 'collected' in self.data and not self.data['collected'].empty:
            print("3. 处理内部收藏关联...")
            collected_df = self.data['collected'].copy()
            collected_df['user_id'] = collected_df['user_id'].astype(str)
            
            def get_collected_song_id(song_id):
                try:
                    return self._get_internal_song_id(song_id, '', '')
                except:
                    return None
            
            collected_df['song_id'] = collected_df['song_id'].apply(get_collected_song_id)
            collected_df = collected_df[collected_df['song_id'].notna()]
            
            if len(collected_df) > 0:
                collected_df['user_id'] = collected_df['user_id'].apply(
                    lambda x: self.original_to_user_id.get(str(x), None)
                )
                collected_df = collected_df[collected_df['user_id'].notna()]
                collected_df['weight'] = 3.0
                collected_df['interaction_type'] = 'collect'
                all_interactions.append(collected_df[['user_id', 'song_id', 'weight', 'interaction_type']])
                print(f"   收藏关联: {len(collected_df):,} 条")
        
        # 合并
        if all_interactions:
            internal_interactions = pd.concat(all_interactions, ignore_index=True)
            if len(internal_interactions) > 0:
                internal_matrix = internal_interactions.groupby(['user_id', 'song_id']).agg({
                    'weight': 'sum',
                    'interaction_type': lambda x: ','.join(sorted(set(x)))
                }).reset_index()
                internal_matrix.columns = ['user_id', 'song_id', 'total_weight', 'interaction_types']
                
                print(f"\n内部交互矩阵统计:")
                print(f"  总交互数: {len(internal_matrix):,}")
                print(f"  唯一用户数: {internal_matrix['user_id'].nunique():,}")
                print(f"  唯一歌曲数: {internal_matrix['song_id'].nunique():,}")
                return internal_matrix
            else:
                print("警告: 内部交互数据合并后为空!")
                return pd.DataFrame(columns=['user_id', 'song_id', 'total_weight', 'interaction_types'])
        else:
            print("警告: 没有找到内部交互数据!")
            return pd.DataFrame(columns=['user_id', 'song_id', 'total_weight', 'interaction_types'])
    
    def build_external_interaction_matrix(self):
        """构建外部用户交互矩阵（与原代码基本相同，适配短ID）"""
        print("\n" + "="*80)
        print("构建外部用户交互矩阵...")
        
        if 'external_history' not in self.data or self.data['external_history'].empty:
            print("警告: 没有外部交互数据!")
            return pd.DataFrame()
        
        print("1. 处理外部收听历史...")
        external_df = self.data['external_history'].copy()
        
        # 采样
        if len(external_df) > 300000:
            external_df = external_df.sample(n=300000, random_state=42)
        
        # 转换用户ID：使用映射字典
        def map_ext_user(uid):
            uid_str = str(uid)
            return self.original_to_user_id.get(uid_str, None)
        
        external_df['user_id'] = external_df['user_id'].apply(map_ext_user)
        external_df = external_df[external_df['user_id'].notna()]
        
        # 转换歌曲ID
        if 'external_music' in self.data and not self.data['external_music'].empty:
            external_music = self.data['external_music']
            
            track_to_info = {}
            for _, row in external_music.iterrows():
                track_id = str(row['track_id'])
                track_to_info[track_id] = {
                    'name': str(row.get('name', '')),
                    'artist': str(row.get('artist', ''))
                }
            
            def get_external_song_id(track_id):
                # 通过原始ID直接查找映射
                if track_id in self.original_to_song_id:
                    return self.original_to_song_id[track_id]
                return None
            
            external_df['song_id'] = external_df['track_id'].apply(get_external_song_id)
        else:
            external_df['song_id'] = None
        
        external_df = external_df[external_df['song_id'].notna()]
        
        if 'playcount' in external_df.columns:
            external_df['playcount'] = pd.to_numeric(external_df['playcount'], errors='coerce').fillna(1)
            external_df['weight'] = np.log1p(external_df['playcount'])
        else:
            external_df['weight'] = 1.0
        
        external_df['interaction_type'] = 'external_play'
        
        if len(external_df) > 0:
            external_matrix = external_df.groupby(['user_id', 'song_id']).agg({
                'weight': 'sum',
                'interaction_type': lambda x: ','.join(sorted(set(x)))
            }).reset_index()
            external_matrix.columns = ['user_id', 'song_id', 'total_weight', 'interaction_types']
            
            print(f"\n外部交互矩阵统计:")
            print(f"  总交互数: {len(external_matrix):,}")
            print(f"  唯一用户数: {external_matrix['user_id'].nunique():,}")
            print(f"  唯一歌曲数: {external_matrix['song_id'].nunique():,}")
            return external_matrix
        else:
            print("警告: 外部数据映射后为空!")
            return pd.DataFrame()
    
    def create_separated_features(self, internal_matrix, external_matrix):
        """创建分离的特征数据（主要修改在 _create_combined_song_features 和 _create_user_features）"""
        print("\n" + "="*80)
        print("创建分离的特征数据...")
        
        # 1. 歌曲特征（合并内部和外部）
        print("1. 创建合并的歌曲特征...")
        song_features = self._create_combined_song_features(internal_matrix, external_matrix)
        self.song_features = song_features      # ← 新增：保存歌曲特征供用户特征计算使用
        
        # 2. 内部用户特征
        print("\n2. 创建内部用户特征...")
        internal_user_features = self._create_user_features(internal_matrix, 'internal')
        
        # 3. 外部用户特征
        print("\n3. 创建外部用户特征...")
        external_user_features = self._create_user_features(external_matrix, 'external')
        
        return {
            'song_features': song_features,
            'internal_user_features': internal_user_features,
            'external_user_features': external_user_features,
            'internal_matrix': internal_matrix,
            'external_matrix': external_matrix
        }
    
    def _create_combined_song_features(self, internal_matrix=None, external_matrix=None):
        """
        创建合并的歌曲特征
        修改点：
        - 添加歌曲交互统计：weight_sum, weight_mean, weight_std, unique_users
        - 调用 _engineer_song_features 时已包含 genre_clean, recency_score, popularity_tier
        - 重命名 original_id -> original_song_id
        """
        song_features_data = []
        
        for song_id, info in self.song_info.items():
            feature_dict = {
                'song_id': song_id,
                'song_name': info.get('song_name', ''),
                'artists': info.get('artists', ''),
                'album': info.get('album', ''),
                'duration_ms': info.get('duration', 0),
                'genre': info.get('genre', ''),
                'popularity': info.get('popularity', 0),
                'source': info.get('source', ''),
                'original_id': info.get('original_id', ''),  # 稍后重命名
                'publish_year': self._extract_year(info.get('publish_date', ''))
            }
            
            # 情感特征
            if info.get('source') == 'internal':
                sentiment_val = info.get('avg_sentiment', 0.5)
            else:
                sentiment_val = info.get('valence', 0.5)
            feature_dict['sentiment'] = sentiment_val
            
            # 音频特征
            audio_features = ['danceability', 'energy', 'valence', 'tempo', 
                            'loudness', 'speechiness', 'acousticness', 
                            'instrumentalness', 'liveness']
            for feat in audio_features:
                feature_dict[feat] = info.get(feat, 0.5)
            
            song_features_data.append(feature_dict)
        
        song_features = pd.DataFrame(song_features_data)

        # ========== 移植：标签特征 ==========
        if 'tags' in self.data and not self.data['tags'].empty:
            print("   正在合并标签特征...")
            tags_df = self.data['tags'].copy()
            tags_df['unified_song_id'] = tags_df['song_id'].apply(
                lambda x: self._get_internal_song_id(x, '', '')  # 使用已有的ID映射函数
            )
            tags_df = tags_df[tags_df['unified_song_id'].notna()]
            if not tags_df.empty:
                tags_df['score'] = pd.to_numeric(tags_df['score'], errors='coerce')
                tag_features = tags_df.groupby('unified_song_id').agg(
                    tag_score_mean=('score', 'mean'),
                    tag_count=('score', 'count')
                ).reset_index()
                song_features = song_features.merge(tag_features, left_on='song_id', right_on='unified_song_id', how='left')
                song_features.drop(columns=['unified_song_id'], inplace=True, errors='ignore')

        # ========== 移植：评论特征 ==========
        if 'comments' in self.data and not self.data['comments'].empty:
            print("   正在合并评论特征...")
            comments_df = self.data['comments'].copy()
            comments_df['unified_song_id'] = comments_df['song_id'].apply(
                lambda x: self._get_internal_song_id(x, '', '')
            )
            comments_df = comments_df[comments_df['unified_song_id'].notna()]
            if not comments_df.empty:
                comments_df['sentiment_score'] = pd.to_numeric(comments_df['sentiment_score'], errors='coerce')
                comments_df['liked_count'] = pd.to_numeric(comments_df['liked_count'], errors='coerce')
                comment_features = comments_df.groupby('unified_song_id').agg(
                    avg_sentiment=('sentiment_score', 'mean'),
                    comment_count=('sentiment_score', 'count'),
                    total_likes=('liked_count', 'sum')
                ).reset_index()
                song_features = song_features.merge(comment_features, left_on='song_id', right_on='unified_song_id', how='left')
                song_features.drop(columns=['unified_song_id'], inplace=True, errors='ignore')

        # ========== 移植：相似度特征 ==========
        if 'similarity' in self.data and not self.data['similarity'].empty:
            print("   正在合并相似度特征...")
            similarity_df = self.data['similarity'].copy()
            similarity_df['unified_song_id'] = similarity_df['song_id'].apply(
                lambda x: self._get_internal_song_id(x, '', '')
            )
            similarity_df = similarity_df[similarity_df['unified_song_id'].notna()]
            if not similarity_df.empty:
                similarity_df['similarity_score'] = pd.to_numeric(similarity_df['similarity_score'], errors='coerce')
                sim_features = similarity_df.groupby('unified_song_id').agg(
                    avg_similarity=('similarity_score', 'mean'),
                    max_similarity=('similarity_score', 'max'),
                    similar_songs_count=('similarity_score', 'count')
                ).reset_index()
                song_features = song_features.merge(sim_features, left_on='song_id', right_on='unified_song_id', how='left')
                song_features.drop(columns=['unified_song_id'], inplace=True, errors='ignore')

        # ========== 移植：歌单特征 ==========
        if 'playlist_songs' in self.data and not self.data['playlist_songs'].empty:
            print("   正在合并歌单特征...")
            playlist_df = self.data['playlist_songs'].copy()
            # 使用歌曲名和艺术家进行更准确的映射
            playlist_df['unified_song_id'] = playlist_df.apply(
                lambda x: self._get_internal_song_id(x['song_id'], x.get('song_name', ''), x.get('artists', '')),
                axis=1
            )
            playlist_df = playlist_df[playlist_df['unified_song_id'].notna()]
            if not playlist_df.empty:
                playlist_df['order'] = pd.to_numeric(playlist_df['order'], errors='coerce')
                playlist_features = playlist_df.groupby('unified_song_id').agg(
                    playlist_count=('playlist_id', 'nunique'),
                    avg_playlist_order=('order', 'mean')
                ).reset_index()
                song_features = song_features.merge(playlist_features, left_on='song_id', right_on='unified_song_id', how='left')
                song_features.drop(columns=['unified_song_id'], inplace=True, errors='ignore')
        
        # ---------- 新增：歌曲交互统计（weight_sum, weight_mean, weight_std, unique_users）----------
        all_interactions = pd.DataFrame()
        if internal_matrix is not None and not internal_matrix.empty:
            all_interactions = pd.concat([all_interactions, internal_matrix], ignore_index=True)
        if external_matrix is not None and not external_matrix.empty:
            all_interactions = pd.concat([all_interactions, external_matrix], ignore_index=True)
        
        if not all_interactions.empty:
            song_stats = all_interactions.groupby('song_id').agg({
                'total_weight': ['sum', 'mean', 'std'],
                'user_id': 'nunique'
            }).reset_index()
            song_stats.columns = ['song_id', 'weight_sum', 'weight_mean', 'weight_std', 'unique_users']
            song_features = song_features.merge(song_stats, on='song_id', how='left')
        else:
            song_features['weight_sum'] = 0.0
            song_features['weight_mean'] = 0.0
            song_features['weight_std'] = None
            song_features['unique_users'] = 0
        
        # 重命名 original_id -> original_song_id
        song_features.rename(columns={'original_id': 'original_song_id'}, inplace=True)
        
        # 特征工程（包括新增的 genre_clean, recency_score, popularity_tier）
        song_features = self._engineer_song_features(song_features)
        
        return song_features
    
    def _engineer_song_features(self, df):
        """歌曲特征工程（增强版）"""
        # 流行度
        if 'popularity' in df.columns:
            df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce').fillna(50)
        
        # 时长特征
        if 'duration_ms' in df.columns:
            df['duration_minutes'] = pd.to_numeric(df['duration_ms'], errors='coerce') / 60000
            df['duration_minutes'] = df['duration_minutes'].fillna(3.5)
        
        # 年份特征
        current_year = datetime.now().year
        if 'publish_year' in df.columns:
            df['publish_year'] = pd.to_numeric(df['publish_year'], errors='coerce').fillna(current_year - 5)
            df['song_age'] = current_year - df['publish_year']
            df['song_age'] = df['song_age'].clip(lower=0)
        
        # 音频特征组合
        audio_cols = ['danceability', 'energy', 'valence']
        for col in audio_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.5)
        
        if all(col in df.columns for col in ['danceability', 'energy']):
            df['energy_dance'] = (df['danceability'] + df['energy']) / 2
        
        if all(col in df.columns for col in ['valence', 'energy']):
            df['mood_score'] = (df['valence'] * 0.6 + df['energy'] * 0.4)
        
        # final_popularity
        if 'popularity' in df.columns:
            min_pop = df['popularity'].min()
            max_pop = df['popularity'].max()
            if max_pop > min_pop:
                df['final_popularity'] = 10 + 90 * (df['popularity'] - min_pop) / (max_pop - min_pop)
            else:
                df['final_popularity'] = 50.0
            df['final_popularity_norm'] = df['final_popularity'] / 100.0
        
        # ---------- 新增：genre_clean ----------
        if 'genre' in df.columns:
            df['genre_clean'] = df['genre'].apply(self._clean_genre)
        else:
            df['genre_clean'] = 'Unknown'
        
        # ---------- 新增：recency_score ----------
        if 'song_age' in df.columns:
            max_age = 50
            df['recency_score'] = 1 - (df['song_age'].clip(0, max_age) / max_age)
        else:
            df['recency_score'] = 0.5
        
        # ---------- 新增：popularity_tier ----------
        if 'final_popularity' in df.columns:
            def tier_from_pop(pop):
                if pop >= 80:
                    return 'hit'
                elif pop >= 50:
                    return 'popular'
                else:
                    return 'normal'
            df['popularity_tier'] = df['final_popularity'].apply(tier_from_pop)
        else:
            df['popularity_tier'] = 'normal'
        
        return df
    
    def _clean_genre(self, genre_str):
        """清洗流派名称"""
        if pd.isna(genre_str) or genre_str == '':
            return 'Unknown'
        genre = str(genre_str).strip().lower()
        # 简单的同义词合并
        if 'pop' in genre:
            return 'Pop'
        elif 'rock' in genre:
            return 'Rock'
        elif 'hip hop' in genre or 'rap' in genre:
            return 'Hip-Hop'
        elif 'electronic' in genre or 'edm' in genre:
            return 'Electronic'
        elif 'jazz' in genre:
            return 'Jazz'
        elif 'classical' in genre:
            return 'Classical'
        elif 'rnb' in genre or 'r&b' in genre:
            return 'R&B'
        elif 'country' in genre:
            return 'Country'
        elif 'metal' in genre:
            return 'Metal'
        elif 'folk' in genre:
            return 'Folk'
        else:
            return genre.title()
    
    def _create_user_features(self, interaction_matrix, source_type):
        """
        创建用户特征（完整版）
        新增字段：
        - weight_std
        - age_group, activity_level
        - top_genre_1/2/3
        - avg_popularity_pref, popularity_bias
        - original_user_id
        """
        if interaction_matrix.empty:
            print(f"  {source_type}用户交互矩阵为空，创建基础特征...")
            user_features_data = []
            for user_id, info in self.user_info.items():
                if info.get('source') == source_type:
                    feature_dict = {
                        'user_id': user_id,
                        'nickname': info.get('nickname', ''),
                        'gender': info.get('gender', 0),
                        'age': info.get('age', 25),
                        'province': info.get('province', ''),
                        'city': info.get('city', ''),
                        'listen_songs': info.get('listen_songs', 0),
                        'source': source_type,
                        'unique_songs': 0,
                        'total_interactions': 0,
                        'total_weight_sum': 0.0,
                        'avg_weight': 0.0,
                        'weight_std': None,
                        'age_group': self._map_age_group(info.get('age', 25)),
                        'activity_level': 'inactive',
                        'diversity_ratio': 0.0,
                        'top_genre_1': None,
                        'top_genre_2': None,
                        'top_genre_3': None,
                        'avg_popularity_pref': None,
                        'popularity_bias': None,
                        'original_user_id': info.get('original_id', '')
                    }
                    user_features_data.append(feature_dict)
            user_features = pd.DataFrame(user_features_data)
        else:
            print(f"  {source_type}用户交互矩阵有数据，创建统计特征...")
            
            # 基础统计
            user_stats = interaction_matrix.groupby('user_id').agg({
                'total_weight': ['sum', 'mean'],
                'song_id': 'nunique'
            }).reset_index()
            user_stats.columns = ['user_id', 'total_weight_sum', 'avg_weight', 'unique_songs']
            
            # 总交互次数
            user_counts = interaction_matrix.groupby('user_id').size().reset_index(name='total_interactions')
            user_stats = user_stats.merge(user_counts, on='user_id', how='left')
            
            # ---------- 新增：weight_std ----------
            if 'total_weight' in interaction_matrix.columns:
                weight_std_df = interaction_matrix.groupby('user_id')['total_weight'].std().reset_index(name='weight_std')
                user_stats = user_stats.merge(weight_std_df, on='user_id', how='left')
            else:
                user_stats['weight_std'] = None
            
            # 从user_info获取基础信息
            user_features_data = []
            for user_id in user_stats['user_id'].unique():
                if user_id in self.user_info:
                    info = self.user_info[user_id]
                    feature_dict = {
                        'user_id': user_id,
                        'nickname': info.get('nickname', ''),
                        'gender': info.get('gender', 0),
                        'age': info.get('age', 25),
                        'province': info.get('province', ''),
                        'city': info.get('city', ''),
                        'listen_songs': info.get('listen_songs', 0),
                        'source': info.get('source', source_type),
                        'original_user_id': info.get('original_id', '')
                    }
                    user_features_data.append(feature_dict)
            
            user_features = pd.DataFrame(user_features_data)
            user_features = pd.merge(user_features, user_stats, on='user_id', how='left')
            
            # ---------- 新增：年龄分组 ----------
            user_features['age_group'] = user_features['age'].apply(self._map_age_group)
            
            # ---------- 新增：活跃度分组 ----------
            user_features['activity_level'] = user_features['total_interactions'].apply(self._map_activity_level)
            
            # ---------- 新增：多样性比率（计算略，可后续扩展）----------
            user_features['diversity_ratio'] = 0.0  # 默认值，可后续完善
            
            # ---------- 新增：用户偏好特征（需关联歌曲表）----------
            if hasattr(self, 'song_features') and not self.song_features.empty:
                song_subset = self.song_features[['song_id', 'genre_clean', 'final_popularity']].copy()
                user_interactions = interaction_matrix.merge(song_subset, on='song_id', how='left')
                
                # --- top_genre ---
                if 'genre_clean' in user_interactions.columns:
                    genre_counts = user_interactions.groupby(['user_id', 'genre_clean']).size().reset_index(name='count')
                    top_genres = genre_counts.sort_values(['user_id', 'count'], ascending=[True, False]) \
                                            .groupby('user_id').head(3)
                    top_genres_pivot = top_genres.groupby('user_id')['genre_clean'].apply(list).reset_index()
                    top_genres_pivot['top_genre_1'] = top_genres_pivot['genre_clean'].apply(lambda x: x[0] if len(x)>0 else None)
                    top_genres_pivot['top_genre_2'] = top_genres_pivot['genre_clean'].apply(lambda x: x[1] if len(x)>1 else None)
                    top_genres_pivot['top_genre_3'] = top_genres_pivot['genre_clean'].apply(lambda x: x[2] if len(x)>2 else None)
                    top_genres_pivot.drop('genre_clean', axis=1, inplace=True)
                    user_features = user_features.merge(top_genres_pivot, on='user_id', how='left')
                
                # --- avg_popularity_pref & popularity_bias ---
                if 'final_popularity' in user_interactions.columns:
                    avg_pop = user_interactions.groupby('user_id')['final_popularity'].mean().reset_index(name='avg_popularity_pref')
                    user_features = user_features.merge(avg_pop, on='user_id', how='left')
                    global_avg_pop = self.song_features['final_popularity'].mean()
                    user_features['popularity_bias'] = user_features['avg_popularity_pref'] - global_avg_pop
            else:
                # 无歌曲特征时置空
                user_features['top_genre_1'] = None
                user_features['top_genre_2'] = None
                user_features['top_genre_3'] = None
                user_features['avg_popularity_pref'] = None
                user_features['popularity_bias'] = None
        
        # 填充缺失值
        fill_cols = ['total_weight_sum', 'avg_weight', 'unique_songs', 'total_interactions']
        for col in fill_cols:
            if col in user_features.columns:
                user_features[col] = user_features[col].fillna(0)
        
        # 确保年龄字段为数值
        if 'age' in user_features.columns:
            user_features['age'] = pd.to_numeric(user_features['age'], errors='coerce').fillna(25)
        
        # 如果 age_group 或 activity_level 未生成，补充默认值
        if 'age_group' not in user_features.columns:
            user_features['age_group'] = 'unknown'
        if 'activity_level' not in user_features.columns:
            user_features['activity_level'] = 'inactive'
        if 'diversity_ratio' not in user_features.columns:
            user_features['diversity_ratio'] = 0.0
        
        return user_features
    
    def _map_age_group(self, age):
        """年龄分段"""
        if pd.isna(age) or age == 0:
            return 'unknown'
        age = int(age)
        if age < 18:
            return 'under18'
        elif age < 26:
            return '18-25'
        elif age < 36:
            return '26-35'
        elif age < 51:
            return '36-50'
        else:
            return '50plus'
    
    def _map_activity_level(self, total_int):
        """活跃度分段"""
        if pd.isna(total_int) or total_int == 0:
            return 'inactive'
        if total_int >= 100:
            return 'high'
        elif total_int >= 30:
            return 'medium'
        else:
            return 'low'
    
    def _extract_year(self, date_str):
        """提取年份（与原代码相同）"""
        if pd.isna(date_str):
            return 2020
        try:
            if isinstance(date_str, str):
                import re
                match = re.search(r'(\d{4})', date_str)
                if match:
                    return int(match.group(1))
            if isinstance(date_str, (int, float)):
                return int(date_str)
        except:
            pass
        return 2020
    
    def filter_sparse_data(self, matrix, min_user_interactions=3, min_song_interactions=3, source_type='internal'):
        """过滤稀疏数据（与原代码相同）"""
        print(f"\n过滤{source_type}稀疏数据 (用户≥{min_user_interactions}次, 歌曲≥{min_song_interactions}次)...")
        if matrix.empty:
            print(f"  {source_type}矩阵为空，跳过过滤")
            return matrix
        
        original_stats = {
            'users': matrix['user_id'].nunique(),
            'songs': matrix['song_id'].nunique(),
            'interactions': len(matrix)
        }
        
        user_counts = matrix.groupby('user_id').size()
        active_users = user_counts[user_counts >= min_user_interactions].index
        song_counts = matrix.groupby('song_id').size()
        active_songs = song_counts[song_counts >= min_song_interactions].index
        
        filtered_matrix = matrix[
            matrix['user_id'].isin(active_users) & 
            matrix['song_id'].isin(active_songs)
        ].copy()
        
        filtered_stats = {
            'users': filtered_matrix['user_id'].nunique(),
            'songs': filtered_matrix['song_id'].nunique(),
            'interactions': len(filtered_matrix)
        }
        
        print(f"过滤前: {original_stats['users']:,} 用户, {original_stats['songs']:,} 歌曲, {original_stats['interactions']:,} 交互")
        print(f"过滤后: {filtered_stats['users']:,} 用户, {filtered_stats['songs']:,} 歌曲, {filtered_stats['interactions']:,} 交互")
        
        return filtered_matrix
    
    def split_train_test(self, matrix, test_size=0.2, random_state=42):
        """划分训练集和测试集（与原代码相同）"""
        if matrix.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        train_data = []
        test_data = []
        np.random.seed(random_state)
        
        for user_id in matrix['user_id'].unique():
            user_interactions = matrix[matrix['user_id'] == user_id]
            if len(user_interactions) >= 5:
                n_test = max(1, int(len(user_interactions) * test_size))
                test_indices = np.random.choice(user_interactions.index, size=n_test, replace=False)
                train_indices = user_interactions.index.difference(test_indices)
                train_data.append(user_interactions.loc[train_indices])
                test_data.append(user_interactions.loc[test_indices])
            elif len(user_interactions) >= 2:
                test_indices = np.random.choice(user_interactions.index, size=1, replace=False)
                train_indices = user_interactions.index.difference(test_indices)
                train_data.append(user_interactions.loc[train_indices])
                test_data.append(user_interactions.loc[test_indices])
            else:
                train_data.append(user_interactions)
        
        if train_data:
            train_interactions = pd.concat(train_data, ignore_index=True)
        else:
            train_interactions = pd.DataFrame()
        if test_data:
            test_interactions = pd.concat(test_data, ignore_index=True)
        else:
            test_interactions = pd.DataFrame()
        
        print(f"训练集: {len(train_interactions):,} 条记录")
        print(f"测试集: {len(test_interactions):,} 条记录")
        return train_interactions, test_interactions
    
    def save_separated_data(self, data_dict):
        """
        保存分离的数据
        修改点：确保所有数据库需要的列都存在于CSV中
        """
        print("\n" + "="*80)
        print("保存分离的数据...")
        
        output_dir = "separated_processed_data"
        os.makedirs(output_dir, exist_ok=True)
        
        # ---------- 1. 歌曲特征 ----------
        song_features = data_dict['song_features']
        # 确保必要的列存在
        required_song_cols = ['song_id', 'song_name', 'artists', 'album', 'duration_ms', 'genre',
                             'popularity', 'source', 'original_song_id', 'publish_year',
                             'danceability', 'energy', 'valence', 'tempo', 'loudness',
                             'speechiness', 'acousticness', 'instrumentalness', 'liveness',
                             'sentiment', 'duration_minutes', 'song_age', 'energy_dance', 
                             'mood_score', 'final_popularity', 'final_popularity_norm',
                             'genre_clean', 'recency_score', 'popularity_tier',
                             'weight_sum', 'weight_mean', 'weight_std', 'unique_users']
        for col in required_song_cols:
            if col not in song_features.columns:
                if col in ['weight_std']:
                    song_features[col] = None
                else:
                    song_features[col] = 0 if col in ['duration_ms', 'publish_year', 'unique_users'] else 0.0
        song_features.to_csv(
            os.path.join(output_dir, "all_song_features.csv"), 
            index=False, encoding='utf-8'
        )
        print(f"✓ 歌曲特征已保存 ({len(song_features):,} 条记录)")
        # 缓存到实例，供后续用户特征计算使用
        self.song_features = song_features
        
        # ---------- 2. 内部数据 ----------
        internal_dir = os.path.join(output_dir, "internal")
        os.makedirs(internal_dir, exist_ok=True)
        
        internal_user_features = data_dict['internal_user_features']
        # 确保用户特征列完整
        required_user_cols = ['user_id', 'nickname', 'gender', 'age', 'province', 'city',
                             'listen_songs', 'source', 'unique_songs', 'total_interactions',
                             'total_weight_sum', 'avg_weight', 'weight_std', 'age_group',
                             'activity_level', 'diversity_ratio', 'top_genre_1', 'top_genre_2',
                             'top_genre_3', 'avg_popularity_pref', 'popularity_bias',
                             'original_user_id', 'role']
        for col in required_user_cols:
            if col not in internal_user_features.columns:
                if col == 'role':
                    internal_user_features[col] = 'user'
                elif col in ['weight_std', 'top_genre_1', 'top_genre_2', 'top_genre_3',
                            'avg_popularity_pref', 'popularity_bias']:
                    internal_user_features[col] = None
                else:
                    internal_user_features[col] = 0 if col in ['unique_songs', 'total_interactions'] else 0.0
        internal_user_features.to_csv(
            os.path.join(internal_dir, "user_features.csv"), 
            index=False, encoding='utf-8'
        )
        
        internal_matrix = data_dict['internal_matrix']
        internal_matrix.to_csv(
            os.path.join(internal_dir, "interaction_matrix.csv"), 
            index=False, encoding='utf-8'
        )
        
        internal_train, internal_test = self.split_train_test(internal_matrix)
        internal_train.to_csv(
            os.path.join(internal_dir, "train_interactions.csv"), 
            index=False, encoding='utf-8'
        )
        internal_test.to_csv(
            os.path.join(internal_dir, "test_interactions.csv"), 
            index=False, encoding='utf-8'
        )
        
        print(f"✓ 内部数据已保存")
        print(f"  用户: {len(internal_user_features):,}")
        print(f"  交互: {len(internal_matrix):,}")
        print(f"  训练集: {len(internal_train):,}")
        print(f"  测试集: {len(internal_test):,}")
        
        # ---------- 3. 外部数据 ----------
        external_dir = os.path.join(output_dir, "external")
        os.makedirs(external_dir, exist_ok=True)
        
        external_user_features = data_dict['external_user_features']
        # 同样确保列完整
        for col in required_user_cols:
            if col not in external_user_features.columns:
                if col == 'role':
                    external_user_features[col] = 'user'
                elif col in ['weight_std', 'top_genre_1', 'top_genre_2', 'top_genre_3',
                            'avg_popularity_pref', 'popularity_bias']:
                    external_user_features[col] = None
                else:
                    external_user_features[col] = 0
        external_user_features.to_csv(
            os.path.join(external_dir, "user_features.csv"), 
            index=False, encoding='utf-8'
        )
        
        external_matrix = data_dict['external_matrix']
        external_matrix.to_csv(
            os.path.join(external_dir, "interaction_matrix.csv"), 
            index=False, encoding='utf-8'
        )
        
        external_train, external_test = self.split_train_test(external_matrix)
        external_train.to_csv(
            os.path.join(external_dir, "train_interactions.csv"), 
            index=False, encoding='utf-8'
        )
        external_test.to_csv(
            os.path.join(external_dir, "test_interactions.csv"), 
            index=False, encoding='utf-8'
        )
        
        print(f"✓ 外部数据已保存")
        print(f"  用户: {len(external_user_features):,}")
        print(f"  交互: {len(external_matrix):,}")
        print(f"  训练集: {len(external_train):,}")
        print(f"  测试集: {len(external_test):,}")
        
        # ---------- 4. 统计信息 ----------
        stats = {
            'total_songs': len(song_features),
            'internal_users': len(internal_user_features),
            'internal_interactions': len(internal_matrix),
            'external_users': len(external_user_features),
            'external_interactions': len(external_matrix),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(os.path.join(output_dir, "data_stats.json"), 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 所有分离数据已保存到: {output_dir}")
        return stats


def main_separated():
    """分离式数据处理主函数（与原代码基本相同）"""
    print("="*80)
    print("分离式音乐数据预处理系统")
    print("="*80)
    
    try:
        processor = SeparatedMusicDataProcessor(data_path=".")
        
        print("\n阶段1: 加载原始数据")
        processor.load_all_data_systematic()
        
        print("\n阶段2: 创建分离的ID映射")
        processor.create_separated_song_mapping()
        processor.create_separated_user_mapping()
        
        print("\n阶段3: 构建分离的交互矩阵")
        internal_matrix = processor.build_internal_interaction_matrix()
        external_matrix = processor.build_external_interaction_matrix()
        
        print("\n阶段4: 过滤稀疏数据")
        internal_matrix_filtered = processor.filter_sparse_data(
            internal_matrix, min_user_interactions=5, min_song_interactions=5, source_type='internal'
        )
        external_matrix_filtered = processor.filter_sparse_data(
            external_matrix, min_user_interactions=3, min_song_interactions=3, source_type='external'
        )
        
        print("\n阶段5: 创建特征数据")
        all_data = processor.create_separated_features(internal_matrix_filtered, external_matrix_filtered)
        
        print("\n阶段6: 保存数据")
        stats = processor.save_separated_data(all_data)
        
        print("\n" + "="*80)
        print("分离式数据处理完成!")
        print("="*80)
        print("\n📊 最终统计信息:")
        print(f"1. 总歌曲数: {stats['total_songs']:,}")
        print(f"2. 内部用户数: {stats['internal_users']:,}")
        print(f"3. 内部交互数: {stats['internal_interactions']:,}")
        print(f"4. 外部用户数: {stats['external_users']:,}")
        print(f"5. 外部交互数: {stats['external_interactions']:,}")
        
        print("\n✅ 分离式数据处理流程全部完成!")
        return processor
        
    except Exception as e:
        print(f"\n❌ 主程序执行时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    processor = main_separated()