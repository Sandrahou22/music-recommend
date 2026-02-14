# file: music_data_processor_complete.py
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

class UnifiedMusicDataProcessor:
    """统一音乐数据处理器"""
    
    def __init__(self, data_path="."):
        self.data_path = data_path
        self.data = {}
        self.song_id_mapping = {}
        self.user_id_mapping = {}
        
    def load_all_data_systematic(self):
        """系统化加载所有数据"""
        print("="*80)
        print("系统化加载所有数据文件...")
        print("="*80)
        
        data_files = {
            'songs': ('all_songs.csv', '歌曲基本信息'),
            'users': ('用户数据_20260124_200012.csv', '用户基本信息'),
            'collected': ('collected_user_ids_20260119_173402.csv', '用户-歌曲关联'),
            'likes': ('user_like_songs_20260120_132245.csv', '用户喜欢歌曲'),
            'plays': ('user_play_history_20260120_132245.csv', '用户播放历史'),
            'playlist_info': ('playlist_info_20260124_144712.csv', '歌单信息'),
            'playlist_songs': ('playlist_songs_20260124_144712.csv', '歌单歌曲'),
            'comments': ('song_comments_20260124_001212.csv', '歌曲评论'),
            'similarity': ('song_similarity_20260124_001212.csv', '歌曲相似度'),
            'tags': ('song_tags_20260124_001212.csv', '歌曲标签'),
            'external_history': ('User Listening History.csv', '外部收听历史'),
            'external_music': ('Music Info.csv', '外部音乐信息')
        }
        
        self.data = {}
        loaded_counts = {}
        
        for key, (filename, description) in data_files.items():
            try:
                filepath = os.path.join(self.data_path, filename)
                if os.path.exists(filepath):
                    # 对于大数据文件进行采样
                    if filename == 'User Listening History.csv':
                        print(f"加载 {description}...")
                        # 读取前100万行，避免内存问题
                        self.data[key] = pd.read_csv(filepath, nrows=1000000)
                    else:
                        self.data[key] = pd.read_csv(filepath)
                    
                    loaded_counts[key] = len(self.data[key])
                    print(f"  ✓ {description}: {loaded_counts[key]:,} 条记录")
                    
                    # 显示前几列信息
                    print(f"    列: {list(self.data[key].columns[:5])}...")
                else:
                    print(f"  ✗ 文件未找到: {filename}")
            except Exception as e:
                print(f"  ✗ 加载 {description} 失败: {str(e)}")
                self.data[key] = pd.DataFrame()
        
        print("\n" + "="*80)
        print(f"成功加载 {len([k for k in loaded_counts if loaded_counts[k] > 0])}/{len(data_files)} 个数据文件")
        
        return self.data
    
    def create_unified_song_mapping(self):
        """创建统一的歌曲ID映射"""
        print("\n" + "="*80)
        print("创建统一的歌曲ID映射...")
        
        song_mapping = {}
        song_info = {}
        
        # 1. 从爬取数据中获取歌曲信息
        if 'songs' in self.data and not self.data['songs'].empty:
            songs_df = self.data['songs']
            for _, row in songs_df.iterrows():
                song_id = str(row['song_id'])
                song_name = str(row['song_name']).strip().lower()
                artists = str(row['artists']).strip().lower()
                
                key = f"{song_name}||{artists}"
                if key not in song_mapping:
                    song_mapping[key] = {
                        'unified_id': f"song_{len(song_mapping)+1:08d}",
                        'sources': []
                    }
                
                song_mapping[key]['sources'].append({
                    'source': 'internal',
                    'song_id': song_id,
                    'original_song_name': row['song_name'],
                    'original_artists': row['artists']
                })
                
                # 保存歌曲信息
                unified_id = song_mapping[key]['unified_id']
                song_info[unified_id] = {
                    'song_name': row['song_name'],
                    'artists': row['artists'],
                    'album': row.get('album', ''),
                    'duration': row.get('duration', 0),
                    'genre': row.get('genre', ''),
                    'popularity': row.get('popularity', 0),
                    'language': row.get('language', ''),
                    'publish_date': row.get('publish_date', '')
                }
        
        # 2. 从外部数据中获取歌曲信息
        if 'external_music' in self.data and not self.data['external_music'].empty:
            external_df = self.data['external_music']
            for _, row in external_df.iterrows():
                track_id = str(row['track_id'])
                song_name = str(row.get('name', '')).strip().lower()
                artists = str(row.get('artist', '')).strip().lower()
                
                if song_name and artists:
                    key = f"{song_name}||{artists}"
                    
                    if key not in song_mapping:
                        song_mapping[key] = {
                            'unified_id': f"song_{len(song_mapping)+1:08d}",
                            'sources': []
                        }
                    
                    song_mapping[key]['sources'].append({
                        'source': 'external',
                        'song_id': track_id,
                        'original_song_name': row.get('name', ''),
                        'original_artists': row.get('artist', '')
                    })
                    
                    # 保存外部歌曲信息
                    unified_id = song_mapping[key]['unified_id']
                    if unified_id not in song_info:
                        song_info[unified_id] = {
                            'song_name': row.get('name', ''),
                            'artists': row.get('artist', ''),
                            'album': '',
                            'duration': row.get('duration_ms', 0),
                            'genre': row.get('genre', ''),
                            'popularity': 0,
                            'language': '',
                            'publish_date': str(row.get('year', '')) if pd.notna(row.get('year')) else ''
                        }
        
        # 3. 创建双向映射
        self.song_id_mapping = {
            'key_to_unified': {},
            'unified_to_sources': {}
        }
        
        for key, mapping_info in song_mapping.items():
            unified_id = mapping_info['unified_id']
            self.song_id_mapping['key_to_unified'][key] = unified_id
            
            for source_info in mapping_info['sources']:
                source_key = f"{source_info['source']}_{source_info['song_id']}"
                self.song_id_mapping['key_to_unified'][source_key] = unified_id
            
            self.song_id_mapping['unified_to_sources'][unified_id] = mapping_info['sources']
        
        self.song_info = song_info
        
        print(f"创建了 {len(song_mapping)} 个统一歌曲ID")
        print(f"映射了 {len(self.song_id_mapping['key_to_unified'])} 个原始ID")
        
        return self.song_id_mapping
    
    def create_unified_user_mapping(self):
        """创建统一的用户ID映射"""
        print("\n" + "="*80)
        print("创建统一的用户ID映射...")
        
        user_mapping = {}
        user_info = {}
        
        # 1. 从用户数据中获取用户信息
        if 'users' in self.data and not self.data['users'].empty:
            users_df = self.data['users']
            for _, row in users_df.iterrows():
                user_id = str(row['user_id'])
                nickname = str(row.get('nickname', '')).strip().lower()
                
                unified_id = f"user_{len(user_mapping)+1:08d}"
                user_mapping[user_id] = unified_id
                
                # 保存用户信息
                user_info[unified_id] = {
                    'original_id': user_id,
                    'nickname': row.get('nickname', ''),
                    'gender': row.get('gender', 0),
                    'age': row.get('age', 0),
                    'province': row.get('province', ''),
                    'city': row.get('city', ''),
                    'listen_songs': row.get('listen_songs', 0),
                    'create_time': row.get('create_time', ''),
                    'source': 'internal'
                }
        
        # 2. 从外部数据中获取用户信息
        if 'external_history' in self.data and not self.data['external_history'].empty:
            external_df = self.data['external_history']
            external_users = external_df['user_id'].unique()[:100000]  # 限制数量
            
            for user_id in external_users:
                user_id_str = f"ext_{user_id}"
                
                if user_id_str not in user_mapping:
                    unified_id = f"user_{len(user_mapping)+1:08d}"
                    user_mapping[user_id_str] = unified_id
                    
                    user_info[unified_id] = {
                        'original_id': user_id_str,
                        'nickname': f'外部用户_{user_id}',
                        'gender': 0,
                        'age': 25,
                        'province': '',
                        'city': '',
                        'listen_songs': 0,
                        'create_time': '',
                        'source': 'external'
                    }
        
        self.user_id_mapping = user_mapping
        self.user_info = user_info
        
        print(f"创建了 {len(user_mapping)} 个统一用户ID")
        
        return self.user_id_mapping
    
    def build_interaction_matrix_with_mapping(self):
        """使用统一ID映射构建交互矩阵 - 优化版"""
        print("\n" + "="*80)
        print("构建统一的交互矩阵...")
        
        all_interactions = []
        
        # 1. 处理播放历史数据
        if 'plays' in self.data and not self.data['plays'].empty:
            print("1. 处理播放历史数据...")
            plays_df = self.data['plays'].copy()
            
            # 统一列名
            plays_df['user_id'] = plays_df['user_id'].astype(str)
            plays_df['song_id'] = plays_df['song_id'].astype(str)
            
            # 应用映射 - 分批处理
            plays_df['unified_user_id'] = plays_df['user_id'].map(self.user_id_mapping)
            
            # 优化：使用向量化操作替代apply
            # 创建缓存，避免重复查找
            song_id_cache = {}
            
            # 分批处理歌曲映射
            batch_size = 10000
            total_rows = len(plays_df)
            unified_song_ids = []
            
            for i in range(0, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                batch = plays_df.iloc[i:batch_end]
                
                batch_unified_ids = []
                for _, row in batch.iterrows():
                    song_id = row['song_id']
                    
                    # 使用缓存
                    if song_id in song_id_cache:
                        unified_id = song_id_cache[song_id]
                    else:
                        unified_id = self._get_unified_song_id(
                            row['song_id'], 
                            row.get('song_name', ''), 
                            row.get('artists', '')
                        )
                        song_id_cache[song_id] = unified_id
                    
                    batch_unified_ids.append(unified_id)
                
                unified_song_ids.extend(batch_unified_ids)
                
                # 显示进度
                if (i // batch_size) % 10 == 0:  # 每10批显示一次
                    progress = (batch_end / total_rows) * 100
                    print(f"    进度: {batch_end:,}/{total_rows:,} ({progress:.1f}%)")
            
            plays_df['unified_song_id'] = unified_song_ids
            
            # 过滤无效映射
            plays_df = plays_df[plays_df['unified_user_id'].notna() & plays_df['unified_song_id'].notna()]
            
            # 计算权重
            plays_df['play_count'] = plays_df['play_count'].fillna(0).astype(float)
            plays_df['score'] = plays_df['score'].fillna(0).astype(float)
            
            # 组合权重
            plays_df['weight'] = (
                0.7 * np.log1p(plays_df['play_count']) + 
                0.3 * plays_df['score'].clip(0, 10) / 10
            )
            
            plays_df['interaction_type'] = 'play'
            
            all_interactions.append(plays_df[['unified_user_id', 'unified_song_id', 'weight', 'interaction_type']])
            print(f"   播放历史: {len(plays_df):,} 条记录")
        
        # 2. 处理喜欢歌曲数据
        if 'likes' in self.data and not self.data['likes'].empty:
            print("2. 处理喜欢歌曲数据...")
            likes_df = self.data['likes'].copy()
            
            likes_df['user_id'] = likes_df['user_id'].astype(str)
            likes_df['song_id'] = likes_df['song_id'].astype(str)
            
            likes_df['unified_user_id'] = likes_df['user_id'].map(self.user_id_mapping)
            likes_df['unified_song_id'] = likes_df.apply(
                lambda x: self._get_unified_song_id(x['song_id'], x.get('song_name', ''), x.get('artists', '')),
                axis=1
            )
            
            likes_df = likes_df[likes_df['unified_user_id'].notna() & likes_df['unified_song_id'].notna()]
            
            # 喜欢行为权重较高
            likes_df['weight'] = 8.0
            likes_df['interaction_type'] = 'like'
            
            all_interactions.append(likes_df[['unified_user_id', 'unified_song_id', 'weight', 'interaction_type']])
            print(f"   喜欢歌曲: {len(likes_df):,} 条记录")
        
        # 3. 处理收藏关联数据
        if 'collected' in self.data and not self.data['collected'].empty:
            print("3. 处理收藏关联数据...")
            collected_df = self.data['collected'].copy()
            
            collected_df['user_id'] = collected_df['user_id'].astype(str)
            collected_df['song_id'] = collected_df['song_id'].astype(str)
            
            collected_df['unified_user_id'] = collected_df['user_id'].map(self.user_id_mapping)
            collected_df['unified_song_id'] = collected_df.apply(
                lambda x: self._get_unified_song_id(x['song_id'], '', ''),
                axis=1
            )
            
            collected_df = collected_df[collected_df['unified_user_id'].notna() & collected_df['unified_song_id'].notna()]
            
            collected_df['weight'] = 5.0
            collected_df['interaction_type'] = 'collect'
            
            all_interactions.append(collected_df[['unified_user_id', 'unified_song_id', 'weight', 'interaction_type']])
            print(f"   收藏关联: {len(collected_df):,} 条记录")
        
        # 4. 处理外部收听历史数据
        if 'external_history' in self.data and not self.data['external_history'].empty:
            print("4. 处理外部收听历史数据...")
            external_df = self.data['external_history'].copy()
            
            # 采样，避免数据量过大
            if len(external_df) > 500000:
                external_df = external_df.sample(n=500000, random_state=42)
            
            external_df['user_id'] = 'ext_' + external_df['user_id'].astype(str)
            external_df['song_id'] = external_df['track_id'].astype(str)
            
            external_df['unified_user_id'] = external_df['user_id'].map(self.user_id_mapping)
            
            # 对于外部数据，需要通过外部音乐信息来获取统一的歌曲ID
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
                    if track_id in track_to_info:
                        info = track_to_info[track_id]
                        return self._get_unified_song_id(track_id, info['name'], info['artist'])
                    return None
                
                external_df['unified_song_id'] = external_df['song_id'].apply(get_external_song_id)
            else:
                external_df['unified_song_id'] = None
            
            external_df = external_df[external_df['unified_user_id'].notna() & external_df['unified_song_id'].notna()]
            
            # 计算外部播放权重
            external_df['playcount'] = external_df['playcount'].fillna(0).astype(float)
            external_df['weight'] = np.log1p(external_df['playcount'])
            
            # 归一化权重到1-10范围
            max_weight = external_df['weight'].max()
            min_weight = external_df['weight'].min()
            if max_weight > min_weight:
                external_df['weight'] = 1 + 9 * (external_df['weight'] - min_weight) / (max_weight - min_weight)
            
            external_df['interaction_type'] = 'external_play'
            
            all_interactions.append(external_df[['unified_user_id', 'unified_song_id', 'weight', 'interaction_type']])
            print(f"   外部收听: {len(external_df):,} 条记录")
        
        # 5. 合并所有交互数据
        print("5. 合并所有交互数据...")
        if all_interactions:
            combined_interactions = pd.concat(all_interactions, ignore_index=True)
            
            # 聚合重复的交互
            interaction_matrix = combined_interactions.groupby(['unified_user_id', 'unified_song_id']).agg({
                'weight': 'sum',
                'interaction_type': lambda x: ','.join(sorted(set(x)))
            }).reset_index()
            
            interaction_matrix.columns = ['user_id', 'song_id', 'total_weight', 'interaction_types']
            
            print(f"\n交互矩阵统计:")
            print(f"  总交互数: {len(interaction_matrix):,}")
            print(f"  唯一用户数: {interaction_matrix['user_id'].nunique():,}")
            print(f"  唯一歌曲数: {interaction_matrix['song_id'].nunique():,}")
            
            # 计算稀疏度
            n_users = interaction_matrix['user_id'].nunique()
            n_songs = interaction_matrix['song_id'].nunique()
            sparsity = 1 - len(interaction_matrix) / (n_users * n_songs)
            print(f"  稀疏度: {sparsity:.6f}")
            
            return interaction_matrix
        else:
            print("警告: 没有找到任何交互数据!")
            return pd.DataFrame(columns=['user_id', 'song_id', 'total_weight', 'interaction_types'])
    
    def _get_unified_song_id(self, original_id, song_name, artists):
        """获取统一的歌曲ID - 增强版"""
        if not original_id:
            return None
        
        # 尝试直接通过原始ID查找
        for source in ['internal', 'external']:
            source_key = f"{source}_{original_id}"
            if source_key in self.song_id_mapping['key_to_unified']:
                return self.song_id_mapping['key_to_unified'][source_key]
        
        # 尝试通过歌曲名和艺术家名查找
        if song_name and artists:
            song_name_clean = str(song_name).strip().lower()
            artists_clean = str(artists).strip().lower()
            
            # 1. 完整匹配
            key = f"{song_name_clean}||{artists_clean}"
            if key in self.song_id_mapping['key_to_unified']:
                return self.song_id_mapping['key_to_unified'][key]
            
            # 2. 仅歌曲名匹配（模糊匹配）
            if song_name_clean:
                # 查找包含相同歌曲名的记录
                matching_keys = [k for k in self.song_id_mapping['key_to_unified'].keys() 
                            if song_name_clean in k and '||' in k]
                if matching_keys:
                    # 使用第一个匹配项
                    return self.song_id_mapping['key_to_unified'][matching_keys[0]]
        
        # 3. 使用外部数据集的spotify_id进行匹配
        if 'external_music' in self.data and not self.data['external_music'].empty:
            ext_df = self.data['external_music']
            if 'spotify_id' in ext_df.columns:
                match = ext_df[ext_df['spotify_id'] == original_id]
                if not match.empty:
                    # 通过外部音乐信息重新获取
                    name = match.iloc[0].get('name', '')
                    artist = match.iloc[0].get('artist', '')
                    if name and artist:
                        key = f"{str(name).strip().lower()}||{str(artist).strip().lower()}"
                        if key in self.song_id_mapping['key_to_unified']:
                            return self.song_id_mapping['key_to_unified'][key]
        
        return None
    
    def create_comprehensive_song_features(self):
        """创建综合的歌曲特征 - 修复版"""
        print("\n" + "="*80)
        print("创建综合的歌曲特征...")
        
        # 从song_info开始构建基础特征
        song_features_data = []
        
        for unified_id, info in self.song_info.items():
            feature_dict = {
                'song_id': unified_id,
                'song_name': info.get('song_name', ''),
                'artists': info.get('artists', ''),
                'album': info.get('album', ''),
                'duration_ms': info.get('duration', 0),
                'genre': info.get('genre', ''),
                'popularity': info.get('popularity', 0),
                'language': info.get('language', ''),
                'publish_year': self._extract_year(info.get('publish_date', ''))
            }
            song_features_data.append(feature_dict)
        
        song_features = pd.DataFrame(song_features_data)
        
        # 添加标签特征 - 修复映射问题
        if 'tags' in self.data and not self.data['tags'].empty:
            print("1. 添加标签特征...")
            tags_df = self.data['tags'].copy()
            
            # 应用映射
            tags_df['unified_song_id'] = tags_df['song_id'].apply(
                lambda x: self._get_unified_song_id(x, '', '')
            )
            
            print(f"    原始标签记录数: {len(tags_df)}")
            print(f"    成功映射的记录数: {tags_df['unified_song_id'].notna().sum()}")
            
            tags_df = tags_df[tags_df['unified_song_id'].notna()]
            
            if not tags_df.empty:
                # 确保score列是数值类型
                tags_df['score'] = pd.to_numeric(tags_df['score'], errors='coerce')
                
                tag_features = tags_df.groupby('unified_song_id').agg({
                    'score': ['mean', 'count']
                }).reset_index()
                
                # 修复：避免多层列名
                tag_features.columns = ['song_id', 'tag_score_mean', 'tag_count']
                
                song_features = pd.merge(song_features, tag_features, on='song_id', how='left')
            else:
                print("    警告: 标签数据映射后为空")
        
        # 添加评论特征
        if 'comments' in self.data and not self.data['comments'].empty:
            print("2. 添加评论特征...")
            comments_df = self.data['comments'].copy()
            
            comments_df['unified_song_id'] = comments_df['song_id'].apply(
                lambda x: self._get_unified_song_id(x, '', '')
            )
            
            comments_df = comments_df[comments_df['unified_song_id'].notna()]
            
            if not comments_df.empty:
                comment_features = comments_df.groupby('unified_song_id').agg({
                    'sentiment_score': ['mean', 'count'],
                    'liked_count': 'sum'
                }).reset_index()
                
                # 修复：避免多层列名
                comment_features.columns = ['song_id', 'avg_sentiment', 'comment_count', 'total_likes']
                song_features = pd.merge(song_features, comment_features, on='song_id', how='left')
        
        # 添加相似度特征
        if 'similarity' in self.data and not self.data['similarity'].empty:
            print("3. 添加相似度特征...")
            similarity_df = self.data['similarity'].copy()
            
            similarity_df['unified_song_id'] = similarity_df['song_id'].apply(
                lambda x: self._get_unified_song_id(x, '', '')
            )
            
            similarity_df = similarity_df[similarity_df['unified_song_id'].notna()]
            
            if not similarity_df.empty:
                similarity_features = similarity_df.groupby('unified_song_id').agg({
                    'similarity_score': ['mean', 'max', 'count']
                }).reset_index()
                
                # 修复：避免多层列名
                similarity_features.columns = ['song_id', 'avg_similarity', 'max_similarity', 'similar_songs_count']
                song_features = pd.merge(song_features, similarity_features, on='song_id', how='left')
        
        # 添加歌单特征
        if 'playlist_songs' in self.data and not self.data['playlist_songs'].empty:
            print("4. 添加歌单特征...")
            playlist_df = self.data['playlist_songs'].copy()
            
            playlist_df['unified_song_id'] = playlist_df.apply(
                lambda x: self._get_unified_song_id(x['song_id'], x.get('song_name', ''), x.get('artists', '')),
                axis=1
            )
            
            playlist_df = playlist_df[playlist_df['unified_song_id'].notna()]
            
            if not playlist_df.empty:
                playlist_features = playlist_df.groupby('unified_song_id').agg({
                    'playlist_id': 'nunique',
                    'order': 'mean'
                }).reset_index()
                
                playlist_features.columns = ['song_id', 'playlist_count', 'avg_playlist_order']
                song_features = pd.merge(song_features, playlist_features, on='song_id', how='left')
        
        # 添加外部音频特征
        print("5. 添加音频特征...")
        audio_features = self._extract_audio_features()
        if audio_features is not None:
            song_features = pd.merge(song_features, audio_features, on='song_id', how='left')
        
        # ==================== 新增：添加歌曲交互统计特征 ====================
        print("6. 添加歌曲交互统计特征...")
        if hasattr(self, 'interaction_matrix'):
            # 使用之前构建的交互矩阵（如果有）
            interaction_stats = self.interaction_matrix.groupby('song_id').agg({
                'total_weight': ['sum', 'mean', 'std'],
                'user_id': 'nunique'
            }).reset_index()
            
            # 修复：避免多层列名
            interaction_stats.columns = ['song_id', 'weight_sum', 'weight_mean', 'weight_std', 'unique_users']
            
            # 合并到歌曲特征
            song_features = pd.merge(song_features, interaction_stats, on='song_id', how='left')
            print(f"    已添加交互统计特征，覆盖 {interaction_stats['song_id'].nunique()} 首歌曲")
        else:
            print("    警告: 没有交互矩阵数据，跳过交互统计特征")
            # 创建空列以保持结构一致
            song_features['weight_sum'] = 0.0
            song_features['weight_mean'] = 0.0
            song_features['weight_std'] = 0.0
            song_features['unique_users'] = 0
        
        # 处理缺失值
        print("7. 处理缺失值...")
        song_features = self._handle_missing_values(song_features)
        
        # 特征工程
        print("8. 特征工程...")
        song_features = self._engineer_features(song_features)
        
        print(f"\n✓ 歌曲特征创建完成! 最终形状: {song_features.shape}")
        print(f"  关键列: {list(song_features.columns)[:15]}...")
        
        return song_features
    
    def _extract_audio_features(self):
        """提取音频特征 - 修复版"""
        if 'external_music' not in self.data or self.data['external_music'].empty:
            return None
        
        audio_features_data = []
        external_music = self.data['external_music']
        
        for _, row in external_music.iterrows():
            track_id = str(row['track_id'])
            unified_id = self._get_unified_song_id(track_id, row.get('name', ''), row.get('artist', ''))
            
            if unified_id:
                feature_dict = {
                    'song_id': unified_id,
                    # 修复：确保所有外部数据集字段名正确匹配
                    'danceability': row.get('danceability', row.get('danceability', 0.5)),
                    'energy': row.get('energy', row.get('energy', 0.5)),
                    'key': row.get('key', row.get('key', 0)),
                    'loudness': row.get('loudness', row.get('loudness', -10)),
                    'mode': row.get('mode', row.get('mode', 1)),
                    'speechiness': row.get('speechiness', row.get('speechiness', 0)),
                    'acousticness': row.get('acousticness', row.get('acousticness', 0)),
                    'instrumentalness': row.get('instrumentalness', row.get('instrumentalness', 0)),
                    'liveness': row.get('liveness', row.get('liveness', 0)),
                    'valence': row.get('valence', row.get('valence', 0.5)),
                    'tempo': row.get('tempo', row.get('tempo', 120)),
                    'time_signature': row.get('time_signature', row.get('time_signature', 4))
                }
                audio_features_data.append(feature_dict)
        
        if audio_features_data:
            audio_features = pd.DataFrame(audio_features_data)
            # 聚合重复项（如果有）
            audio_features = audio_features.groupby('song_id').mean().reset_index()
            return audio_features
        
        return None
    
    def _handle_missing_values(self, df):
        """处理缺失值"""
        # 数值特征用中位数填充
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().any():
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)
        
        # 分类特征用众数填充
        categorical_cols = ['genre', 'language']
        for col in categorical_cols:
            if col in df.columns and df[col].isnull().any():
                mode_val = df[col].mode()[0] if not df[col].mode().empty else '未知'
                df[col].fillna(mode_val, inplace=True)
        
        return df
    
    def _engineer_features(self, df):
        """特征工程 - 修复版"""
        # 创建歌曲年龄特征
        if 'publish_year' in df.columns:
            current_year = datetime.now().year
            # 确保年份是数值类型
            df['publish_year'] = pd.to_numeric(df['publish_year'], errors='coerce')
            df['publish_year'] = df['publish_year'].fillna(current_year - 5)
            df['song_age'] = current_year - df['publish_year']
            df['song_age'] = df['song_age'].clip(lower=0)
        
        # 创建时长特征（分钟）
        if 'duration_ms' in df.columns:
            df['duration_minutes'] = pd.to_numeric(df['duration_ms'], errors='coerce') / 60000
            df['duration_minutes'] = df['duration_minutes'].fillna(3.5)  # 默认3.5分钟
        
        # 流行度分组 - 确保所有歌曲都有分组
        if 'popularity' in df.columns:
            df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
            df['popularity'] = df['popularity'].fillna(df['popularity'].median())
            
            try:
                # 使用等距分箱，避免qcut在数据分布不均时出错
                df['popularity_group'] = pd.cut(
                    df['popularity'],
                    bins=5,
                    labels=['很低', '低', '中', '高', '很高'],
                    include_lowest=True
                )
            except Exception as e:
                # 如果分箱失败，使用默认值
                df['popularity_group'] = '中'
        
        # 组合音频特征 - 确保字段存在
        audio_cols = ['danceability', 'energy', 'valence', 'tempo']
        for col in audio_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0.5 if col != 'tempo' else 120)
        
        # 只在所有必需字段都存在时才创建组合特征
        required_audio = ['danceability', 'energy', 'valence']
        if all(col in df.columns for col in required_audio):
            df['energy_dance'] = (df['danceability'] + df['energy']) / 2
            df['mood_score'] = (df['valence'] + df['energy']) / 2
        
        return df
    
    def _extract_year(self, date_str):
        """从日期字符串中提取年份"""
        if pd.isna(date_str):
            return 2020
        
        try:
            # 尝试解析日期字符串
            if isinstance(date_str, str):
                # 移除时间部分
                date_part = date_str.split(' ')[0]
                # 尝试不同的日期格式
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d', '%Y']:
                    try:
                        dt = datetime.strptime(date_part, fmt)
                        return dt.year
                    except:
                        continue
            
            # 如果是数字，直接作为年份
            if isinstance(date_str, (int, float)):
                return int(date_str)
        except:
            pass
        
        return 2020
    
    def create_comprehensive_user_features(self, interaction_matrix):
        """创建综合的用户特征 - 修复版"""
        print("\n" + "="*80)
        print("创建综合的用户特征...")
        
        user_features_data = []
        
        # 从用户信息开始
        for unified_id, info in self.user_info.items():
            feature_dict = {
                'user_id': unified_id,
                'nickname': info.get('nickname', ''),
                'gender': info.get('gender', 0),
                'age': info.get('age', 25),
                'province': info.get('province', ''),
                'city': info.get('city', ''),
                'listen_songs': info.get('listen_songs', 0),
                'source': info.get('source', 'internal')
            }
            user_features_data.append(feature_dict)
        
        user_features = pd.DataFrame(user_features_data)
        
        # ==================== 修复：避免重复列名 ====================
        print("1. 从交互矩阵提取用户行为特征...")
        
        if not interaction_matrix.empty:
            # 直接从传入的 interaction_matrix 计算统计
            user_stats = interaction_matrix.groupby('user_id').agg({
                'total_weight': ['sum', 'mean'],
                'song_id': 'nunique'
            }).reset_index()
            
            # 修复：避免多层列名
            user_stats.columns = ['user_id', 'total_weight_sum', 'avg_weight', 'unique_songs']
            
            # 计算总交互次数（包括重复）
            user_counts = interaction_matrix.groupby('user_id').size().reset_index(name='total_interactions')
            user_stats = user_stats.merge(user_counts, on='user_id', how='left')
            
            # 计算权重标准差
            weight_std = interaction_matrix.groupby('user_id')['total_weight'].std().reset_index(name='weight_std')
            user_stats = user_stats.merge(weight_std, on='user_id', how='left')
            
            # 修复合并：明确指定后缀，避免_x, _y
            user_features = pd.merge(
                user_features, 
                user_stats, 
                on='user_id', 
                how='left',
                suffixes=('', '_stats')  # 明确指定后缀
            )
            
            print(f"    统计特征: {len(user_stats):,} 个用户")
        else:
            print("    警告: 交互矩阵为空，跳过统计特征")
            # 创建空列以保持结构一致
            user_features['total_weight_sum'] = 0.0
            user_features['avg_weight'] = 0.0
            user_features['unique_songs'] = 0
            user_features['total_interactions'] = 0
            user_features['weight_std'] = 0.0
        
        # 处理缺失值
        print("2. 处理缺失值...")
        
        # 填充缺失的交互统计
        for col in ['unique_songs', 'total_interactions', 'total_weight_sum', 'avg_weight']:
            if col in user_features.columns:
                user_features[col] = user_features[col].fillna(0)
            else:
                user_features[col] = 0
        
        # 年龄
        if 'age' in user_features.columns:
            age_median = user_features['age'].median()
            user_features['age'].fillna(age_median, inplace=True)
        
        # 性别
        if 'gender' in user_features.columns:
            user_features['gender'].fillna(0, inplace=True)
        
        # 特征工程
        print("3. 特征工程...")
        
        # 年龄分组
        if 'age' in user_features.columns:
            bins = [0, 18, 25, 35, 50, 100]
            labels = ['<18', '18-25', '26-35', '36-50', '>50']
            user_features['age_group'] = pd.cut(
                user_features['age'], bins=bins, labels=labels, right=False
            )
        
        # 活跃度分级 - 使用 total_weight_sum
        if 'total_weight_sum' in user_features.columns:
            try:
                user_features['activity_level'] = pd.qcut(
                    user_features['total_weight_sum'], 
                    q=4, 
                    labels=['低活跃', '中低活跃', '中高活跃', '高活跃'],
                    duplicates='drop'
                )
            except:
                user_features['activity_level'] = pd.cut(
                    user_features['total_weight_sum'],
                    bins=4,
                    labels=['低活跃', '中低活跃', '中高活跃', '高活跃']
                )
        
        # 交互多样性
        if 'unique_songs' in user_features.columns and 'total_interactions' in user_features.columns:
            # 避免除零
            user_features['diversity_ratio'] = user_features['unique_songs'] / user_features['total_interactions'].replace(0, 1)
            
            print(f"    多样性统计: 均值={user_features['diversity_ratio'].mean():.3f}, "
                f"范围=[{user_features['diversity_ratio'].min():.3f}, {user_features['diversity_ratio'].max():.3f}]")
        
        # ==================== 新增：用户偏好特征 ====================
        print("4. 计算用户偏好特征...")
        
        # 流行度偏好
        if not interaction_matrix.empty and hasattr(self, 'song_features_intermediate'):
            # 需要先有歌曲特征来计算流行度偏好
            temp_song_features = getattr(self, 'song_features_intermediate', None)
            
            if temp_song_features is not None and 'final_popularity' in temp_song_features.columns:
                # 合并歌曲流行度
                merged = interaction_matrix.merge(
                    temp_song_features[['song_id', 'final_popularity']], 
                    on='song_id', 
                    how='left'
                )
                
                # 计算用户平均流行度偏好
                avg_pop = merged.groupby('user_id')['final_popularity'].mean().reset_index(name='avg_popularity_pref')
                user_features = pd.merge(user_features, avg_pop, on='user_id', how='left')
                
                # 计算流行度偏差（相对于整体平均50）
                user_features['popularity_bias'] = user_features['avg_popularity_pref'] - 50.0
                
                print(f"    流行度偏好: 均值={user_features['avg_popularity_pref'].mean():.1f}")
            else:
                # 设置默认值
                user_features['avg_popularity_pref'] = 50.0
                user_features['popularity_bias'] = 0.0
        else:
            # 设置默认值
            user_features['avg_popularity_pref'] = 50.0
            user_features['popularity_bias'] = 0.0
        
        # 流派偏好 - 需要歌曲流派信息
        if not interaction_matrix.empty and hasattr(self, 'song_features_intermediate'):
            temp_song_features = getattr(self, 'song_features_intermediate', None)
            
            if temp_song_features is not None and 'genre_clean' in temp_song_features.columns:
                # 合并歌曲流派
                merged = interaction_matrix.merge(
                    temp_song_features[['song_id', 'genre_clean']], 
                    on='song_id', 
                    how='left'
                )
                
                # 计算用户流派偏好
                genre_counts = merged.groupby(['user_id', 'genre_clean']).size().reset_index(name='count')
                genre_counts = genre_counts.sort_values(['user_id', 'count'], ascending=[True, False])
                
                # 为每个用户获取前3个流派
                for i in range(3):
                    top_genre = genre_counts[genre_counts.groupby('user_id').cumcount() == i]
                    col_name = f'top_genre_{i+1}'
                    user_features[col_name] = user_features['user_id'].map(
                        top_genre.set_index('user_id')['genre_clean']
                    )
                
                print(f"    流派偏好: {user_features['top_genre_1'].notna().sum():,} 用户有偏好记录")
            else:
                # 创建空列
                for i in range(1, 4):
                    user_features[f'top_genre_{i}'] = None
        else:
            # 创建空列
            for i in range(1, 4):
                user_features[f'top_genre_{i}'] = None
        
        print(f"\n✓ 用户特征创建完成! 最终形状: {user_features.shape}")
        print(f"  列名: {list(user_features.columns)}")
        
        return user_features
    
    def filter_sparse_data(self, interaction_matrix, min_user_interactions=5, min_song_interactions=5):
        """过滤稀疏数据"""
        print("\n" + "="*80)
        print(f"过滤稀疏数据 (用户≥{min_user_interactions}次, 歌曲≥{min_song_interactions}次)...")
        
        original_stats = {
            'users': interaction_matrix['user_id'].nunique(),
            'songs': interaction_matrix['song_id'].nunique(),
            'interactions': len(interaction_matrix)
        }
        
        # 过滤低频用户
        user_counts = interaction_matrix.groupby('user_id').size()
        active_users = user_counts[user_counts >= min_user_interactions].index
        
        # 过滤低频歌曲
        song_counts = interaction_matrix.groupby('song_id').size()
        active_songs = song_counts[song_counts >= min_song_interactions].index
        
        filtered_matrix = interaction_matrix[
            interaction_matrix['user_id'].isin(active_users) & 
            interaction_matrix['song_id'].isin(active_songs)
        ].copy()
        
        filtered_stats = {
            'users': filtered_matrix['user_id'].nunique(),
            'songs': filtered_matrix['song_id'].nunique(),
            'interactions': len(filtered_matrix)
        }
        
        print(f"过滤前: {original_stats['users']:,} 用户, {original_stats['songs']:,} 歌曲, {original_stats['interactions']:,} 交互")
        print(f"过滤后: {filtered_stats['users']:,} 用户, {filtered_stats['songs']:,} 歌曲, {filtered_stats['interactions']:,} 交互")
        
        # 计算保留比例
        user_ratio = filtered_stats['users'] / original_stats['users'] if original_stats['users'] > 0 else 0
        song_ratio = filtered_stats['songs'] / original_stats['songs'] if original_stats['songs'] > 0 else 0
        interaction_ratio = filtered_stats['interactions'] / original_stats['interactions'] if original_stats['interactions'] > 0 else 0
        
        print(f"保留比例: 用户 {user_ratio:.2%}, 歌曲 {song_ratio:.2%}, 交互 {interaction_ratio:.2%}")
        
        return filtered_matrix
    
    def split_train_test(self, interaction_matrix, test_size=0.2, random_state=42):
        """划分训练集和测试集"""
        print("\n" + "="*80)
        print("划分训练集和测试集...")
        
        train_data = []
        test_data = []
        
        np.random.seed(random_state)
        
        for user_id in interaction_matrix['user_id'].unique():
            user_interactions = interaction_matrix[interaction_matrix['user_id'] == user_id]
            
            if len(user_interactions) >= 10:
                # 有足够数据，随机划分
                n_test = max(1, int(len(user_interactions) * test_size))
                test_indices = np.random.choice(user_interactions.index, size=n_test, replace=False)
                train_indices = user_interactions.index.difference(test_indices)
                
                train_data.append(user_interactions.loc[train_indices])
                test_data.append(user_interactions.loc[test_indices])
            elif len(user_interactions) >= 3:
                # 数据较少，留一条作为测试
                test_indices = np.random.choice(user_interactions.index, size=1, replace=False)
                train_indices = user_interactions.index.difference(test_indices)
                
                train_data.append(user_interactions.loc[train_indices])
                test_data.append(user_interactions.loc[test_indices])
            else:
                # 数据太少，全部作为训练
                train_data.append(user_interactions)
        
        if train_data:
            train_interactions = pd.concat(train_data, ignore_index=True)
        else:
            train_interactions = pd.DataFrame(columns=interaction_matrix.columns)
        
        if test_data:
            test_interactions = pd.concat(test_data, ignore_index=True)
        else:
            test_interactions = pd.DataFrame(columns=interaction_matrix.columns)
        
        print(f"训练集: {len(train_interactions):,} 条记录")
        print(f"测试集: {len(test_interactions):,} 条记录")
        print(f"划分比例: {len(test_interactions)/(len(train_interactions)+len(test_interactions)):.2%}")
        
        return train_interactions, test_interactions
    
    def save_processed_data(self, song_features, user_features, 
                          interaction_matrix, train_interactions, test_interactions):
        """保存处理后的数据"""
        print("\n" + "="*80)
        print("保存处理后的数据...")
        
        output_dir = "processed_data_complete"
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存歌曲特征
        song_features.to_csv(
            os.path.join(output_dir, "song_features_complete.csv"), index=False, encoding='utf-8'
        )
        print(f"✓ 歌曲特征已保存 ({len(song_features):,} 条记录)")
        
        # 保存用户特征
        user_features.to_csv(
            os.path.join(output_dir, "user_features_complete.csv"), index=False, encoding='utf-8'
        )
        print(f"✓ 用户特征已保存 ({len(user_features):,} 条记录)")
        
        # 保存交互矩阵
        interaction_matrix.to_csv(
            os.path.join(output_dir, "interaction_matrix_complete.csv"), index=False, encoding='utf-8'
        )
        print(f"✓ 交互矩阵已保存 ({len(interaction_matrix):,} 条记录)")
        
        # 保存训练集和测试集
        train_interactions.to_csv(
            os.path.join(output_dir, "train_interactions_complete.csv"), index=False, encoding='utf-8'
        )
        test_interactions.to_csv(
            os.path.join(output_dir, "test_interactions_complete.csv"), index=False, encoding='utf-8'
        )
        print(f"✓ 训练集已保存 ({len(train_interactions):,} 条记录)")
        print(f"✓ 测试集已保存 ({len(test_interactions):,} 条记录)")
        
        # 保存数据统计信息
        stats = {
            'n_songs': len(song_features),
            'n_users': len(user_features),
            'n_interactions': len(interaction_matrix),
            'n_train': len(train_interactions),
            'n_test': len(test_interactions),
            'train_test_ratio': len(test_interactions) / (len(train_interactions) + len(test_interactions)) 
                                if (len(train_interactions) + len(test_interactions)) > 0 else 0
        }
        
        with open(os.path.join(output_dir, "data_stats_complete.json"), 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        print(f"✓ 数据统计信息已保存")
        
        print("\n所有数据已保存完成!")
        print(f"输出目录: {output_dir}")
        
        return stats

# 在 main() 函数中修改，确保在正确时机计算用户特征
def main():
    """主函数：执行完整的数据处理流程"""
    print("="*80)
    print("统一音乐数据预处理系统")
    print("="*80)
    
    try:
        # 1. 初始化处理器
        processor = UnifiedMusicDataProcessor(data_path=".")
        
        # 2. 加载所有数据
        print("\n阶段1: 加载原始数据")
        processor.load_all_data_systematic()
        
        # 3. 创建统一的ID映射
        print("\n阶段2: 创建统一的ID映射")
        processor.create_unified_song_mapping()
        processor.create_unified_user_mapping()
        
        # 4. 构建交互矩阵
        print("\n阶段3: 构建交互矩阵")
        interaction_matrix = processor.build_interaction_matrix_with_mapping()
        
        # 5. 过滤稀疏数据
        print("\n阶段4: 过滤稀疏数据")
        filtered_matrix = processor.filter_sparse_data(
            interaction_matrix, 
            min_user_interactions=5, 
            min_song_interactions=5
        )
        
        # 6. 创建歌曲特征
        print("\n阶段5: 创建歌曲特征")
        song_features = processor.create_comprehensive_song_features()
        
        # 保存中间歌曲特征（用于用户特征计算）
        processor.song_features_intermediate = song_features.copy()
        
        # 7. 创建用户特征 - 传入过滤后的交互矩阵
        print("\n阶段6: 创建用户特征")
        user_features = processor.create_comprehensive_user_features(filtered_matrix)
        
        # 8. 过滤特征数据，只保留交互矩阵中的用户和歌曲
        print("\n阶段7: 对齐特征数据")
        
        # 过滤歌曲特征，只保留在交互矩阵中的歌曲
        songs_in_interaction = set(filtered_matrix['song_id'].unique())
        song_features = song_features[song_features['song_id'].isin(songs_in_interaction)].copy()
        
        # 过滤用户特征，只保留在交互矩阵中的用户
        users_in_interaction = set(filtered_matrix['user_id'].unique())
        user_features = user_features[user_features['user_id'].isin(users_in_interaction)].copy()
        
        # 过滤交互矩阵，只保留在特征数据中的用户和歌曲
        filtered_matrix = filtered_matrix[
            filtered_matrix['user_id'].isin(user_features['user_id']) & 
            filtered_matrix['song_id'].isin(song_features['song_id'])
        ].copy()
        
        print(f"对齐后: {song_features.shape[0]:,} 歌曲, {user_features.shape[0]:,} 用户, {len(filtered_matrix):,} 交互")
        
        # 9. 划分训练集和测试集
        print("\n阶段8: 划分训练集和测试集")
        train_interactions, test_interactions = processor.split_train_test(filtered_matrix)
        
        # 10. 保存处理后的数据
        print("\n阶段9: 保存数据")
        stats = processor.save_processed_data(
            song_features, user_features, filtered_matrix, 
            train_interactions, test_interactions
        )
        
        # 11. 显示最终统计信息
        print("\n" + "="*80)
        print("数据处理完成!")
        print("="*80)
        
        print("\n📊 最终统计信息:")
        print(f"1. 歌曲数: {stats['n_songs']:,}")
        print(f"2. 用户数: {stats['n_users']:,}")
        print(f"3. 总交互数: {stats['n_interactions']:,}")
        print(f"4. 训练集大小: {stats['n_train']:,}")
        print(f"5. 测试集大小: {stats['n_test']:,}")
        print(f"6. 测试集比例: {stats['train_test_ratio']:.2%}")
        
        # 计算数据密度
        if stats['n_users'] > 0 and stats['n_songs'] > 0:
            density = stats['n_interactions'] / (stats['n_users'] * stats['n_songs'])
            print(f"7. 数据密度: {density:.6f}")
        
        print("\n✅ 数据处理流程全部完成!")
        print("您可以使用处理后的数据运行推荐系统了。")
        
        return processor
        
    except Exception as e:
        print(f"\n❌ 主程序执行时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # 运行完整的数据处理流程
    processor = main()