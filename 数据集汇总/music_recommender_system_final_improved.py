# file: music_recommender_system_optimized.py

import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix, save_npz, load_npz
from sklearn.preprocessing import MinMaxScaler, quantile_transform
from sklearn.neighbors import NearestNeighbors
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans
import warnings
warnings.filterwarnings('ignore')
import os
import json
from datetime import datetime
import pickle
import time
import random
from collections import Counter

class DataAlignmentAndEnhancement:
    """数据对齐和特征增强 - 修复版"""
    
    def __init__(self, data_dir=None):
        # 自动检测数据目录
        if data_dir is None:
            possible_dirs = ["processed_data", "processed_data_complete", ".", "data"]
            self.data_dir = self._find_data_dir(possible_dirs)
        else:
            self.data_dir = data_dir
            
        self.aligned_data_dir = "aligned_data_optimized"
        os.makedirs(self.aligned_data_dir, exist_ok=True)
        
        print(f"原始数据目录: {self.data_dir}")
        print(f"对齐数据目录: {self.aligned_data_dir}")
        
    def _find_data_dir(self, possible_dirs):
        """自动寻找包含必要文件的目录"""
        for d in possible_dirs:
            if os.path.exists(d):
                required_files = ["song_features.csv", "interaction_matrix.csv", "user_features.csv"]
                found = all(os.path.exists(os.path.join(d, f)) for f in required_files)
                if found:
                    print(f"✓ 找到数据目录: {d}")
                    return d
        raise FileNotFoundError("找不到包含必要数据文件的目录，请确保存在 processed_data 或 processed_data_complete 文件夹")
        
    def load_original_data(self):
        """加载原始数据"""
        print("="*80)
        print("1. 加载原始数据...")
        
        files = {
            "song_features": "song_features.csv",
            "interaction_matrix": "interaction_matrix.csv", 
            "user_features": "user_features.csv"
        }
        
        for attr, filename in files.items():
            filepath = os.path.join(self.data_dir, filename)
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"找不到文件: {filepath}")
            setattr(self, attr, pd.read_csv(filepath))
            df = getattr(self, attr)
            print(f"  ✓ {filename}: {df.shape}")
        
        # 数据清洗
        self.interaction_matrix['user_id'] = self.interaction_matrix['user_id'].astype(str)
        self.user_features['user_id'] = self.user_features['user_id'].astype(str)
        
        # 过滤外部用户
        mask = ~self.interaction_matrix['user_id'].str.contains('ext_', na=False, case=False)
        self.internal_interactions = self.interaction_matrix[mask].copy()
        print(f"  ✓ 内部交互: {self.internal_interactions.shape}")
        
    def align_and_filter_data_balanced(self, min_user_interactions=3, min_song_interactions=2):
        """数据对齐（放宽条件保留更多数据）"""
        print("\n" + "="*80)
        print("2. 数据对齐与过滤...")
        
        # 统计
        user_counts = self.internal_interactions.groupby('user_id').size()
        valid_users = user_counts[user_counts >= min_user_interactions].index.tolist()
        print(f"  活跃用户(≥{min_user_interactions}次): {len(valid_users)}")
        
        song_counts = self.internal_interactions.groupby('song_id').size()
        valid_songs = song_counts[song_counts >= min_song_interactions].index.tolist()
        print(f"  有效歌曲(≥{min_song_interactions}次): {len(valid_songs)}")
        
        if len(valid_users) == 0 or len(valid_songs) == 0:
            raise ValueError("过滤后没有有效数据，请检查原始数据")
        
        # 应用过滤
        self.filtered_interactions = self.internal_interactions[
            self.internal_interactions['user_id'].isin(valid_users) & 
            self.internal_interactions['song_id'].isin(valid_songs)
        ].copy()
        
        # 对齐特征表
        self.filtered_song_features = self.song_features[
            self.song_features['song_id'].isin(valid_songs)
        ].copy()
        
        self.filtered_user_features = self.user_features[
            self.user_features['user_id'].isin(valid_users)
        ].copy()
        
        # 确保ID一致
        final_users = set(self.filtered_interactions['user_id'].unique()) & set(self.filtered_user_features['user_id'].unique())
        final_songs = set(self.filtered_interactions['song_id'].unique()) & set(self.filtered_song_features['song_id'].unique())
        
        self.filtered_interactions = self.filtered_interactions[
            self.filtered_interactions['user_id'].isin(final_users) & 
            self.filtered_interactions['song_id'].isin(final_songs)
        ]
        self.filtered_song_features = self.filtered_song_features[self.filtered_song_features['song_id'].isin(final_songs)]
        self.filtered_user_features = self.filtered_user_features[self.filtered_user_features['user_id'].isin(final_users)]
        
        self.unique_users = len(final_users)
        self.unique_songs = len(final_songs)
        self.total_interactions = len(self.filtered_interactions)
        self.density = self.total_interactions / (self.unique_users * self.unique_songs) * 100 if self.unique_users * self.unique_songs > 0 else 0
            
        print(f"\n  最终统计:")
        print(f"    用户数: {self.unique_users:,}")
        print(f"    歌曲数: {self.unique_songs:,}")
        print(f"    交互数: {self.total_interactions:,}")
        print(f"    密度: {self.density:.4f}%")
        
    def enhance_features_advanced(self):
        """特征增强"""
        print("\n" + "="*80)
        print("3. 特征增强...")
        
        # 1. 歌曲特征
        print("  处理歌曲特征...")
        song_features = self.filtered_song_features.copy()
        
        # 交互统计
        if len(self.filtered_interactions) > 0:
            song_stats = self.filtered_interactions.groupby('song_id').agg({
                'total_weight': ['sum', 'mean', 'count'],
                'user_id': 'nunique'
            }).reset_index()
            song_stats.columns = ['song_id', 'weight_sum', 'weight_mean', 'weight_std', 'unique_users']
            song_features = song_features.merge(song_stats, on='song_id', how='left')
        
        # 填充缺失
        for col in ['weight_sum', 'weight_mean', 'weight_std', 'unique_users']:
            if col not in song_features.columns:
                song_features[col] = 0
            else:
                song_features[col] = song_features[col].fillna(0)
        
        # 核心修复：流行度计算
        print("    计算流行度...")
        if 'popularity' in song_features.columns:
            raw_pop = pd.to_numeric(song_features['popularity'], errors='coerce').fillna(0)
            if raw_pop.max() <= 1.0:
                raw_pop = raw_pop * 100
        else:
            raw_pop = pd.Series([0] * len(song_features))
        
        # 交互热度
        interaction_score = np.log1p(song_features.get('weight_sum', 0))
        
        # 组合
        combined_score = raw_pop.fillna(0) * 0.5 + interaction_score * 10 * 0.5
        
        # 归一化
        if len(combined_score.unique()) > 1:
            try:
                pop_norm = quantile_transform(
                    combined_score.values.reshape(-1, 1),
                    n_quantiles=min(100, len(combined_score)),
                    random_state=42
                ).flatten()
                song_features['final_popularity'] = 10 + pop_norm * 90
            except:
                scaler = MinMaxScaler(feature_range=(10, 100))
                song_features['final_popularity'] = scaler.fit_transform(combined_score.values.reshape(-1, 1)).flatten()
        else:
            song_features['final_popularity'] = 50.0
        
        song_features['final_popularity'] = song_features['final_popularity'].clip(10, 100)
        song_features['final_popularity_norm'] = song_features['final_popularity'] / 100.0
        
        print(f"    流行度范围: [{song_features['final_popularity'].min():.1f}, {song_features['final_popularity'].max():.1f}], 均值: {song_features['final_popularity'].mean():.2f}")
        
        # 音频特征
        audio_cols = ['danceability', 'energy', 'valence', 'tempo', 'loudness', 'speechiness', 
                     'acousticness', 'instrumentalness', 'liveness']
        for col in audio_cols:
            if col in song_features.columns:
                song_features[col] = pd.to_numeric(song_features[col], errors='coerce').fillna(0.5)
            else:
                song_features[col] = 0.5
        
        # 组合特征
        song_features['energy_dance'] = (song_features['danceability'] + song_features['energy']) / 2
        song_features['mood_score'] = (song_features['valence'] * 0.6 + song_features['energy'] * 0.4)
        
        # 时间
        current_year = datetime.now().year
        song_features['publish_year'] = pd.to_numeric(song_features.get('publish_year', current_year-5), errors='coerce').fillna(current_year-5)
        song_features['song_age'] = (current_year - song_features['publish_year']).clip(0, 100)
        song_features['recency_score'] = np.exp(-song_features['song_age'] / 15)
        
        # 分层
        song_features['popularity_tier'] = pd.cut(
            song_features['final_popularity'],
            bins=[0, 30, 60, 100],
            labels=['normal', 'popular', 'hit']
        )
        
        # 流派
        if 'genre' in song_features.columns:
            song_features['genre'] = song_features['genre'].fillna('unknown')
            top_genres = song_features['genre'].value_counts().head(12).index.tolist()
            song_features['genre_clean'] = song_features['genre'].apply(lambda x: x if x in top_genres else 'other')
        else:
            song_features['genre'] = 'unknown'
            song_features['genre_clean'] = 'other'
        
        self.enhanced_song_features = song_features
        
        # 2. 用户特征
        print("  处理用户特征...")
        user_features = self.filtered_user_features.copy()
        
        if len(self.filtered_interactions) > 0:
            user_stats = self.filtered_interactions.groupby('user_id').agg({
                'total_weight': ['sum', 'mean'],
                'song_id': 'nunique'
            }).reset_index()
            user_stats.columns = ['user_id', 'total_weight_sum', 'total_weight_mean', 'unique_songs']
            
            user_counts = self.filtered_interactions.groupby('user_id').size().reset_index(name='total_interactions')
            user_stats = user_stats.merge(user_counts, on='user_id', how='left')
            
            user_features = user_features.merge(user_stats, on='user_id', how='left')
        
        for col in ['total_interactions', 'unique_songs', 'total_weight_sum']:
            if col not in user_features.columns:
                user_features[col] = 0
            else:
                user_features[col] = user_features[col].fillna(0)
        
        # 年龄
        if 'age' in user_features.columns:
            user_features['age'] = pd.to_numeric(user_features['age'], errors='coerce').fillna(25)
            bins = [0, 22, 30, 40, 100]
            labels = ['young', 'adult', 'middle', 'senior']
            user_features['age_group'] = pd.cut(user_features['age'], bins=bins, labels=labels, right=False)
        
        # 流派偏好
        if len(self.filtered_interactions) > 0 and 'genre_clean' in self.enhanced_song_features.columns:
            user_genre = self.filtered_interactions.merge(
                self.enhanced_song_features[['song_id', 'genre_clean']], on='song_id', how='left'
            )
            if not user_genre.empty:
                genre_counts = user_genre.groupby(['user_id', 'genre_clean']).size().reset_index(name='count')
                genre_counts = genre_counts.sort_values(['user_id', 'count'], ascending=[True, False])
                
                for i in range(3):
                    top = genre_counts[genre_counts.groupby('user_id').cumcount() == i]
                    user_features[f'top_genre_{i+1}'] = user_features['user_id'].map(top.set_index('user_id')['genre_clean'].to_dict())
        
        # 流行度偏好
        if len(self.filtered_interactions) > 0:
            pop_pref = self.filtered_interactions.merge(
                self.enhanced_song_features[['song_id', 'final_popularity']], on='song_id', how='left'
            )
            avg_pop = pop_pref.groupby('user_id')['final_popularity'].mean()
            user_features['avg_popularity_pref'] = user_features['user_id'].map(avg_pop).fillna(50.0)
            user_features['popularity_bias'] = 50 - user_features['avg_popularity_pref']
        
        self.enhanced_user_features = user_features
        
        print(f"  歌曲特征: {self.enhanced_song_features.shape}")
        print(f"  用户特征: {self.enhanced_user_features.shape}")
        
    def create_balanced_train_test_split(self, test_size=0.2, random_state=42):
        """划分训练测试集"""
        print("\n4. 划分训练/测试集...")
        train_data, test_data = [], []
        
        np.random.seed(random_state)
        
        for user_id in self.filtered_interactions['user_id'].unique():
            user_df = self.filtered_interactions[self.filtered_interactions['user_id'] == user_id]
            user_df = user_df.sort_values('total_weight', ascending=False)
            
            n = len(user_df)
            if n >= 8:
                n_test = max(1, int(n * test_size))
                test_data.append(user_df.iloc[:n_test])
                train_data.append(user_df.iloc[n_test:])
            elif n >= 3:
                test_data.append(user_df.iloc[:1])
                train_data.append(user_df.iloc[1:])
            else:
                train_data.append(user_df)
        
        self.train_interactions = pd.concat(train_data, ignore_index=True) if train_data else pd.DataFrame()
        self.test_interactions = pd.concat(test_data, ignore_index=True) if test_data else pd.DataFrame()
        
        print(f"  训练集: {len(self.train_interactions):,}")
        print(f"  测试集: {len(self.test_interactions):,}")
        
    def save_aligned_data(self):
        """保存数据"""
        print("\n5. 保存对齐数据...")
        
        files_to_save = [
            (self.filtered_interactions, "filtered_interactions.csv"),
            (self.enhanced_song_features, "enhanced_song_features.csv"),
            (self.enhanced_user_features, "enhanced_user_features.csv"),
            (self.train_interactions, "train_interactions.csv"),
            (self.test_interactions, "test_interactions.csv")
        ]
        
        for df, filename in files_to_save:
            if len(df) > 0:
                filepath = os.path.join(self.aligned_data_dir, filename)
                df.to_csv(filepath, index=False)
                print(f"  ✓ {filename}: {len(df)}条")
        
        # 保存统计
        stats = {
            'n_songs': len(self.enhanced_song_features),
            'n_users': len(self.enhanced_user_features),
            'n_interactions': len(self.filtered_interactions),
            'n_train': len(self.train_interactions),
            'n_test': len(self.test_interactions),
            'density': float(self.density),
            'popularity_mean': float(self.enhanced_song_features['final_popularity'].mean()),
            'created_at': datetime.now().isoformat()
        }
        
        with open(os.path.join(self.aligned_data_dir, "data_stats.json"), 'w') as f:
            json.dump(stats, f, indent=2)
        
        print(f"\n✓ 数据已保存到: {self.aligned_data_dir}")
        
    def run_balanced(self, create_test_split=True):
        """运行完整流程"""
        self.load_original_data()
        self.align_and_filter_data_balanced()
        self.enhance_features_advanced()
        if create_test_split:
            self.create_balanced_train_test_split()
        self.save_aligned_data()
        return self.filtered_interactions, self.enhanced_song_features, self.enhanced_user_features


class OptimizedMusicRecommender:
    """优化版推荐系统"""
    
    def __init__(self, data_dir="aligned_data_optimized"):
        self.data_dir = data_dir
        self.cache_dir = "recommender_cache_optimized"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.load_data()
        self.build_matrices()
        self.calculate_similarities_optimized()
        
    def validate_data(self):
        """验证数据完整性"""
        print("  验证数据完整性...")
        
        song_file = os.path.join(self.data_dir, "enhanced_song_features.csv")
        if not os.path.exists(song_file):
            raise FileNotFoundError(f"找不到文件: {song_file}")
        
        df = pd.read_csv(song_file, nrows=5)  # 只读前几行检查列名
        
        required_cols = ['song_id', 'final_popularity', 'genre_clean']
        missing = [col for col in required_cols if col not in df.columns]
        
        if missing:
            raise ValueError(f"数据文件缺少必要列: {missing}，需要重新生成对齐数据")
        
        print("    ✓ 数据验证通过")
        
    def load_data(self):
        """加载数据 - 支持SQL Server"""
        print("="*80)
        print("加载数据...")
        
        # 数据库连接配置
        db_config = {
            'server': 'localhost',
            'database': 'MusicRecommendationDB',
            'username': 'sa',
            'password': '123456',  # ← 改成您的密码
            'driver': 'ODBC Driver 18 for SQL Server'
        }
        
        conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                f"@{db_config['server']}/{db_config['database']}"
                f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
        
        from sqlalchemy import create_engine
        engine = create_engine(conn_str)
        
        # 从SQL读取数据
        self.song_features = pd.read_sql("SELECT * FROM enhanced_song_features", engine)
        print(f"✓ 歌曲特征: {self.song_features.shape}")
        
        # ==================== 歌曲特征字段修复 ====================
    
        # 1. 处理final_popularity（优先使用原始popularity）
        if 'popularity' in self.song_features.columns:
            # 使用原始的popularity（0-100）作为基础
            raw_pop = pd.to_numeric(self.song_features['popularity'], errors='coerce')
            print(f"  原始popularity范围: [{raw_pop.min()}, {raw_pop.max()}]")
            
            # 如果存在final_popularity且不为空，与其混合；否则直接用原始值
            if 'final_popularity' in self.song_features.columns:
                final_pop = pd.to_numeric(self.song_features['final_popularity'], errors='coerce')
                # 优先使用final_popularity，缺失时用原始popularity
                self.song_features['final_popularity'] = final_pop.fillna(raw_pop).fillna(50.0)
            else:
                self.song_features['final_popularity'] = raw_pop.fillna(50.0)
        else:
            print("  警告：CSV中没有popularity字段")
            self.song_features['final_popularity'] = 50.0
        
        # 确保在合理范围
        self.song_features['final_popularity'] = self.song_features['final_popularity'].clip(0, 100)
        
        # 2. 归一化流行度
        self.song_features['final_popularity_norm'] = (
            self.song_features['final_popularity'] / 100.0
        )
        
        # 3. 处理genre_clean（基于原始genre生成）
        if 'genre' in self.song_features.columns:
            print(f"  原始genre示例: {self.song_features['genre'].dropna().head(3).tolist()}")
            
            # 清洗策略：保留前12个热门流派，其他归为'other'
            genre_counts = self.song_features['genre'].value_counts()
            top_genres = genre_counts.head(12).index.tolist()
            print(f"  热门流派: {top_genres[:5]}...")
            
            self.song_features['genre_clean'] = self.song_features['genre'].apply(
                lambda x: x if pd.notna(x) and x in top_genres else 'other'
            )
        else:
            print("  警告：CSV中没有genre字段")
            self.song_features['genre_clean'] = 'unknown'
        
        # 如果genre_clean已存在但全是空值，重新生成
        if self.song_features['genre_clean'].isna().all():
            self.song_features['genre_clean'] = 'unknown'
        
        # 4. 处理popularity_tier（基于final_popularity分层）
        self.song_features['popularity_tier'] = pd.cut(
            self.song_features['final_popularity'],
            bins=[0, 40, 70, 100],  # 调整阈值：0-40普通，40-70热门，70-100爆款
            labels=['normal', 'popular', 'hit'],
            include_lowest=True
        ).astype(str)
        
        # 打印验证
        tier_counts = self.song_features['popularity_tier'].value_counts()
        print(f"  分层统计: {dict(tier_counts)}")
        
        # 5. 处理音频特征（确保数值，填充默认值）
        audio_cols = ['danceability', 'energy', 'valence', 'tempo', 'loudness', 
                    'speechiness', 'acousticness', 'instrumentalness', 'liveness']
        for col in audio_cols:
            if col in self.song_features.columns:
                self.song_features[col] = pd.to_numeric(
                    self.song_features[col], errors='coerce'
                ).fillna(0.5)
            else:
                self.song_features[col] = 0.5
        
        # 6. 处理recency_score（代码可能用到）
        if 'recency_score' not in self.song_features.columns:
            if 'song_age' in self.song_features.columns:
                self.song_features['song_age'] = pd.to_numeric(
                    self.song_features['song_age'], errors='coerce'
                ).fillna(5)
                self.song_features['recency_score'] = np.exp(
                    -self.song_features['song_age'] / 15
                )
            else:
                self.song_features['recency_score'] = 0.5
        else:
            self.song_features['recency_score'] = pd.to_numeric(
                self.song_features['recency_score'], errors='coerce'
            ).fillna(0.5)
        
        # 7. 处理energy_dance和mood_score（代码用到）
        if 'energy_dance' not in self.song_features.columns:
            self.song_features['energy_dance'] = (
                self.song_features['danceability'] + self.song_features['energy']
            ) / 2
        
        if 'mood_score' not in self.song_features.columns:
            self.song_features['mood_score'] = (
                self.song_features['valence'] * 0.6 + self.song_features['energy'] * 0.4
            )
        
        # 8. 确保song_id是字符串
        self.song_features['song_id'] = self.song_features['song_id'].astype(str)
        
        print(f"  流行度范围: [{self.song_features['final_popularity'].min():.1f}, "
            f"{self.song_features['final_popularity'].max():.1f}], "
            f"均值: {self.song_features['final_popularity'].mean():.2f}")
        
        # ==================== 用户特征字段修复 ====================
        
        self.user_features = pd.read_sql("SELECT * FROM enhanced_user_features", engine)
        print(f"✓ 用户特征: {self.user_features.shape}")
        
        # 1. 确保user_id是字符串
        self.user_features['user_id'] = self.user_features['user_id'].astype(str)
        
        # 2. 填充数值特征空值
        user_numeric_cols = {
            'unique_songs': 0,
            'total_interactions': 0,
            'total_weight_sum': 0.0,
            'avg_weight': 0.0,
            'weight_std': 0.0,
            'popularity_bias': 0.0,      # 关键修复字段
            'avg_popularity_pref': 50.0,  # 关键修复字段
            'diversity_ratio': 0.0,
            'age': 25,
            'gender': 0,
            'listen_songs': 0
        }
        
        for col, default_val in user_numeric_cols.items():
            if col in self.user_features.columns:
                self.user_features[col] = pd.to_numeric(
                    self.user_features[col], errors='coerce'
                ).fillna(default_val)
            else:
                self.user_features[col] = default_val
        
        # 3. 处理top_genre字段（代码依赖）
        for i in range(1, 4):
            col = f'top_genre_{i}'
            if col not in self.user_features.columns:
                self.user_features[col] = None
        
        # 4. 处理age_group和activity_level（分类特征）
        if 'age_group' in self.user_features.columns:
            self.user_features['age_group'] = self.user_features['age_group'].fillna('unknown')
        else:
            # 根据age计算
            bins = [0, 18, 25, 35, 50, 100]
            labels = ['<18', '18-25', '26-35', '36-50', '>50']
            self.user_features['age_group'] = pd.cut(
                self.user_features['age'], bins=bins, labels=labels, right=False
            ).astype(str)
        
        if 'activity_level' in self.user_features.columns:
            self.user_features['activity_level'] = self.user_features['activity_level'].fillna('低活跃')
        else:
            self.user_features['activity_level'] = '低活跃'
        
        # ==================== 交互数据加载 ====================
        
        self.interaction_matrix = pd.read_sql("SELECT * FROM filtered_interactions", engine)
        print(f"✓ 交互矩阵: {self.interaction_matrix.shape}")
        
        self.train_interactions = pd.read_sql("SELECT * FROM train_interactions", engine)
        self.test_interactions = pd.read_sql("SELECT * FROM test_interactions", engine)
        print(f"✓ 训练/测试: {len(self.train_interactions):,} / {len(self.test_interactions):,}")
        
        # 统一ID类型
        for df in [self.interaction_matrix, self.train_interactions, self.test_interactions]:
            if 'user_id' in df.columns:
                df['user_id'] = df['user_id'].astype(str)
            if 'song_id' in df.columns:
                df['song_id'] = df['song_id'].astype(str)
        
        # 确保total_weight是数值
        for df in [self.interaction_matrix, self.train_interactions, self.test_interactions]:
            if 'total_weight' in df.columns:
                df['total_weight'] = pd.to_numeric(df['total_weight'], errors='coerce').fillna(0)
        
        print("  数据加载和修复完成")
        
    def build_matrices(self):
        """构建矩阵"""
        print("\n构建矩阵...")
        cache_file = os.path.join(self.cache_dir, "user_song_matrix.npz")
        
        if os.path.exists(cache_file):
            print("  从缓存加载...")
            self.user_song_matrix = load_npz(cache_file)
            with open(os.path.join(self.cache_dir, "mappings.pkl"), 'rb') as f:
                mappings = pickle.load(f)
                self.user_to_idx = mappings['user_to_idx']
                self.idx_to_user = mappings['idx_to_user']
                self.song_to_idx = mappings['song_to_idx']
                self.idx_to_song = mappings['idx_to_song']
        else:
            print("  构建新矩阵...")
            train_data = self.train_interactions
            all_users = train_data['user_id'].unique().tolist()
            all_songs = train_data['song_id'].unique().tolist()
            
            self.user_to_idx = {u: i for i, u in enumerate(all_users)}
            self.idx_to_user = {i: u for i, u in enumerate(all_users)}
            self.song_to_idx = {s: i for i, s in enumerate(all_songs)}
            self.idx_to_song = {i: s for i, s in enumerate(all_songs)}
            
            rows = [self.user_to_idx[str(uid)] for uid in train_data['user_id']]
            cols = [self.song_to_idx[sid] for sid in train_data['song_id']]
            data = train_data['total_weight'].values
            
            self.user_song_matrix = csr_matrix((data, (rows, cols)), shape=(len(all_users), len(all_songs)))
            
            save_npz(cache_file, self.user_song_matrix)
            with open(os.path.join(self.cache_dir, "mappings.pkl"), 'wb') as f:
                pickle.dump({
                    'user_to_idx': self.user_to_idx,
                    'idx_to_user': self.idx_to_user,
                    'song_to_idx': self.song_to_idx,
                    'idx_to_song': self.idx_to_song
                }, f)
        
        self.n_users, self.n_songs = self.user_song_matrix.shape
        density = self.user_song_matrix.nnz / (self.n_users * self.n_songs) * 100
        print(f"  矩阵: {self.n_users}x{self.n_songs}, 密度: {density:.4f}%")
        
    def calculate_similarities_optimized(self):
        """计算相似度"""
        print("\n计算相似度...")
        start = time.time()
        
        self.calculate_popular_songs()
        self.calculate_user_similarities()
        self.calculate_content_similarities()
        self.calculate_matrix_factorization()
        
        print(f"✓ 完成，耗时: {time.time()-start:.2f}秒")
        
    def calculate_popular_songs(self):
        """分层热门歌曲"""
        print("  1. 热门歌曲分层...")
        
        # 按流行度分3层
        self.tiered_songs = {
            'hit': self.song_features[self.song_features['final_popularity'] >= 70]['song_id'].tolist(),
            'popular': self.song_features[(self.song_features['final_popularity'] >= 40) & 
                                         (self.song_features['final_popularity'] < 70)]['song_id'].tolist(),
            'normal': self.song_features[self.song_features['final_popularity'] < 40]['song_id'].tolist()
        }
        
        for tier, songs in self.tiered_songs.items():
            print(f"    {tier}: {len(songs)}首")
        
        self.song_popularity = self.song_features.set_index('song_id')['final_popularity'].to_dict()
        
    def calculate_user_similarities(self):
        """用户相似度（使用MF向量）"""
        print("  2. 用户相似度...")
        cache_file = os.path.join(self.cache_dir, "user_sim.pkl")
        
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                self.user_similarities = pickle.load(f)
            print(f"    从缓存加载: {len(self.user_similarities)}用户")
            return
        
        # 先计算MF
        self.calculate_matrix_factorization()
        
        self.user_similarities = {}
        
        if self.user_factors is not None:
            # 归一化
            norms = np.linalg.norm(self.user_factors, axis=1, keepdims=True)
            norms[norms == 0] = 1
            factors_norm = self.user_factors / norms
            
            # 批量计算
            batch_size = 2000
            for i in range(0, self.n_users, batch_size):
                end_i = min(i + batch_size, self.n_users)
                sim_batch = np.dot(factors_norm[i:end_i], factors_norm.T)
                
                for j in range(i, end_i):
                    user_id = self.idx_to_user[j]
                    scores = sim_batch[j - i].copy()
                    scores[j] = -1  # 排除自己
                    
                    top_idx = np.argsort(scores)[-20:][::-1]
                    neighbors = {self.idx_to_user[idx]: float(scores[idx]) 
                               for idx in top_idx if scores[idx] > 0.15}
                    
                    if neighbors:
                        self.user_similarities[user_id] = neighbors
        
        with open(cache_file, 'wb') as f:
            pickle.dump(self.user_similarities, f)
        
        print(f"    计算完成: {len(self.user_similarities)}用户")
        
    def calculate_content_similarities(self):
        """内容相似度"""
        print("  3. 内容相似度...")
        cache_file = os.path.join(self.cache_dir, "content_sim.pkl")
        
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                self.content_similarities = pickle.load(f)
            print(f"    从缓存加载: {len(self.content_similarities)}歌曲")
            return
        
        features = ['danceability', 'energy', 'valence', 'final_popularity_norm', 'recency_score']
        available = [f for f in features if f in self.song_features.columns]
        
        song_ids = self.song_features['song_id'].tolist()
        X = self.song_features[available].fillna(0.5).values
        X = MinMaxScaler().fit_transform(X)
        
        # 使用最近邻
        n_neighbors = min(21, len(song_ids))
        nn = NearestNeighbors(n_neighbors=n_neighbors, metric='cosine')
        nn.fit(X)
        distances, indices = nn.kneighbors(X)
        
        self.content_similarities = {}
        for i, song_id in enumerate(song_ids):
            sims = {}
            for j in range(1, len(indices[i])):
                neighbor = song_ids[indices[i][j]]
                sim = 1 - distances[i][j]
                if sim > 0.25:
                    sims[neighbor] = sim
            if sims:
                self.content_similarities[song_id] = dict(sorted(sims.items(), key=lambda x: x[1], reverse=True)[:15])
        
        with open(cache_file, 'wb') as f:
            pickle.dump(self.content_similarities, f)
        
        print(f"    计算完成: {len(self.content_similarities)}歌曲")
        
    def calculate_matrix_factorization(self):
        """矩阵分解"""
        cache_file = os.path.join(self.cache_dir, "mf.pkl")
        
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                d = pickle.load(f)
                self.user_factors = d['user_factors']
                self.song_factors = d['song_factors']
        else:
            print("  4. 矩阵分解...")
            n_comp = min(50, min(self.user_song_matrix.shape) - 1)
            if n_comp >= 20:
                svd = TruncatedSVD(n_components=n_comp, random_state=42)
                self.user_factors = svd.fit_transform(self.user_song_matrix)
                self.song_factors = svd.components_.T
                
                with open(cache_file, 'wb') as f:
                    pickle.dump({'user_factors': self.user_factors, 'song_factors': self.song_factors}, f)
                
                print(f"    解释方差: {svd.explained_variance_ratio_.sum():.4f}")
            else:
                self.user_factors = None
                self.song_factors = None
        
    def get_user_profile(self, user_id):
        """获取用户画像"""
        user_id = str(user_id)
        if user_id not in self.user_features['user_id'].values:
            return None
        
        row = self.user_features[self.user_features['user_id'] == user_id].iloc[0]
        
        return {
            'user_id': user_id,
            'n_songs': int(row.get('unique_songs', 0)),
            'top_genres': [row.get(f'top_genre_{i}') for i in range(1, 4) if pd.notna(row.get(f'top_genre_{i}'))],
            'popularity_bias': float(row.get('popularity_bias', 0)),
            'avg_popularity': float(row.get('avg_popularity_pref', 50))
        }
        
    def item_based_cf(self, user_id, n=20):
        """基于物品的协同过滤（适配开题报告）"""
        if user_id not in self.user_to_idx:
            return []
        
        user_idx = self.user_to_idx[user_id]
        # 获取用户历史喜欢的歌曲
        liked_songs = self.user_song_matrix[user_idx].nonzero()[1]
        
        # 计算物品相似度（歌曲-歌曲矩阵）
        if not hasattr(self, 'item_similarities'):
            # 基于共现矩阵计算物品相似度（可在build_matrices中预计算）
            from sklearn.metrics.pairwise import cosine_similarity
            # 转置得到物品-用户矩阵，计算物品间余弦相似度
            item_user_matrix = self.user_song_matrix.T
            self.item_similarities = cosine_similarity(item_user_matrix)
        
        scores = {}
        for song_idx in liked_songs:
            # 获取与该歌曲相似的其他歌曲
            similar_items = self.item_similarities[song_idx]
            for other_idx, sim_score in enumerate(similar_items):
                if other_idx not in liked_songs and sim_score > 0.1:
                    other_id = self.idx_to_song[other_idx]
                    scores[other_id] = scores.get(other_id, 0) + sim_score
        
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def content_based(self, user_id, n=20):
        """基于内容的推荐"""
        user_id = str(user_id)
        if user_id not in self.user_to_idx:
            return []
        
        user_idx = self.user_to_idx[user_id]
        interacted = list(self.user_song_matrix[user_idx].nonzero()[1])
        
        scores = {}
        for song_idx in interacted[:50]:
            song_id = self.idx_to_song[song_idx]
            pref = self.user_song_matrix[user_idx, song_idx]
            
            for sim_song, sim_score in self.content_similarities.get(song_id, {}).items():
                sim_idx = self.song_to_idx.get(sim_song)
                if sim_idx and sim_idx not in interacted:
                    scores[sim_song] = scores.get(sim_song, 0) + sim_score * pref
        
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def matrix_factorization_rec(self, user_id, n=20):
        """MF推荐"""
        user_id = str(user_id)
        if user_id not in self.user_to_idx or self.user_factors is None:
            return []
        
        user_idx = self.user_to_idx[user_id]
        user_vec = self.user_factors[user_idx]
        interacted = set(self.user_song_matrix[user_idx].nonzero()[1])
        
        scores = {}
        for song_id, song_idx in self.song_to_idx.items():
            if song_idx not in interacted:
                score = np.dot(user_vec, self.song_factors[song_idx])
                if score > 0:
                    scores[song_id] = score
        
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def get_cold_start_recs(self, profile, n=10):
        """冷启动推荐（分层采样）"""
        if profile is None:
            # 完全冷启动：随机各层
            candidates = (random.sample(self.tiered_songs.get('hit', []), min(3, len(self.tiered_songs.get('hit', [])))) +
                         random.sample(self.tiered_songs.get('popular', []), min(4, len(self.tiered_songs.get('popular', [])))) +
                         random.sample(self.tiered_songs.get('normal', []), min(3, len(self.tiered_songs.get('normal', [])))))
            return [(cid, 0.5) for cid in candidates[:n]]
        
        pref_genres = profile.get('top_genres', [])
        bias = profile.get('popularity_bias', 0)
        
        # 根据偏好动态配比
        if bias < -10:  # 喜欢冷门
            allocation = {'hit': 2, 'popular': 3, 'normal': 5}
        elif bias > 10:  # 喜欢热门
            allocation = {'hit': 5, 'popular': 3, 'normal': 2}
        else:
            allocation = {'hit': 3, 'popular': 4, 'normal': 3}
        
        candidates = []
        for tier, count in allocation.items():
            tier_songs = self.tiered_songs.get(tier, [])
            if not tier_songs:
                continue
            
            # 过滤流派
            if pref_genres:
                tier_df = self.song_features[self.song_features['song_id'].isin(tier_songs)]
                matched = tier_df[tier_df['genre_clean'].isin(pref_genres)]['song_id'].tolist()
                pool = matched if len(matched) >= count else tier_songs
            else:
                pool = tier_songs
            
            selected = random.sample(pool, min(count, len(pool)))
            for sid in selected:
                pop = self.song_popularity.get(sid, 50)
                info = self.get_song_info(sid)
                boost = 0.3 if info and info.get('genre') in pref_genres else 0
                score = (pop / 100) * 0.5 + boost
                candidates.append((sid, score))
        
        # 去重排序
        seen = set()
        result = []
        for cid, score in sorted(candidates, key=lambda x: x[1], reverse=True):
            if cid not in seen:
                result.append((cid, score))
                seen.add(cid)
        
        return result[:n]
    
    def mmr_rerank(self, candidates, user_id, n=10):
        """MMR多样性重排"""
        if len(candidates) <= n:
            return candidates
        
        profile = self.get_user_profile(user_id)
        history = self.get_user_history(user_id, n=10)
        history_genres = {h.get('genre', '') for h in history}
        
        selected = [candidates[0]]
        remaining = candidates[1:]
        
        while len(selected) < n and remaining:
            best_score = -float('inf')
            best_idx = -1
            
            for i, (song_id, relevance) in enumerate(remaining):
                info = self.get_song_info(song_id)
                if not info:
                    continue
                
                # 计算与已选的最大相似度
                max_sim = 0
                for sel_id, _ in selected:
                    content_sim = self.content_similarities.get(song_id, {}).get(sel_id, 0)
                    sel_info = self.get_song_info(sel_id)
                    genre_sim = 0.5 if sel_info and info.get('genre') == sel_info.get('genre') else 0
                    max_sim = max(max_sim, content_sim, genre_sim)
                
                # 多样性奖励
                diversity_bonus = 0
                if len(selected) < 3 and info.get('genre') in history_genres:
                    diversity_bonus = -0.2
                
                mmr_score = 0.6 * relevance - 0.4 * max_sim + diversity_bonus
                
                if mmr_score > best_score:
                    best_score = mmr_score
                    best_idx = i
            
            if best_idx >= 0:
                selected.append(remaining.pop(best_idx))
        
        return selected
    
    def hybrid_recommendation(self, user_id, n=10):
        """混合推荐"""
        user_id = str(user_id)
        profile = self.get_user_profile(user_id)
        n_songs = profile.get('n_songs', 0) if profile else 0
        
        # 多路召回
        all_scores = {}
        
        if n_songs >= 30:  # 活跃用户
            sources = {
                'cf': self.item_based_cf(user_id, 40),
                'content': self.content_based(user_id, 30),
                'mf': self.matrix_factorization_rec(user_id, 30)
            }
            weights = {'cf': 0.4, 'content': 0.3, 'mf': 0.3}
        elif n_songs >= 5:  # 普通用户
            sources = {
                'cf': self.item_based_cf(user_id, 30),
                'content': self.content_based(user_id, 40),
                'mf': self.matrix_factorization_rec(user_id, 20),
                'cold': self.get_cold_start_recs(profile, 10)
            }
            weights = {'cf': 0.3, 'content': 0.3, 'mf': 0.2, 'cold': 0.2}
        else:  # 冷启动
            sources = {
                'cf': self.item_based_cf(user_id, 15),
                'content': self.content_based(user_id, 15),
                'cold': self.get_cold_start_recs(profile, 70)
            }
            weights = {'cf': 0.2, 'content': 0.2, 'mf': 0, 'cold': 0.6}
        
        # 融合
        for source, recs in sources.items():
            if not recs:
                continue
            max_score = max([r[1] for r in recs]) if recs else 1
            for sid, score in recs:
                norm_score = (score / max_score) if max_score > 0 else 0.5
                all_scores[sid] = all_scores.get(sid, 0) + norm_score * weights.get(source, 0.1)
        
        if not all_scores:
            return self.get_cold_start_recs(profile, n)
        
        candidates = sorted(all_scores.items(), key=lambda x: x[1], reverse=True)[:min(100, len(all_scores))]
        return self.mmr_rerank(candidates, user_id, n)
    
    def save_recommendations_to_sql(self, user_id, recs, algorithm_type='hybrid', 
                                   expire_days=7, engine=None):
        """
        将推荐结果保存到SQL数据库
        
        Args:
            user_id: 用户ID
            recs: 推荐列表 [(song_id, score), ...]
            algorithm_type: 算法类型标识
            expire_days: 推荐结果过期天数
            engine: SQLAlchemy引擎（如果为None则创建新连接）
        """
        if not recs:
            return
        
        from datetime import datetime, timedelta
        import pandas as pd
        
        # 准备数据
        now = datetime.now()
        expire_time = now + timedelta(days=expire_days)
        
        data = []
        for rank, (song_id, score) in enumerate(recs, 1):
            data.append({
                'user_id': str(user_id),
                'song_id': str(song_id),
                'recommendation_score': float(score),
                'algorithm_type': algorithm_type,
                'rank_position': rank,
                'is_viewed': False,
                'is_clicked': False,
                'is_listened': False,
                'created_at': now,
                'expires_at': expire_time
            })
        
        try:
            # 如果没有传入engine，创建新连接
            if engine is None:
                from sqlalchemy import create_engine
                db_config = {
                    'server': 'localhost',
                    'database': 'MusicRecommendationDB',
                    'username': 'sa',
                    'password': '123456',  # ← 改成您的密码
                    'driver': 'ODBC Driver 18 for SQL Server'
                }
                conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                           f"@{db_config['server']}/{db_config['database']}"
                           f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
                engine = create_engine(conn_str)
            
            # 转换为DataFrame
            df = pd.DataFrame(data)
            
            # 使用try-except处理可能的重复插入错误（违反唯一约束）
            try:
                df.to_sql('recommendations', engine, if_exists='append', index=False)
                print(f"  ✓ 已保存用户 {user_id} 的 {len(recs)} 条推荐")
            except Exception as e:
                # 如果是重复插入错误，使用merge/update逻辑
                if 'UNIQUE' in str(e) or 'duplicate' in str(e).lower():
                    # 先删除该用户今天的旧推荐，再插入新的
                    with engine.begin() as conn:
                        from sqlalchemy import text
                        conn.execute(text(
                            f"DELETE FROM recommendations WHERE user_id = '{user_id}' "
                            f"AND CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)"
                        ))
                    # 重新插入
                    df.to_sql('recommendations', engine, if_exists='append', index=False)
                    print(f"  ✓ 已更新用户 {user_id} 的推荐（覆盖旧数据）")
                else:
                    raise e
                    
        except Exception as e:
            print(f"  ⚠️ 保存推荐结果失败: {e}")
            # 不抛出异常，避免影响主流程
    
    def batch_save_recommendations(self, user_ids, n=10, algorithm_type='hybrid'):
        """
        批量为多个用户生成并保存推荐（用于定时任务或离线批量计算）
        
        Args:
            user_ids: 用户ID列表
            n: 每个用户推荐数量
            algorithm_type: 算法类型
        """
        from sqlalchemy import create_engine
        from datetime import datetime
        
        db_config = {
            'server': 'localhost',
            'database': 'MusicRecommendationDB',
            'username': 'sa',
            'password': '123456',  # ← 改成您的密码
            'driver': 'ODBC Driver 18 for SQL Server'
        }
        conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                   f"@{db_config['server']}/{db_config['database']}"
                   f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
        engine = create_engine(conn_str)
        
        print(f"\n开始批量生成并保存推荐（{len(user_ids)}个用户）...")
        start_time = datetime.now()
        
        success_count = 0
        fail_count = 0
        
        for i, user_id in enumerate(user_ids, 1):
            try:
                # 生成推荐
                recs = self.hybrid_recommendation(user_id, n=n)
                
                if recs:
                    # 保存到SQL（复用同一个engine）
                    self.save_recommendations_to_sql(
                        user_id, recs, 
                        algorithm_type=algorithm_type,
                        engine=engine  # 传入已有连接，避免重复创建
                    )
                    success_count += 1
                else:
                    print(f"  用户 {user_id} 无推荐结果")
                    fail_count += 1
                    
                if i % 100 == 0:
                    print(f"  进度: {i}/{len(user_ids)} ({i/len(user_ids)*100:.1f}%)")
                    
            except Exception as e:
                print(f"  用户 {user_id} 处理失败: {e}")
                fail_count += 1
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n批量保存完成:")
        print(f"  成功: {success_count} 用户")
        print(f"  失败: {fail_count} 用户")
        print(f"  耗时: {elapsed:.1f}秒 ({elapsed/len(user_ids):.2f}秒/用户)")
    
    def get_song_info(self, song_id):
        """获取歌曲信息"""
        if not hasattr(self, '_cache'):
            self._cache = {}
        
        if song_id in self._cache:
            return self._cache[song_id]
        
        data = self.song_features[self.song_features['song_id'] == song_id]
        if not data.empty:
            row = data.iloc[0]
            info = {
                'song_id': song_id,
                'song_name': row.get('song_name', '未知'),
                'artists': row.get('artists', '未知'),
                'genre': row.get('genre_clean', row.get('genre', '未知')),
                'popularity': int(row.get('final_popularity', 50)),
            }
            self._cache[song_id] = info
            return info
        return None
    
    def get_user_history(self, user_id, n=5):
        """获取用户历史"""
        hist = self.train_interactions[self.train_interactions['user_id'] == user_id]
        if hist.empty:
            return []
        
        hist = hist.sort_values('total_weight', ascending=False).head(n)
        return [self.get_song_info(row['song_id']) for _, row in hist.iterrows() if self.get_song_info(row['song_id'])]
    
    def test_recommendation(self, user_id=None, n=8):
        """测试"""
        print("\n" + "-"*60)
        print("推荐测试")
        
        if user_id is None:
            counts = self.train_interactions.groupby('user_id').size()
            user_id = counts.idxmax()
        
        profile = self.get_user_profile(user_id)
        if profile:
            print(f"用户: {user_id}")
            print(f"  听过: {profile['n_songs']}首")
            print(f"  流派偏好: {profile['top_genres']}")
            print(f"  流行度偏好: {'冷门' if profile['popularity_bias'] < -5 else '热门' if profile['popularity_bias'] > 5 else '平均'}")
        
        history = self.get_user_history(user_id, 3)
        if history:
            print("\n历史收听:")
            for h in history:
                print(f"  - {h['song_name']} ({h['genre']}, 流行度{h['popularity']})")
        
        recs = self.hybrid_recommendation(user_id, n)
        print(f"\n推荐结果 ({len(recs)}首):")
        
        genres = []
        for i, (sid, score) in enumerate(recs, 1):
            info = self.get_song_info(sid)
            if info:
                genres.append(info['genre'])
                print(f"  {i}. {info['song_name']} ({info['genre']}, 流行度{info['popularity']}) [{score:.2f}]")
        
        unique_genres = len(set(genres))
        avg_pop = np.mean([self.song_popularity.get(r[0], 50) for r in recs]) if recs else 0
        print(f"\n统计: {unique_genres}个流派, 平均流行度{avg_pop:.1f}")
        
        return recs


class RecommenderEvaluator:
    """评估器（集成SQL保存功能）"""
    
    def __init__(self, recommender, save_to_sql=False):
        self.rec = recommender
        self.save_to_sql = save_to_sql
        
        # 初始化数据库连接（如果需要保存）
        if save_to_sql:
            try:
                from sqlalchemy import create_engine
                db_config = {
                    'server': 'localhost',
                    'database': 'MusicRecommendationDB',
                    'username': 'sa',
                    'password': '123456',  # ← 改成您的密码
                    'driver': 'ODBC Driver 18 for SQL Server'
                }
                conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                           f"@{db_config['server']}/{db_config['database']}"
                           f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
                self.engine = create_engine(conn_str)
                print("  ✓ 已连接到SQL Server（推荐结果将保存到数据库）")
            except Exception as e:
                print(f"  ⚠️ 数据库连接失败: {e}，将仅进行评估不保存")
                self.engine = None
                self.save_to_sql = False
        else:
            self.engine = None
        
    def evaluate(self, n_users=300, k=10, save_recs=False):
        """
        评估推荐系统性能
        
        Args:
            n_users: 评估用户数量
            k: Top-K推荐
            save_recs: 是否保存推荐结果到数据库
        """
        print("="*80)
        print(f"系统评估 ({n_users}用户, Top{k})...")
        if save_recs and self.save_to_sql:
            print("  [模式：评估并保存推荐结果到SQL]")
        else:
            print("  [模式：仅评估，不保存]")
        
        import numpy as np
        import random
        from datetime import datetime
        
        random.seed(42)
        np.random.seed(42)
        
        users = list(self.rec.user_to_idx.keys())
        test_users = random.sample(users, min(n_users, len(users)))
        
        metrics = {
            'precision': [], 
            'recall': [], 
            'hit': [], 
            'ndcg': [], 
            'diversity': [], 
            'coverage': set(), 
            'popularity': []
        }
        
        # 用于批量保存的缓冲
        recs_buffer = []
        batch_size = 50  # 每50用户保存一次
        
        start_time = datetime.now()
        
        for i, uid in enumerate(test_users):
            if (i+1) % 50 == 0:
                print(f"  进度: {i+1}/{n_users}")
                # 批量保存
                if save_recs and self.engine and recs_buffer:
                    self._flush_recs_buffer(recs_buffer)
                    recs_buffer = []
            
            # 获取测试集（该用户在测试集中交互过的歌曲）
            test_songs = []
            if len(self.rec.test_interactions) > 0:
                test_df = self.rec.test_interactions[self.rec.test_interactions['user_id'] == uid]
                test_songs = test_df['song_id'].tolist()
            
            if not test_songs:
                continue
            
            # 生成推荐
            try:
                recs = self.rec.hybrid_recommendation(uid, k)
            except Exception as e:
                print(f"    用户 {uid} 推荐生成失败: {e}")
                continue
            
            items = [r[0] for r in recs]
            
            if not items:
                continue
            
            # 保存到缓冲
            if save_recs and self.engine:
                recs_buffer.append((uid, recs))
            
            # 更新覆盖率统计
            metrics['coverage'].update(items)
            
            # 计算命中率
            hits = len(set(items) & set(test_songs))
            metrics['precision'].append(hits / k)
            metrics['recall'].append(hits / len(test_songs))
            metrics['hit'].append(1 if hits > 0 else 0)
            
            # 计算NDCG
            dcg = sum([1.0/np.log2(idx+2) for idx, it in enumerate(items) if it in test_songs])
            idcg = sum([1.0/np.log2(i+2) for i in range(min(len(test_songs), k))])
            metrics['ndcg'].append(dcg/idcg if idcg > 0 else 0)
            
            # 计算多样性（流派种类数/推荐数）
            genres = [self.rec.get_song_info(it).get('genre_clean', 'unknown') 
                     for it in items if self.rec.get_song_info(it)]
            metrics['diversity'].append(len(set(genres)) / len(genres) if genres else 0)
            
            # 计算平均流行度
            pops = [self.rec.song_popularity.get(it, 50) for it in items]
            metrics['popularity'].append(np.mean(pops) if pops else 50)
        
        # 保存剩余的推荐
        if save_recs and self.engine and recs_buffer:
            self._flush_recs_buffer(recs_buffer)
        
        # 汇总结果
        results = {
            'Precision@K': np.mean(metrics['precision']) if metrics['precision'] else 0,
            'Recall@K': np.mean(metrics['recall']) if metrics['recall'] else 0,
            'HitRate': np.mean(metrics['hit']) if metrics['hit'] else 0,
            'NDCG@K': np.mean(metrics['ndcg']) if metrics['ndcg'] else 0,
            'Diversity': np.mean(metrics['diversity']) if metrics['diversity'] else 0,
            'Coverage': len(metrics['coverage']) / self.rec.n_songs if self.rec.n_songs else 0,
            'AvgPopularity': np.mean(metrics['popularity']) if metrics['popularity'] else 0,
        }
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print("\n评估结果:")
        for key, val in results.items():
            if isinstance(val, float):
                print(f"  {key}: {val:.4f}")
            else:
                print(f"  {key}: {val}")
        print(f"  评估耗时: {elapsed:.1f}秒")
        
        return results
    
    def _flush_recs_buffer(self, buffer):
        """批量刷新推荐结果到数据库"""
        if not buffer or not self.engine:
            return
        
        try:
            import pandas as pd
            from datetime import datetime, timedelta
            
            now = datetime.now()
            expire_time = now + timedelta(days=7)  # 7天过期
            
            data = []
            for user_id, recs in buffer:
                for rank, (song_id, score) in enumerate(recs, 1):
                    data.append({
                        'user_id': str(user_id),
                        'song_id': str(song_id),
                        'recommendation_score': float(score),
                        'algorithm_type': 'hybrid',
                        'rank_position': rank,
                        'is_viewed': False,
                        'is_clicked': False,
                        'is_listened': False,
                        'created_at': now,
                        'expires_at': expire_time
                    })
            
            df = pd.DataFrame(data)
            
            # 处理重复：如果该用户今天已有推荐，先删除再插入（覆盖策略）
            user_ids_today = list(set([d['user_id'] for d in data]))
            if user_ids_today:
                from sqlalchemy import text
                with self.engine.begin() as conn:
                    for uid in user_ids_today:
                        conn.execute(text(
                            f"DELETE FROM recommendations WHERE user_id = '{uid}' "
                            f"AND CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)"
                        ))
            
            # 插入新数据
            df.to_sql('recommendations', self.engine, if_exists='append', index=False)
            print(f"    已保存 {len(buffer)} 用户的推荐（共 {len(data)} 条）")
            
        except Exception as e:
            print(f"    保存推荐结果失败: {e}")
            # 不抛出异常，避免影响主流程


def main():
    """主函数"""
    print("="*80)
    print("音乐推荐系统 (SQL集成版 - 数据验证)")
    print("="*80)
    
    align_dir = "aligned_data_optimized"
    need_align = False
    
    # 检查是否需要重新对齐（保持原有逻辑）
    if not os.path.exists(align_dir):
        need_align = True
        print(f"\n[!] 对齐数据目录不存在: {align_dir}")
    else:
        song_file = os.path.join(align_dir, "enhanced_song_features.csv")
        if not os.path.exists(song_file):
            need_align = True
            print(f"\n[!] 找不到歌曲特征文件")
        else:
            try:
                df = pd.read_csv(song_file, nrows=3)
                if 'final_popularity' not in df.columns:
                    need_align = True
                    print(f"\n[!] 数据文件缺少final_popularity列")
                else:
                    print(f"\n[✓] 检测到有效对齐数据")
            except Exception as e:
                need_align = True
                print(f"\n[!] 数据文件读取失败: {e}")
    
    # 数据对齐（保持原有逻辑）
    if need_align:
        print("\n" + "="*80)
        print("重新生成对齐数据...")
        print("="*80)
        try:
            aligner = DataAlignmentAndEnhancement()
            aligner.run_balanced()
            print("\n[✓] 数据对齐完成")
        except Exception as e:
            print(f"\n[✗] 数据对齐失败: {e}")
            return
    
    # 初始化推荐系统（会自动从SQL加载数据，使用您修改后的load_data）
    print("\n" + "="*80)
    print("初始化推荐系统...")
    print("="*80)
    try:
        recommender = OptimizedMusicRecommender()
    except Exception as e:
        print(f"\n[✗] 初始化失败: {e}")
        return
    
    # ========== 1. 系统评估并保存结果到SQL ==========
    print("\n" + "="*80)
    print("系统评估（并保存推荐结果到SQL）...")
    print("="*80)
    
    # 创建评估器（save_to_sql=True 启用数据库保存）
    evaluator = RecommenderEvaluator(recommender, save_to_sql=True)
    results = evaluator.evaluate(n_users=500, k=10, save_recs=True)
    
    # ========== 2. 为测试用户生成推荐并展示 ==========
    print("\n" + "="*80)
    print("测试案例（同时保存到SQL）...")
    print("="*80)
    
    # 获取活跃用户
    user_activity = recommender.train_interactions.groupby('user_id').size()
    
    # 高活跃用户
    active_user = user_activity.idxmax()
    print(f"\n>>> 高活跃用户 (交互{user_activity.max()}次):")
    recs = recommender.test_recommendation(active_user, 8)
    # 保存这条推荐
    recommender.save_recommendations_to_sql(active_user, recs, algorithm_type='hybrid')
    
    # 普通用户
    median_user = user_activity.median()
    median_users = user_activity[user_activity == int(median_user)]
    if len(median_users) > 0:
        normal_user = median_users.index[0]
        print(f"\n>>> 普通活跃用户 (交互{int(median_user)}次):")
        recs = recommender.test_recommendation(normal_user, 8)
        recommender.save_recommendations_to_sql(normal_user, recs, algorithm_type='hybrid')
    
    # 冷启动用户
    cold_user = user_activity.idxmin()
    print(f"\n>>> 冷启动用户 (交互{user_activity.min()}次):")
    recs = recommender.test_recommendation(cold_user, 8)
    recommender.save_recommendations_to_sql(cold_user, recs, algorithm_type='cold')
    
    # ========== 3. 批量生成前1000用户的推荐（可选，用于生产环境） ==========
    print("\n" + "="*80)
    print("批量生成用户推荐（前500活跃用户）...")
    print("="*80)
    
    # 获取最活跃的500用户（实际生产环境可以用所有用户）
    top_users = user_activity.head(500).index.tolist()
    recommender.batch_save_recommendations(top_users, n=10, algorithm_type='hybrid')
    
    # ========== 4. 验证SQL中的数据 ==========
    print("\n" + "="*80)
    print("验证SQL中的推荐数据...")
    print("="*80)
    
    try:
        from sqlalchemy import create_engine, text
        db_config = {
            'server': 'localhost',
            'database': 'MusicRecommendationDB',
            'username': 'sa',
            'password': '123456',  # ← 改成您的密码
            'driver': 'ODBC Driver 18 for SQL Server'
        }
        conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                   f"@{db_config['server']}/{db_config['database']}"
                   f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
        engine = create_engine(conn_str)
        
        # 查询今日推荐统计
        stats_df = pd.read_sql("""
            SELECT 
                COUNT(*) as total_recs,
                COUNT(DISTINCT user_id) as total_users,
                COUNT(DISTINCT song_id) as total_songs,
                AVG(recommendation_score) as avg_score
            FROM recommendations 
            WHERE CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)
        """, engine)
        
        print(f"今日推荐统计:")
        print(f"  总推荐数: {stats_df.iloc[0]['total_recs']}")
        print(f"  覆盖用户: {stats_df.iloc[0]['total_users']}")
        print(f"  涉及歌曲: {stats_df.iloc[0]['total_songs']}")
        print(f"  平均得分: {stats_df.iloc[0]['avg_score']:.2f}")
        
        # 查询各算法类型分布
        algo_df = pd.read_sql("""
            SELECT algorithm_type, COUNT(*) as count
            FROM recommendations
            WHERE CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)
            GROUP BY algorithm_type
        """, engine)
        
        print(f"\n算法类型分布:")
        for _, row in algo_df.iterrows():
            print(f"  {row['algorithm_type']}: {row['count']}条")
            
    except Exception as e:
        print(f"查询统计失败: {e}")
    
    print("\n" + "="*80)
    print("运行完成！所有推荐结果已保存到SQL Server")
    print("表名: recommendations")
    print("="*80)


if __name__ == "__main__":
    main()