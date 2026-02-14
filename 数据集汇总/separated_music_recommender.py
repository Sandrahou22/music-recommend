# file: separated_music_recommender.py
"""
分离式音乐推荐系统（全算法保留 + LightFM）
- 完整缓存机制
- 矩阵分解（MF）+ Faiss 加速
- 所有算法：ItemCF, UserCF, Content, MF, Sentiment, Artist, LightFM
- MMR多样性重排（可关闭）
- 推荐结果保存到SQL
- 离线评估（性能优化）
- 权重自动调优（5算法）+ 固定Artist/LightFM权重
- 文本特征 + 冷启动优化
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix, save_npz, load_npz
from sklearn.preprocessing import RobustScaler, LabelEncoder
from sklearn.neighbors import NearestNeighbors
from sklearn.decomposition import TruncatedSVD
import warnings
warnings.filterwarnings('ignore')
import os
import pickle
import time
import random
from datetime import datetime, timedelta
from collections import Counter
import hashlib

# 尝试导入 Faiss
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    print("Faiss not installed, using original MF method.")

# 尝试导入 Sentence-Transformers
try:
    from sentence_transformers import SentenceTransformer
    TEXT_MODEL_AVAILABLE = True
except ImportError:
    TEXT_MODEL_AVAILABLE = False
    print("sentence-transformers not installed, text features disabled.")

# 尝试导入 LightFM
try:
    from lightfm import LightFM
    from lightfm.data import Dataset
    LIGHTFM_AVAILABLE = True
except ImportError:
    LIGHTFM_AVAILABLE = False
    print("lightfm not installed, LightFM disabled.")

# ---------------------------- 数据加载器 ----------------------------
class SeparatedDataLoader:
    """分离式数据加载器 - 从SQL Server读取数据（增强字段修复）"""
    
    def __init__(self, base_dir=None):
        self.db_config = {
            'server': 'localhost',
            'database': 'MusicRecommendationDB',
            'username': 'sa',
            'password': '123456',   # 改为你的密码
            'driver': 'ODBC Driver 18 for SQL Server'
        }
    
    def _get_engine(self):
        from sqlalchemy import create_engine
        conn_str = (f"mssql+pyodbc://{self.db_config['username']}:{self.db_config['password']}"
                    f"@{self.db_config['server']}/{self.db_config['database']}"
                    f"?driver={self.db_config['driver'].replace(' ', '+')}&Encrypt=no")
        return create_engine(conn_str, echo=False)
    
    def load_all_data(self):
        print("="*80)
        print("从SQL Server加载分离式数据...")
        engine = self._get_engine()
        
        # ----- 1. 歌曲特征（含字段修复）-----
        song_df = pd.read_sql("SELECT * FROM enhanced_song_features", engine)
        song_df['song_id'] = song_df['song_id'].astype(str)
        
        # 修复final_popularity
        if 'final_popularity' not in song_df.columns:
            song_df['final_popularity'] = song_df.get('popularity', 50.0)
        song_df['final_popularity'] = pd.to_numeric(song_df['final_popularity'], errors='coerce').fillna(50)
        song_df['final_popularity_norm'] = song_df['final_popularity'] / 100.0
        
        # 修复genre_clean
        if 'genre_clean' not in song_df.columns:
            if 'genre' in song_df.columns:
                top_genres = song_df['genre'].value_counts().head(12).index.tolist()
                song_df['genre_clean'] = song_df['genre'].apply(
                    lambda x: x if pd.notna(x) and x in top_genres else 'other'
                )
            else:
                song_df['genre_clean'] = 'unknown'
        
        # 修复音频特征
        audio_cols = ['danceability', 'energy', 'valence', 'tempo', 'loudness',
                      'speechiness', 'acousticness', 'instrumentalness', 'liveness']
        for col in audio_cols:
            if col not in song_df.columns:
                song_df[col] = 0.5
            else:
                song_df[col] = pd.to_numeric(song_df[col], errors='coerce').fillna(0.5)
        
        # 修复衍生特征
        song_df['energy_dance'] = (song_df['danceability'] + song_df['energy']) / 2
        song_df['mood_score'] = (song_df['valence'] * 0.6 + song_df['energy'] * 0.4)
        if 'recency_score' not in song_df.columns:
            if 'song_age' in song_df.columns:
                song_df['recency_score'] = np.exp(-song_df['song_age'].fillna(5) / 15)
            else:
                song_df['recency_score'] = 0.5
        
        # 修复source
        if 'source' not in song_df.columns:
            song_df['source'] = 'internal'
        
        self.song_features = song_df
        self.internal_songs = song_df[song_df['source'] == 'internal'].copy()
        self.external_songs = song_df[song_df['source'] == 'external'].copy()
        print(f"  歌曲特征: {len(song_df):,} (内部{len(self.internal_songs):,}, 外部{len(self.external_songs):,})")
        
        # ----- 2. 用户特征（含字段修复）-----
        user_df = pd.read_sql("SELECT * FROM enhanced_user_features", engine)
        user_df['user_id'] = user_df['user_id'].astype(str)
        
        # 修复数值列
        numeric_cols = ['unique_songs', 'total_interactions', 'total_weight_sum', 'avg_weight',
                        'weight_std', 'popularity_bias', 'avg_popularity_pref', 'diversity_ratio', 'age']
        for col in numeric_cols:
            if col in user_df.columns:
                user_df[col] = pd.to_numeric(user_df[col], errors='coerce').fillna(0)
            else:
                user_df[col] = 0 if col != 'avg_popularity_pref' else 50.0
        
        # 修复source
        if 'source' not in user_df.columns:
            # 根据ID前缀推断（兼容旧数据）
            user_df['source'] = user_df['user_id'].apply(
                lambda x: 'internal' if str(x).startswith('U') and len(str(x)) <= 7 else 'external'
            )
        
        # ----- 3. 交互数据 -----
        interaction_df = pd.read_sql("SELECT * FROM filtered_interactions", engine)
        interaction_df['user_id'] = interaction_df['user_id'].astype(str)
        interaction_df['song_id'] = interaction_df['song_id'].astype(str)
        
        train_df = pd.read_sql("SELECT * FROM train_interactions", engine)
        train_df['user_id'] = train_df['user_id'].astype(str)
        train_df['song_id'] = train_df['song_id'].astype(str)
        
        test_df = pd.read_sql("SELECT * FROM test_interactions", engine)
        test_df['user_id'] = test_df['user_id'].astype(str)
        test_df['song_id'] = test_df['song_id'].astype(str)
        
        # ----- 4. 按来源拆分 -----
        internal_users = set(user_df[user_df['source'] == 'internal']['user_id'])
        external_users = set(user_df[user_df['source'] == 'external']['user_id'])
        
        self.internal_data = {
            'user_features': user_df[user_df['source'] == 'internal'].copy(),
            'interaction_matrix': interaction_df[interaction_df['user_id'].isin(internal_users)].copy(),
            'train_interactions': train_df[train_df['user_id'].isin(internal_users)].copy(),
            'test_interactions': test_df[test_df['user_id'].isin(internal_users)].copy()
        }
        
        self.external_data = {
            'user_features': user_df[user_df['source'] == 'external'].copy(),
            'interaction_matrix': interaction_df[interaction_df['user_id'].isin(external_users)].copy(),
            'train_interactions': train_df[train_df['user_id'].isin(external_users)].copy(),
            'test_interactions': test_df[test_df['user_id'].isin(external_users)].copy()
        }
        
        print(f"\n数据分离统计:")
        print(f"  内部用户: {len(self.internal_data['user_features']):,}  内部交互: {len(self.internal_data['interaction_matrix']):,}")
        print(f"  外部用户: {len(self.external_data['user_features']):,}  外部交互: {len(self.external_data['interaction_matrix']):,}")
        
        return {
            'all_songs': self.song_features,
            'internal': self.internal_data,
            'external': self.external_data
        }


# ---------------------------- 特定来源推荐器（含完整缓存） ----------------------------
# ---------------------------- 特定来源推荐器 ----------------------------
class SourceSpecificRecommender:
    """特定来源推荐器 - 包含所有算法"""
    
    def __init__(self, source_data, song_features, source_type, cache_dir="recommender_cache"):
        self.source_type = source_type
        self.song_features = song_features
        self.user_features = source_data['user_features']
        self.interaction_matrix = source_data['interaction_matrix']
        self.train_interactions = source_data['train_interactions']
        self.test_interactions = source_data['test_interactions']

        # ---------- 字段兼容处理 ----------
        # 1. 确保 final_popularity 存在
        if 'final_popularity' not in self.song_features.columns:
            if 'popularity' in self.song_features.columns:
                self.song_features['final_popularity'] = self.song_features['popularity']
            else:
                self.song_features['final_popularity'] = 50.0
        self.song_features['final_popularity'] = pd.to_numeric(self.song_features['final_popularity'], errors='coerce').fillna(50)

        # 2. 处理 genre_clean
        if 'genre_clean' not in self.song_features.columns:
            if 'genre' in self.song_features.columns:
                self.song_features['genre_clean'] = self.song_features['genre'].fillna('unknown')
            else:
                self.song_features['genre_clean'] = 'unknown'

        # 3. 处理音频特征（默认值）
        audio_cols = ['danceability', 'energy', 'valence', 'tempo', 'loudness', 
                    'speechiness', 'acousticness', 'instrumentalness', 'liveness']
        for col in audio_cols:
            if col not in self.song_features.columns:
                self.song_features[col] = 0.5

        # 4. 处理情感特征（内部使用 avg_sentiment，外部使用 valence）
        if 'avg_sentiment' not in self.song_features.columns:
            if self.source_type == 'internal':
                self.song_features['avg_sentiment'] = 0.5
            else:
                self.song_features['avg_sentiment'] = self.song_features.get('valence', 0.5)

        # 5. 确保 final_popularity_norm 存在
        if 'final_popularity_norm' not in self.song_features.columns:
            self.song_features['final_popularity_norm'] = self.song_features['final_popularity'] / 100.0

        # 6. 确保 recency_score 和 song_age 存在
        if 'recency_score' not in self.song_features.columns:
            self.song_features['recency_score'] = 0.5
        if 'song_age' not in self.song_features.columns:
            self.song_features['song_age'] = 0

        # ---------- 过滤该来源的歌曲 ----------
        self.source_songs = song_features[song_features['source'] == source_type].copy()

        # ---------- 歌曲信息字典缓存 ----------
        self.song_info_dict = {}
        for _, row in self.source_songs.iterrows():
            self.song_info_dict[row['song_id']] = {
                'song_id': row['song_id'],
                'song_name': row.get('song_name', '未知'),
                'artists': row.get('artists', '未知'),
                'genre': row.get('genre_clean', row.get('genre', '未知')),
                'popularity': int(row.get('final_popularity', 50)),
                'source': self.source_type
            }

        # 缓存目录
        self.cache_dir = os.path.join(cache_dir, source_type)
        os.makedirs(self.cache_dir, exist_ok=True)

        print(f"\n初始化{source_type}推荐器...")
        print(f"  用户数: {len(self.user_features):,}")
        print(f"  歌曲数: {len(self.source_songs):,}")
        print(f"  交互数: {len(self.interaction_matrix):,}")

        self.build_matrices()
        self.calculate_matrix_factorization()

        # ---------- 文本 embedding（可选，需在计算内容相似度之前加载）----------
        self.text_embeddings = None
        self.use_text = False
        if TEXT_MODEL_AVAILABLE:
            self._load_text_embeddings()

        self.calculate_similarities()

        # LightFM 模型（可选）
        self.lightfm_model = None
        self.lightfm_user_features = None
        self.lightfm_item_features = None
        self.lightfm_user_mapping = None
        self.lightfm_item_mapping = None
        if LIGHTFM_AVAILABLE:
            self._train_lightfm()
    
    def _prepare_features(self):
        """字段兼容处理（与之前相同）"""
        # 确保 final_popularity 存在
        if 'final_popularity' not in self.song_features.columns:
            if 'popularity' in self.song_features.columns:
                self.song_features['final_popularity'] = self.song_features['popularity']
            else:
                self.song_features['final_popularity'] = 50.0
        self.song_features['final_popularity'] = pd.to_numeric(self.song_features['final_popularity'], errors='coerce').fillna(50)
        
        # 处理 genre_clean
        if 'genre_clean' not in self.song_features.columns:
            if 'genre' in self.song_features.columns:
                self.song_features['genre_clean'] = self.song_features['genre'].fillna('unknown')
            else:
                self.song_features['genre_clean'] = 'unknown'
        
        # 处理音频特征
        audio_cols = ['danceability', 'energy', 'valence', 'tempo', 'loudness', 
                    'speechiness', 'acousticness', 'instrumentalness', 'liveness']
        for col in audio_cols:
            if col not in self.song_features.columns:
                self.song_features[col] = 0.5
        
        # 处理情感特征
        if 'avg_sentiment' not in self.song_features.columns:
            if self.source_type == 'internal':
                self.song_features['avg_sentiment'] = 0.5
            else:
                self.song_features['avg_sentiment'] = self.song_features.get('valence', 0.5)
    
    # ------------------------ 矩阵构建与缓存 ------------------------
    def build_matrices(self):
        """构建用户-歌曲矩阵（带缓存）"""
        cache_file = os.path.join(self.cache_dir, "user_song_matrix.npz")
        mapping_file = os.path.join(self.cache_dir, "mappings.pkl")
        
        if os.path.exists(cache_file) and os.path.exists(mapping_file):
            print(f"  从缓存加载{self.source_type}用户-歌曲矩阵...")
            self.user_song_matrix = load_npz(cache_file)
            with open(mapping_file, 'rb') as f:
                mappings = pickle.load(f)
                self.user_to_idx = mappings['user_to_idx']
                self.idx_to_user = mappings['idx_to_user']
                self.song_to_idx = mappings['song_to_idx']
                self.idx_to_song = mappings['idx_to_song']
        else:
            print(f"  构建{self.source_type}用户-歌曲矩阵...")
            all_users = self.train_interactions['user_id'].unique().tolist()
            all_songs = self.train_interactions['song_id'].unique().tolist()
            
            self.user_to_idx = {u: i for i, u in enumerate(all_users)}
            self.idx_to_user = {i: u for i, u in enumerate(all_users)}
            self.song_to_idx = {s: i for i, s in enumerate(all_songs)}
            self.idx_to_song = {i: s for i, s in enumerate(all_songs)}
            
            rows = [self.user_to_idx[uid] for uid in self.train_interactions['user_id']]
            cols = [self.song_to_idx[sid] for sid in self.train_interactions['song_id']]
            data = self.train_interactions['total_weight'].values
            
            self.user_song_matrix = csr_matrix((data, (rows, cols)),
                                              shape=(len(all_users), len(all_songs)))
            
            save_npz(cache_file, self.user_song_matrix)
            with open(mapping_file, 'wb') as f:
                pickle.dump({
                    'user_to_idx': self.user_to_idx,
                    'idx_to_user': self.idx_to_user,
                    'song_to_idx': self.song_to_idx,
                    'idx_to_song': self.idx_to_song
                }, f)
        
        self.n_users, self.n_songs = self.user_song_matrix.shape
        density = self.user_song_matrix.nnz / (self.n_users * self.n_songs) * 100
        print(f"    矩阵: {self.n_users}x{self.n_songs}, 密度: {density:.4f}%")
    
    # ------------------------ 相似度计算（带缓存） ------------------------
    def calculate_similarities(self):
        self._calculate_popular_songs()
        self._calculate_user_similarities()
        self._calculate_content_similarities()
    
    def _calculate_popular_songs(self):
        """热门歌曲分层"""
        if 'final_popularity' in self.source_songs.columns:
            pop_values = self.source_songs['final_popularity'].values
        else:
            song_counts = self.interaction_matrix.groupby('song_id').size()
            pop_values = song_counts.reindex(self.source_songs['song_id'], fill_value=0).values
        
        try:
            q33 = np.percentile(pop_values[pop_values > 0], 33) if len(pop_values[pop_values > 0]) > 0 else 30
            q67 = np.percentile(pop_values[pop_values > 0], 67) if len(pop_values[pop_values > 0]) > 0 else 70
        except:
            q33, q67 = 30, 70
        
        self.tiered_songs = {
            'hit': self.source_songs[self.source_songs['song_id'].isin(
                self.interaction_matrix['song_id']
            ) & (self.source_songs['final_popularity'] >= q67)]['song_id'].tolist(),
            'popular': self.source_songs[self.source_songs['song_id'].isin(
                self.interaction_matrix['song_id']
            ) & (self.source_songs['final_popularity'] >= q33) & 
            (self.source_songs['final_popularity'] < q67)]['song_id'].tolist(),
            'normal': self.source_songs[self.source_songs['song_id'].isin(
                self.interaction_matrix['song_id']
            ) & (self.source_songs['final_popularity'] < q33)]['song_id'].tolist()
        }
        
        self.song_popularity = {}
        for _, row in self.source_songs.iterrows():
            self.song_popularity[row['song_id']] = row.get('final_popularity', 50)
    
    def _calculate_user_similarities(self, batch_size=500):
        """用户相似度 - 全量计算（MF向量+余弦相似度）并预构建UserCF缓存"""
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity
        import gc
        import time
        import pickle
        import os

        cache_file = os.path.join(self.cache_dir, "user_sim.pkl")
        items_cache_file = os.path.join(self.cache_dir, "user_sim_items.pkl")
        cf_scores_file = os.path.join(self.cache_dir, "user_cf_scores.pkl")

        # 尝试从缓存加载
        if os.path.exists(cache_file) and os.path.exists(items_cache_file) and os.path.exists(cf_scores_file):
            try:
                print(f"    从缓存加载{self.source_type}用户相似度...")
                with open(cache_file, 'rb') as f:
                    self.user_similarities = pickle.load(f)
                with open(items_cache_file, 'rb') as f:
                    self.similar_user_items = pickle.load(f)
                with open(cf_scores_file, 'rb') as f:
                    self.user_cf_scores = pickle.load(f)
                print(f"      加载完成: {len(self.user_similarities)}用户, UserCF缓存: {len(self.similar_user_items)}用户, 预聚合得分: {len(self.user_cf_scores)}用户")
                return
            except Exception as e:
                print(f"    缓存加载失败，重新计算: {e}")

        print(f"    计算{self.source_type}用户相似度（全量）...")
        total_users = self.n_users

        # 确保MF已计算
        if self.user_factors is None:
            print("      先计算MF降维向量...")
            self.calculate_matrix_factorization()

        if self.user_factors is None:
            print("      MF计算失败，跳过用户相似度")
            self.user_similarities = {}
            self.similar_user_items = {}
            self.user_cf_scores = {}
            return

        factors = self.user_factors
        print(f"      使用MF向量({factors.shape[1]}维)，共{total_users}用户")

        # 归一化
        norms = np.linalg.norm(factors, axis=1, keepdims=True)
        norms[norms == 0] = 1
        normalized = factors / norms

        total_batches = (total_users + batch_size - 1) // batch_size
        self.user_similarities = {}
        self.user_cf_scores = {}
        start_time = time.time()

        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, total_users)

            if batch_idx % 10 == 0:
                progress = batch_end / total_users * 100
                elapsed = time.time() - start_time
                eta = (elapsed / (batch_end + 1)) * (total_users - batch_end) if batch_end > 0 else 0
                print(f"\r      批次 {batch_idx+1}/{total_batches} | "
                    f"进度 {batch_end}/{total_users} ({progress:.1f}%) | "
                    f"耗时 {elapsed:.1f}s | 预计剩余 {eta:.1f}s", end="")

            batch_factors = normalized[batch_start:batch_end]
            batch_sim = cosine_similarity(batch_factors, normalized)

            for i in range(batch_end - batch_start):
                global_idx = batch_start + i
                user_id = self.idx_to_user[global_idx]
                scores = batch_sim[i]

                # 获取Top16邻居
                top_indices = np.argpartition(scores, -16)[-16:]
                top_indices_sorted = top_indices[np.argsort(-scores[top_indices])]

                neighbors = {}
                for idx in top_indices_sorted:
                    if idx != global_idx and scores[idx] > 0.05:
                        neighbor_id = self.idx_to_user[idx]
                        neighbors[neighbor_id] = float(scores[idx])
                        if len(neighbors) >= 15:
                            break

                if neighbors:
                    self.user_similarities[user_id] = neighbors

                # 预聚合邻居物品得分
                user_items = set(self.user_song_matrix[global_idx].nonzero()[1])
                agg_scores = {}
                for sim_user, sim_score in neighbors.items():
                    sim_idx = self.user_to_idx[sim_user]
                    row = self.user_song_matrix[sim_idx]
                    for pos, song_idx in enumerate(row.indices):
                        if song_idx not in user_items:
                            weight = row.data[pos]
                            song_id = self.idx_to_song[song_idx]
                            agg_scores[song_id] = agg_scores.get(song_id, 0) + sim_score * weight
                if agg_scores:
                    top_items = sorted(agg_scores.items(), key=lambda x: x[1], reverse=True)[:100]
                    self.user_cf_scores[user_id] = dict(top_items)

            if (batch_idx + 1) % 20 == 0:
                gc.collect()

        print(f"\n      计算完成！共{len(self.user_similarities)}用户有相似邻居，{len(self.user_cf_scores)}用户有预聚合得分")

        # 保存用户相似度缓存
        with open(cache_file, 'wb') as f:
            pickle.dump(self.user_similarities, f)
        print(f"      用户相似度缓存已保存 ({os.path.getsize(cache_file)/1024/1024:.1f}MB)")

        # 构建UserCF缓存
        print("      正在构建UserCF缓存...")
        self.similar_user_items = {}
        count = 0
        total_sim_users = len(self.user_similarities)
        for user_id, sim_dict in self.user_similarities.items():
            user_idx = self.user_to_idx[user_id]
            items = {}
            for sim_id in sim_dict.keys():
                if sim_id in self.user_to_idx:
                    sim_idx = self.user_to_idx[sim_id]
                    row = self.user_song_matrix[sim_idx]
                    items[sim_id] = {
                        'indices': row.indices.copy(),
                        'data': row.data.copy()
                    }
            self.similar_user_items[user_id] = items
            count += 1
            if count % 1000 == 0:
                print(f"        已缓存 {count}/{total_sim_users} 用户...")

        with open(items_cache_file, 'wb') as f:
            pickle.dump(self.similar_user_items, f)
        print(f"      UserCF缓存已保存 ({os.path.getsize(items_cache_file)/1024/1024:.1f}MB)，覆盖 {len(self.similar_user_items)} 用户")

        with open(cf_scores_file, 'wb') as f:
            pickle.dump(self.user_cf_scores, f)
        print(f"      预聚合得分缓存已保存 ({os.path.getsize(cf_scores_file)/1024/1024:.1f}MB)，覆盖 {len(self.user_cf_scores)} 用户")
    
    def _calculate_content_similarities(self):
        # 融合音频 + 文本特征（加权）
        cache_file = os.path.join(self.cache_dir, "content_sim.pkl")
        
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                self.content_similarities = pickle.load(f)
            print(f"    从缓存加载{self.source_type}内容相似度...")
            return
        
        print(f"    计算{self.source_type}内容相似度...")
        
        audio_features = ['danceability', 'energy', 'valence', 'tempo', 
                        'loudness', 'speechiness', 'acousticness',
                        'instrumentalness', 'liveness']
        extra_features = ['final_popularity_norm', 'recency_score', 'song_age']
        
        available_audio = [f for f in audio_features if f in self.source_songs.columns]
        available_extra = [f for f in extra_features if f in self.source_songs.columns]
        
        X_audio = self.source_songs[available_audio].fillna(0.5).values if available_audio else np.zeros((len(self.source_songs), 0))
        X_extra = self.source_songs[available_extra].fillna(0.5).values if available_extra else np.zeros((len(self.source_songs), 0))
        
        if X_audio.shape[1] > 0:
            scaler = RobustScaler()
            X_audio_scaled = scaler.fit_transform(X_audio)
        else:
            X_audio_scaled = np.zeros((len(self.source_songs), 0))
        
        # 文本特征加权
        audio_weight = 0.3
        extra_weight = 0.1
        
        if self.use_text and self.text_embeddings is not None:
            scaler_text = RobustScaler()
            X_text_scaled = scaler_text.fit_transform(self.text_embeddings)
            text_weight = 0.6
            X_combined = np.hstack([
                X_audio_scaled * audio_weight,
                X_text_scaled * text_weight,
                X_extra * extra_weight
            ])
        else:
            X_combined = np.hstack([X_audio_scaled * audio_weight, X_extra * extra_weight])
        
        song_ids = self.source_songs['song_id'].tolist()
        n_neighbors = min(31, len(song_ids))
        nn = NearestNeighbors(n_neighbors=n_neighbors, metric='euclidean', algorithm='ball_tree')
        nn.fit(X_combined)
        distances, indices = nn.kneighbors(X_combined)
        
        self.content_similarities = {}
        for i, song_id in enumerate(song_ids):
            sims = {}
            for j in range(1, len(indices[i])):
                neighbor = song_ids[indices[i][j]]
                sim = np.exp(-distances[i][j] ** 2 / 2)
                if sim > 0.1:
                    sims[neighbor] = sim
            if sims:
                self.content_similarities[song_id] = dict(sorted(sims.items(), key=lambda x: x[1], reverse=True)[:20])
        
        with open(cache_file, 'wb') as f:
            pickle.dump(self.content_similarities, f)
        
        avg_neighbors = np.mean([len(v) for v in self.content_similarities.values()]) if self.content_similarities else 0
        print(f"      计算完成: {len(self.content_similarities)}歌曲，平均{avg_neighbors:.1f}个邻居")
    
    def _load_text_embeddings(self):
        """加载或生成文本 embedding"""
        cache_file = os.path.join(self.cache_dir, "text_embeddings.npy")
        
        if os.path.exists(cache_file):
            print(f"    从缓存加载{self.source_type}文本embedding...")
            self.text_embeddings = np.load(cache_file)
            self.use_text = True
            return
        
        print(f"    生成{self.source_type}文本embedding（使用Sentence-BERT）...")
        try:
            model = SentenceTransformer('all-MiniLM-L6-v2')
            texts = (self.source_songs['song_name'].fillna('') + ' ' + self.source_songs['artists'].fillna('')).tolist()
            batch_size = 256
            embeddings = []
            for i in range(0, len(texts), batch_size):
                batch = texts[i:i+batch_size]
                emb = model.encode(batch, show_progress_bar=False)
                embeddings.append(emb)
            self.text_embeddings = np.vstack(embeddings)
            np.save(cache_file, self.text_embeddings)
            self.use_text = True
            print(f"      文本embedding生成完成，维度{self.text_embeddings.shape[1]}")
        except Exception as e:
            print(f"      文本embedding生成失败: {e}，将仅使用音频特征")
            self.use_text = False
            self.text_embeddings = None
    
    # ------------------------ 矩阵分解（MF，带缓存 + Faiss）-----------------------
    def calculate_matrix_factorization(self):
        """矩阵分解（SVD，带缓存）"""
        cache_file = os.path.join(self.cache_dir, "mf.pkl")
        faiss_index_file = os.path.join(self.cache_dir, "faiss.index")
        
        if os.path.exists(cache_file):
            print(f"    从缓存加载{self.source_type}矩阵分解...")
            with open(cache_file, 'rb') as f:
                d = pickle.load(f)
                self.user_factors = d['user_factors']
                self.song_factors = d['song_factors']
        else:
            print(f"    计算{self.source_type}矩阵分解...")
            n_components = min(50, self.user_song_matrix.shape[1] - 1)
            if n_components < 10:
                self.user_factors = None
                self.song_factors = None
                return
            
            try:
                svd = TruncatedSVD(n_components=n_components, random_state=42)
                self.user_factors = svd.fit_transform(self.user_song_matrix)
                self.song_factors = svd.components_.T
                with open(cache_file, 'wb') as f:
                    pickle.dump({'user_factors': self.user_factors, 'song_factors': self.song_factors}, f)
                print(f"      MF完成，维度: {self.user_factors.shape[1]}, 解释方差: {svd.explained_variance_ratio_.sum():.4f}")
            except Exception as e:
                print(f"      MF计算失败: {e}")
                self.user_factors = None
                self.song_factors = None
                return
        
        # 构建Faiss索引（如果可用）
        self.faiss_index = None
        if FAISS_AVAILABLE and self.song_factors is not None:
            if os.path.exists(faiss_index_file):
                try:
                    self.faiss_index = faiss.read_index(faiss_index_file)
                    print(f"      从缓存加载Faiss索引")
                except:
                    pass
            else:
                print(f"      构建Faiss索引...")
                norms = np.linalg.norm(self.song_factors, axis=1, keepdims=True)
                norms[norms == 0] = 1
                normalized = (self.song_factors / norms).astype(np.float32)
                d = normalized.shape[1]
                self.faiss_index = faiss.IndexFlatIP(d)
                self.faiss_index.add(normalized)
                faiss.write_index(self.faiss_index, faiss_index_file)
                print(f"      Faiss索引构建完成，包含{self.faiss_index.ntotal}个向量")
    
    # ------------------------ 核心推荐算法 ------------------------
    def item_based_cf(self, user_id, n=20):
        """基于物品的协同过滤"""
        if user_id not in self.user_to_idx:
            return []
        user_idx = self.user_to_idx[user_id]
        liked_songs = self.user_song_matrix[user_idx].nonzero()[1]
        if len(liked_songs) == 0:
            return []
        
        scores = {}
        for song_idx in liked_songs:
            other_users = self.user_song_matrix[:, song_idx].nonzero()[0]
            for other_user_idx in other_users:
                other_songs = self.user_song_matrix[other_user_idx].nonzero()[1]
                for other_song_idx in other_songs:
                    if other_song_idx not in liked_songs:
                        other_song_id = self.idx_to_song[other_song_idx]
                        scores[other_song_id] = scores.get(other_song_id, 0) + 1
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def user_based_cf(self, user_id, n=20):
        if user_id not in self.user_cf_scores:
            return []
        scores = self.user_cf_scores[user_id]
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def content_based(self, user_id, n=20):
        """基于内容的推荐"""
        if user_id not in self.user_to_idx:
            return []
        user_idx = self.user_to_idx[user_id]
        interacted = list(self.user_song_matrix[user_idx].nonzero()[1])
        if not interacted:
            return []
        
        scores = {}
        interacted_sample = interacted[:30] if len(interacted) > 30 else interacted
        for song_idx in interacted_sample:
            song_id = self.idx_to_song[song_idx]
            similar = self.content_similarities.get(song_id, {})
            for sim_song, sim_score in similar.items():
                scores[sim_song] = scores.get(sim_song, 0) + sim_score
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def matrix_factorization_rec(self, user_id, n=20):
        """矩阵分解推荐 - 使用Faiss加速（若可用）"""
        if self.user_factors is None or user_id not in self.user_to_idx:
            return []
        
        user_idx = self.user_to_idx[user_id]
        user_vec = self.user_factors[user_idx]
        interacted = set(self.user_song_matrix[user_idx].nonzero()[1])
        
        if self.faiss_index is not None:
            norm = np.linalg.norm(user_vec)
            if norm == 0:
                return []
            user_vec_norm = (user_vec / norm).astype(np.float32).reshape(1, -1)
            k = min(n + 50, self.faiss_index.ntotal)
            scores, indices = self.faiss_index.search(user_vec_norm, k)
            result = []
            for score, idx in zip(scores[0], indices[0]):
                if idx not in interacted:
                    song_id = self.idx_to_song[idx]
                    result.append((song_id, float(score)))
                    if len(result) >= n:
                        break
            return result
        else:
            all_scores = np.dot(self.song_factors, user_vec)
            scores = {}
            for song_id, song_idx in self.song_to_idx.items():
                if song_idx not in interacted and all_scores[song_idx] > 0:
                    scores[song_id] = all_scores[song_idx]
            if scores:
                max_score = max(scores.values())
                scores = {k: v/max_score for k, v in scores.items()}
            return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def sentiment_based_rec(self, user_id, n=20):
        """基于用户历史歌曲的情感偏好进行推荐（仅内部）"""
        if self.source_type != 'internal' or 'avg_sentiment' not in self.source_songs.columns:
            return []
        
        if user_id not in self.user_to_idx:
            return []
        
        user_idx = self.user_to_idx[user_id]
        interacted = list(self.user_song_matrix[user_idx].nonzero()[1])
        if len(interacted) == 0:
            return []
        
        # 计算用户历史歌曲的平均情感分数
        user_sentiment = 0.5
        count = 0
        weights = self.user_song_matrix[user_idx].data
        indices = self.user_song_matrix[user_idx].indices
        if len(weights) > 0:
            top_k = min(30, len(weights))
            top_indices = np.argsort(weights)[-top_k:]
            for idx_pos in top_indices:
                song_idx = indices[idx_pos]
                song_id = self.idx_to_song.get(song_idx)
                if song_id:
                    row = self.source_songs[self.source_songs['song_id'] == song_id]
                    if not row.empty and 'avg_sentiment' in row.columns:
                        sent = row.iloc[0]['avg_sentiment']
                        if pd.notna(sent):
                            user_sentiment += sent
                            count += 1
        if count > 0:
            user_sentiment /= count
        else:
            return []
        
        scores = {}
        for _, row in self.source_songs.iterrows():
            song_id = row['song_id']
            if song_id in self.song_to_idx and self.song_to_idx[song_id] not in interacted:
                song_sent = row.get('avg_sentiment', 0.5)
                if pd.notna(song_sent):
                    sim = 1 - abs(user_sentiment - song_sent)
                    if sim > 0.6:
                        scores[song_id] = sim
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    def artist_based_rec(self, user_id, n=20):
        """基于艺术家相似度推荐"""
        if user_id not in self.user_to_idx:
            return []
        user_idx = self.user_to_idx[user_id]
        interacted = list(self.user_song_matrix[user_idx].nonzero()[1])
        if not interacted:
            return []
        
        artists = []
        for song_idx in interacted:
            song_id = self.idx_to_song[song_idx]
            info = self.get_song_info(song_id)
            if info and info.get('artists') and info['artists'] != '未知':
                artists.append(info['artists'])
        if not artists:
            return []
        
        top_artists = [a for a, _ in Counter(artists).most_common(3)]
        scores = {}
        for artist in top_artists:
            artist_songs = self.source_songs[self.source_songs['artists'] == artist]['song_id'].tolist()
            for sid in artist_songs:
                if sid in self.song_to_idx and self.song_to_idx[sid] not in interacted:
                    scores[sid] = scores.get(sid, 0) + 1
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    
    # ------------------------ LightFM 模型 ------------------------
    def _train_lightfm(self):
        """训练 LightFM 模型（带缓存，支持用户/物品特征）"""
        cache_file = os.path.join(self.cache_dir, "lightfm.pkl")
        
        if os.path.exists(cache_file):
            print(f"    从缓存加载{self.source_type} LightFM模型...")
            with open(cache_file, 'rb') as f:
                d = pickle.load(f)
                self.lightfm_model = d['model']
                self.lightfm_user_mapping = d['user_mapping']
                self.lightfm_item_mapping = d['item_mapping']
                self.lightfm_user_features = d.get('user_features', None)
                self.lightfm_item_features = d.get('item_features', None)
            return
        
        # ===== 是否启用 LightFM（设为 False 可暂时跳过）=====
        ENABLE_LIGHTFM = False   # 改为 True 启用，但训练可能很慢
        
        if not ENABLE_LIGHTFM:
            print(f"    跳过{self.source_type} LightFM训练（未启用）")
            self.lightfm_model = None
            self.lightfm_user_mapping = None
            self.lightfm_item_mapping = None
            self.lightfm_user_features = None
            self.lightfm_item_features = None
            return
        
        print(f"    训练{self.source_type} LightFM模型（带特征，快速模式）...")
        
        # ===== 快速参数设置（可调）=====
        no_components = 10      # 降维
        epochs = 3              # 只训练 3 轮
        loss = 'warp'
        num_threads = 4
        verbose = True
        
        # ===== 准备用户特征 =====
        user_feature_cols = ['age', 'gender', 'province', 'city', 'activity_level', 'popularity_bias']
        existing_user_cols = [col for col in user_feature_cols if col in self.user_features.columns]
        if not existing_user_cols:
            print("      警告：未找到任何用户特征列，将只使用交互数据")
            USE_FEATURES = False
        else:
            user_features_df = self.user_features.set_index('user_id')[existing_user_cols].fillna(0)
            from sklearn.preprocessing import LabelEncoder
            for col in ['gender', 'province', 'city', 'activity_level']:
                if col in existing_user_cols:
                    le = LabelEncoder()
                    user_features_df[col] = le.fit_transform(user_features_df[col].astype(str))
        
        # ===== 准备物品特征 =====
        item_feature_cols = ['danceability', 'energy', 'valence', 'tempo', 'loudness',
                            'speechiness', 'acousticness', 'instrumentalness', 'liveness',
                            'final_popularity_norm', 'recency_score', 'song_age', 'genre_clean']
        existing_item_cols = [col for col in item_feature_cols if col in self.source_songs.columns]
        if not existing_item_cols:
            print("      警告：未找到任何物品特征列，将只使用交互数据")
            USE_FEATURES = False
        else:
            item_features_df = self.source_songs.set_index('song_id')[existing_item_cols].fillna(0)
            if 'genre_clean' in existing_item_cols:
                le_genre = LabelEncoder()
                item_features_df['genre_clean'] = le_genre.fit_transform(item_features_df['genre_clean'].astype(str))
        
        USE_FEATURES = True  # 如果希望强制使用特征，请确保以上数据准备成功
        
        if not USE_FEATURES:
            # 回退到无特征模式
            print("    回退到无特征 LightFM 训练...")
            dataset = Dataset()
            dataset.fit(
                self.train_interactions['user_id'].unique(),
                self.train_interactions['song_id'].unique()
            )
            (interactions, weights) = dataset.build_interactions(
                self.train_interactions[['user_id', 'song_id', 'total_weight']].values
            )
            model = LightFM(no_components=no_components, loss=loss, random_state=42)
            model.fit(interactions, epochs=epochs, num_threads=num_threads, verbose=verbose)
            
            self.lightfm_model = model
            self.lightfm_user_mapping = dataset.mapping()[0]
            self.lightfm_item_mapping = dataset.mapping()[2]
            self.lightfm_user_features = None
            self.lightfm_item_features = None
            
            with open(cache_file, 'wb') as f:
                pickle.dump({
                    'model': model,
                    'user_mapping': self.lightfm_user_mapping,
                    'item_mapping': self.lightfm_item_mapping,
                }, f)
            print(f"      LightFM模型训练完成（无特征），保存至缓存")
            return
        
        # ===== 使用特征的版本 =====
        train_users = set(self.train_interactions['user_id'].unique())
        train_items = set(self.train_interactions['song_id'].unique())
        
        user_features_df = user_features_df[user_features_df.index.isin(train_users)]
        item_features_df = item_features_df[item_features_df.index.isin(train_items)]
        
        dataset = Dataset()
        dataset.fit(
            train_users,
            train_items,
            user_features=user_features_df.columns.tolist(),
            item_features=item_features_df.columns.tolist()
        )
        
        (interactions, weights) = dataset.build_interactions(
            self.train_interactions[['user_id', 'song_id', 'total_weight']].values
        )
        
        user_features = dataset.build_user_features(
            ((uid, user_features_df.loc[uid].to_dict()) for uid in user_features_df.index)
        )
        item_features = dataset.build_item_features(
            ((iid, item_features_df.loc[iid].to_dict()) for iid in item_features_df.index)
        )
        
        model = LightFM(no_components=no_components, loss=loss, random_state=42)
        model.fit(interactions, user_features=user_features, item_features=item_features,
                epochs=epochs, num_threads=num_threads, verbose=verbose)
        
        self.lightfm_model = model
        self.lightfm_user_mapping = dataset.mapping()[0]
        self.lightfm_item_mapping = dataset.mapping()[2]
        self.lightfm_user_features = user_features
        self.lightfm_item_features = item_features
        
        with open(cache_file, 'wb') as f:
            pickle.dump({
                'model': model,
                'user_mapping': self.lightfm_user_mapping,
                'item_mapping': self.lightfm_item_mapping,
                'user_features': user_features,
                'item_features': item_features
            }, f)
        print(f"      LightFM模型训练完成（带特征），保存至缓存")
    
    def lightfm_rec(self, user_id, n=20):
        """使用 LightFM 模型推荐"""
        if self.lightfm_model is None or user_id not in self.lightfm_user_mapping:
            return []
        user_idx = self.lightfm_user_mapping.get(user_id)
        if user_idx is None:
            return []
        
        # 获取已交互歌曲
        if user_id in self.user_to_idx:
            interacted = set(self.user_song_matrix[self.user_to_idx[user_id]].nonzero()[1])
        else:
            interacted = set()
        
        # 对所有物品预测得分
        n_items = len(self.lightfm_item_mapping)
        item_indices = list(range(n_items))
        scores = self.lightfm_model.predict(user_idx, item_indices,
                                            user_features=self.lightfm_user_features,
                                            item_features=self.lightfm_item_features)
        
        # 排序并过滤已交互
        item_score_pairs = [(self._idx_to_item_id(i), scores[i]) for i in item_indices]
        item_score_pairs = [(iid, s) for iid, s in item_score_pairs if iid in self.song_to_idx and self.song_to_idx[iid] not in interacted]
        item_score_pairs.sort(key=lambda x: x[1], reverse=True)
        return item_score_pairs[:n]
    
    def _idx_to_item_id(self, idx):
        """根据 LightFM 内部索引获取歌曲ID（逆向映射）"""
        for iid, i in self.lightfm_item_mapping.items():
            if i == idx:
                return iid
        return None
    
    # ------------------------ 冷启动与MMR ------------------------
    def get_cold_start_recs(self, profile=None, n=10):
        # 优先使用用户画像
        if profile and profile.get('top_genres'):
            hot_songs = self.source_songs[
                self.source_songs['song_id'].isin(self.tiered_songs.get('hit', []) + self.tiered_songs.get('popular', []))
            ]
            candidates = []
            for genre in profile['top_genres']:
                genre_songs = hot_songs[hot_songs['genre_clean'] == genre]['song_id'].tolist()
                candidates.extend(genre_songs)
            if candidates:
                candidates = list(dict.fromkeys(candidates))
                selected = random.sample(candidates, min(n, len(candidates)))
                return [(sid, 0.5) for sid in selected]
        
        # 无画像：从热门中按比例抽取
        hit_songs = self.tiered_songs.get('hit', [])
        pop_songs = self.tiered_songs.get('popular', [])
        if not hit_songs and not pop_songs:
            return []
        total = len(hit_songs) + len(pop_songs)
        n_hit = min(int(n * 0.6), len(hit_songs))
        n_pop = min(n - n_hit, len(pop_songs))
        if n_hit + n_pop < n:
            extra = n - (n_hit + n_pop)
            if n_hit < len(hit_songs):
                n_hit = min(n_hit + extra, len(hit_songs))
            else:
                n_pop = min(n_pop + extra, len(pop_songs))
        selected = random.sample(hit_songs, n_hit) + random.sample(pop_songs, n_pop)
        random.shuffle(selected)
        return [(sid, 0.5) for sid in selected]
    
    def mmr_rerank(self, candidates, user_id, n=10, lambda_=0.6):
        """MMR多样性重排"""
        if len(candidates) <= n:
            return candidates
        
        selected = [candidates[0]]
        remaining = candidates[1:]
        
        while len(selected) < n and remaining:
            best_score = -float('inf')
            best_idx = -1
            for i, (song_id, relevance) in enumerate(remaining):
                max_sim = 0
                for sel_id, _ in selected:
                    content_sim = self.content_similarities.get(song_id, {}).get(sel_id, 0)
                    genre_sim = 0.5 if self.get_song_info(sel_id) and self.get_song_info(song_id) and \
                        self.get_song_info(sel_id).get('genre') == self.get_song_info(song_id).get('genre') else 0
                    max_sim = max(max_sim, content_sim, genre_sim)
                mmr_score = lambda_ * relevance - (1 - lambda_) * max_sim
                if mmr_score > best_score:
                    best_score = mmr_score
                    best_idx = i
            if best_idx >= 0:
                selected.append(remaining.pop(best_idx))
        return selected
    
    # ------------------------ 混合推荐（全算法保留）-----------------------
    def hybrid_recommendation_parallel(self, user_id, n=10, use_mmr=True,
                                       w_itemcf=0.15, w_usercf=0.0, w_content=0.15,
                                       w_mf=0.25, w_sentiment=0.1, w_artist=0.1, w_lightfm=0.15):
        """
        并行混合推荐（7种算法）
        - w_artist 和 w_lightfm 固定，其余5个可调优
        """
        # 外部用户禁用 sentiment
        if self.source_type != 'internal':
            w_sentiment = 0.0
            # 重新归一化
            total = w_itemcf + w_usercf + w_content + w_mf + w_artist + w_lightfm
            w_itemcf /= total
            w_usercf /= total
            w_content /= total
            w_mf /= total
            w_artist /= total
            w_lightfm /= total
        
        all_scores = {}
        recall_k = n * 5  # 可调
        
        tasks = {
            'itemcf': lambda: self.item_based_cf(user_id, n=recall_k),
            'usercf': lambda: self.user_based_cf(user_id, n=recall_k),
            'content': lambda: self.content_based(user_id, n=recall_k),
            'mf': lambda: self.matrix_factorization_rec(user_id, n=recall_k),
            'artist': lambda: self.artist_based_rec(user_id, n=recall_k),
            'lightfm': lambda: self.lightfm_rec(user_id, n=recall_k)
        }
        if w_sentiment > 0:
            tasks['sentiment'] = lambda: self.sentiment_based_rec(user_id, n=recall_k)
        
        weight_map = {
            'itemcf': w_itemcf,
            'usercf': w_usercf,
            'content': w_content,
            'mf': w_mf,
            'sentiment': w_sentiment,
            'artist': w_artist,
            'lightfm': w_lightfm
        }
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_algo = {executor.submit(func): name for name, func in tasks.items()}
            for future in as_completed(future_to_algo):
                algo = future_to_algo[future]
                try:
                    recs = future.result()
                    if recs:
                        max_score = max(r[1] for r in recs)
                        weight = weight_map[algo]
                        for sid, score in recs:
                            all_scores[sid] = all_scores.get(sid, 0) + (score / max_score) * weight
                except Exception as e:
                    print(f"      并行任务 {algo} 失败: {e}")
        
        if not all_scores:
            return self.get_cold_start_recs(self.get_user_profile(user_id), n)
        
        # 流行度惩罚
        penalty_factor = 0.1
        for sid in list(all_scores.keys()):
            pop = self.song_popularity.get(sid, 50) / 100.0
            all_scores[sid] -= penalty_factor * pop
        
        candidates = sorted(all_scores.items(), key=lambda x: x[1], reverse=True)
        if candidates and candidates[0][1] < 0.3:
            hot_songs = self.tiered_songs.get('hit', []) + self.tiered_songs.get('popular', [])
            existing = {sid for sid, _ in candidates}
            for sid in hot_songs:
                if sid not in existing:
                    candidates.append((sid, 0.2))
                    existing.add(sid)
                    if len(candidates) >= n * 2:
                        break
            candidates.sort(key=lambda x: x[1], reverse=True)
        
        if use_mmr:
            return self.mmr_rerank(candidates, user_id, n)
        else:
            return candidates[:n]
    
    # ------------------------ 权重调优（仅针对5个可变算法）-----------------------
    def tune_weights(self, val_users, n=10, metric='ndcg'):
        """在验证集上搜索最优权重组合（ItemCF, UserCF, Content, MF, Sentiment），
           Artist 和 LightFM 权重固定为 0.1"""
        best_score = -1
        best_weights = None
        # 固定权重
        w_artist_fixed = 0.1
        w_lightfm_fixed = 0.1
        # 剩余 0.8 分配给 5 个算法
        for w_item in np.arange(0.0, 0.5, 0.1):
            for w_user in np.arange(0.0, 0.4, 0.1):
                for w_cont in np.arange(0.0, 0.4, 0.1):
                    for w_mf in np.arange(0.0, 0.5, 0.1):
                        for w_sent in np.arange(0.0, 0.3, 0.1):
                            total = w_item + w_user + w_cont + w_mf + w_sent
                            if abs(total - 0.8) > 0.01:
                                continue
                            if self.source_type != 'internal':
                                w_sent = 0.0
                                total = w_item + w_user + w_cont + w_mf
                                if abs(total - 0.8) > 0.01:
                                    continue
                            # 评估
                            score = self._eval_weights(val_users, w_item, w_user, w_cont, w_mf, w_sent,
                                                        w_artist_fixed, w_lightfm_fixed, n, metric)
                            if score > best_score:
                                best_score = score
                                best_weights = (w_item, w_user, w_cont, w_mf, w_sent)
                                print(f"      新最佳: {best_weights} -> {metric}={score:.4f}")
        print(f"    权重调优完成，最佳: {best_weights} (最佳{metric}={best_score:.4f})")
        return best_weights
    
    def _eval_weights(self, val_users, w_item, w_user, w_cont, w_mf, w_sent,
                      w_artist, w_lightfm, n=10, metric='ndcg'):
        scores = []
        for uid in val_users:
            test_songs = set(self.test_interactions[self.test_interactions['user_id'] == uid]['song_id'].tolist())
            if not test_songs:
                continue
            recs = self.hybrid_recommendation_parallel(uid, n=n, use_mmr=False,
                                                        w_itemcf=w_item, w_usercf=w_user,
                                                        w_content=w_cont, w_mf=w_mf,
                                                        w_sentiment=w_sent,
                                                        w_artist=w_artist,
                                                        w_lightfm=w_lightfm)
            if not recs:
                continue
            rec_songs = [r[0] for r in recs]
            if metric == 'ndcg':
                dcg = sum(1 / np.log2(idx+2) for idx, song in enumerate(rec_songs) if song in test_songs)
                idcg = sum(1 / np.log2(idx+2) for idx in range(min(len(test_songs), n)))
                score = dcg / idcg if idcg > 0 else 0
            elif metric == 'precision':
                hits = len(set(rec_songs) & test_songs)
                score = hits / n
            elif metric == 'hitrate':
                score = 1 if len(set(rec_songs) & test_songs) > 0 else 0
            else:
                score = 0
            scores.append(score)
        return np.mean(scores) if scores else 0
    
    # ------------------------ 辅助方法 ------------------------
    def get_user_history(self, user_id, n=5):
        """获取用户历史"""
        if user_id not in self.user_to_idx:
            return []
        user_idx = self.user_to_idx[user_id]
        interacted = self.user_song_matrix[user_idx].nonzero()[1]
        if len(interacted) == 0:
            return []
        weights = self.user_song_matrix[user_idx].data
        indices = self.user_song_matrix[user_idx].indices
        if len(weights) > 0:
            top_n = min(n, len(weights))
            top_indices = np.argsort(weights)[-top_n:][::-1]
            top_song_indices = indices[top_indices]
            history = []
            for idx in top_song_indices:
                song_id = self.idx_to_song.get(idx)
                if song_id:
                    info = self.get_song_info(song_id)
                    if info:
                        history.append(info)
            return history
        return []
    
    def get_song_info(self, song_id):
        """获取歌曲信息（字典快速查找）"""
        return self.song_info_dict.get(song_id)
    
    def get_user_profile(self, user_id):
        """获取用户画像"""
        if user_id not in self.user_features['user_id'].values:
            return None
        row = self.user_features[self.user_features['user_id'] == user_id].iloc[0]
        return {
            'user_id': user_id,
            'n_songs': int(row.get('unique_songs', 0)),
            'total_interactions': int(row.get('total_interactions', 0)),
            'top_genres': [row.get(f'top_genre_{i}') for i in range(1, 4) 
                        if pd.notna(row.get(f'top_genre_{i}'))],
            'popularity_bias': float(row.get('popularity_bias', 0)),
            'avg_popularity': float(row.get('avg_popularity_pref', 50)),
            'source': self.source_type
        }
    def artist_based_rec(self, user_id, n=20):
        """基于艺术家相似度推荐"""
        if user_id not in self.user_to_idx:
            return []
        user_idx = self.user_to_idx[user_id]
        interacted = list(self.user_song_matrix[user_idx].nonzero()[1])
        if not interacted:
            return []
        
        # 获取用户历史歌曲的艺术家
        artists = []
        for song_idx in interacted:
            song_id = self.idx_to_song[song_idx]
            info = self.get_song_info(song_id)
            if info and info.get('artists') and info['artists'] != '未知':
                artists.append(info['artists'])
        if not artists:
            return []
        
        # 统计出现次数最多的艺术家（取前3）
        from collections import Counter
        top_artists = [a for a, _ in Counter(artists).most_common(3)]
        
        # 找出这些艺术家的其他歌曲（未听过的）
        scores = {}
        for artist in top_artists:
            artist_songs = self.source_songs[self.source_songs['artists'] == artist]['song_id'].tolist()
            for sid in artist_songs:
                if sid in self.song_to_idx and self.song_to_idx[sid] not in interacted:
                    scores[sid] = scores.get(sid, 0) + 1
        
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]


# ---------------------------- 分离式推荐系统主类 ----------------------------
class SeparatedMusicRecommender:
    """分离式音乐推荐系统（全算法保留 + 优化）"""
    
    def __init__(self, data_dir="separated_processed_data", cache_dir="recommender_cache"):
        self.data_dir = data_dir
        self.cache_dir = cache_dir
        
        data_loader = SeparatedDataLoader(data_dir)
        all_data = data_loader.load_all_data()
        
        self.internal_recommender = SourceSpecificRecommender(
            all_data['internal'], all_data['all_songs'], 'internal', 
            cache_dir=os.path.join(cache_dir, 'internal')
        )
        
        self.external_recommender = SourceSpecificRecommender(
            all_data['external'], all_data['all_songs'], 'external',
            cache_dir=os.path.join(cache_dir, 'external')
        )
        
        self._load_cross_popular_songs()
        
        print("\n" + "="*80)
        print("分离式推荐系统初始化完成！")
        print("="*80)
    
    def _load_cross_popular_songs(self):
        """加载交叉热门歌曲"""
        internal_hits = self.internal_recommender.tiered_songs.get('hit', [])
        internal_popular = self.internal_recommender.tiered_songs.get('popular', [])
        external_hits = self.external_recommender.tiered_songs.get('hit', [])
        external_popular = self.external_recommender.tiered_songs.get('popular', [])
        
        self.cross_popular_songs = {
            'internal_to_external': random.sample(
                external_hits[:50] + external_popular[:30], 
                min(30, len(external_hits) + len(external_popular))
            ),
            'external_to_internal': random.sample(
                internal_hits[:50] + internal_popular[:30],
                min(30, len(internal_hits) + len(internal_popular))
            )
        }
        print(f"交叉热门歌曲加载完成:")
        print(f"  内部→外部补充歌曲: {len(self.cross_popular_songs['internal_to_external'])}")
        print(f"  外部→内部补充歌曲: {len(self.cross_popular_songs['external_to_internal'])}")
    
    def get_user_type(self, user_id):
        """判断用户类型"""
        user_id_str = str(user_id)
        if user_id_str in self.internal_recommender.user_features['user_id'].values:
            row = self.internal_recommender.user_features[self.internal_recommender.user_features['user_id'] == user_id_str]
            if not row.empty:
                return row.iloc[0].get('source', 'internal')
        if user_id_str in self.external_recommender.user_features['user_id'].values:
            row = self.external_recommender.user_features[self.external_recommender.user_features['user_id'] == user_id_str]
            if not row.empty:
                return row.iloc[0].get('source', 'external')
        return 'internal'
    
    def recommend(self, user_id, n=10, use_cross_supplement=True):
        user_type = self.get_user_type(user_id)
        if user_type == 'internal':
            return self._recommend_for_internal_user(
                user_id, n, use_cross_supplement,
                w_itemcf=0.15, w_usercf=0.0, w_content=0.15,
                w_mf=0.25, w_sentiment=0.1, w_artist=0.1, w_lightfm=0.15
            )
        else:
            return self._recommend_for_external_user(
                user_id, n, use_cross_supplement,
                w_itemcf=0.2, w_usercf=0.0, w_content=0.15,
                w_mf=0.3, w_sentiment=0.0, w_artist=0.1, w_lightfm=0.15
            )
    
    def _recommend_for_internal_user(self, user_id, n, use_cross_supplement,
                                      w_itemcf, w_usercf, w_content, w_mf, w_sentiment, w_artist, w_lightfm):
        main_recs = self.internal_recommender.hybrid_recommendation_parallel(
            user_id, n=int(n*0.7), use_mmr=True,
            w_itemcf=w_itemcf, w_usercf=w_usercf,
            w_content=w_content, w_mf=w_mf,
            w_sentiment=w_sentiment, w_artist=w_artist,
            w_lightfm=w_lightfm
        )
        if use_cross_supplement and len(main_recs) < n:
            supplement = self.cross_popular_songs['internal_to_external']
            existing = {r[0] for r in main_recs}
            added = 0
            for song_id in supplement:
                if song_id not in existing:
                    song_info = self.external_recommender.get_song_info(song_id)
                    if song_info:
                        score = self.external_recommender.song_popularity.get(song_id, 50) / 100 * 0.3
                        main_recs.append((song_id, score))
                        existing.add(song_id)
                        added += 1
                        if added >= n - len(main_recs):
                            break
            main_recs.sort(key=lambda x: x[1], reverse=True)
        return main_recs[:n]
    
    def _recommend_for_external_user(self, user_id, n, use_cross_supplement,
                                    w_itemcf, w_usercf, w_content, w_mf, w_sentiment, w_artist, w_lightfm):
        # 外部用户推荐
        main_recs = self.external_recommender.hybrid_recommendation_parallel(
            user_id, n=int(n*0.7), use_mmr=True,
            w_itemcf=w_itemcf, w_usercf=w_usercf,
            w_content=w_content, w_mf=w_mf,
            w_sentiment=w_sentiment, w_artist=w_artist,
            w_lightfm=w_lightfm
        )
        if use_cross_supplement and len(main_recs) < n:
            supplement = self.cross_popular_songs['external_to_internal']
            existing = {r[0] for r in main_recs}
            added = 0
            for song_id in supplement:
                if song_id not in existing:
                    song_info = self.internal_recommender.get_song_info(song_id)
                    if song_info:
                        score = self.internal_recommender.song_popularity.get(song_id, 50) / 100 * 0.3
                        main_recs.append((song_id, score))
                        existing.add(song_id)
                        added += 1
                        if added >= n - len(main_recs):
                            break
            main_recs.sort(key=lambda x: x[1], reverse=True)
        return main_recs[:n]
    
    def get_song_info(self, song_id):
        """跨源获取歌曲信息"""
        info = self.internal_recommender.get_song_info(song_id)
        if info:
            return info
        info = self.external_recommender.get_song_info(song_id)
        if info:
            return info
        return {
            'song_id': song_id,
            'song_name': '未知歌曲',
            'artists': '未知艺术家',
            'genre': '未知',
            'popularity': 50,
            'source': 'unknown'
        }
    
    def get_user_history(self, user_id, n=5):
        """获取用户历史"""
        user_type = self.get_user_type(user_id)
        if user_type == 'internal':
            return self.internal_recommender.get_user_history(user_id, n)
        else:
            return self.external_recommender.get_user_history(user_id, n)
    
    # ------------------------ SQL 推荐结果保存 ------------------------
    def save_recommendations_to_sql(self, user_id, recs, algorithm_type='hybrid', 
                                expire_days=7, engine=None):
        """保存推荐结果到 recommendations 表（带用户有效性检查）"""
        if not recs:
            return
        
        from sqlalchemy import create_engine, text
        import pandas as pd
        
        if engine is None:
            db_config = {
                'server': 'localhost',
                'database': 'MusicRecommendationDB',
                'username': 'sa',
                'password': '123456',
                'driver': 'ODBC Driver 18 for SQL Server'
            }
            conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                    f"@{db_config['server']}/{db_config['database']}"
                    f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
            engine = create_engine(conn_str)
        
        try:
            with engine.connect() as conn:
                existing_users = pd.read_sql("SELECT user_id FROM enhanced_user_features", conn)
                valid_user_ids = set(existing_users['user_id'].astype(str))
        except Exception as e:
            print(f"  ⚠️ 无法查询用户表，跳过保存: {e}")
            return
        
        user_id_str = str(user_id)
        if user_id_str not in valid_user_ids:
            print(f"  ⚠️ 用户 {user_id_str} 不在 enhanced_user_features 表中，跳过保存")
            return
        
        now = datetime.now()
        expire_time = now + timedelta(days=expire_days)
        
        data = []
        for rank, (song_id, score) in enumerate(recs, 1):
            data.append({
                'user_id': user_id_str,
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
            df = pd.DataFrame(data)
            with engine.begin() as conn:
                conn.execute(text(
                    f"DELETE FROM recommendations WHERE user_id = '{user_id_str}' "
                    f"AND CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)"
                ))
            df.to_sql('recommendations', engine, if_exists='append', index=False)
            print(f"  ✓ 已保存用户 {user_id_str} 的 {len(recs)} 条推荐")
        except Exception as e:
            print(f"  ⚠️ 保存推荐结果失败: {e}")
    
    def batch_save_recommendations(self, user_ids, n=10, algorithm_type='hybrid'):
        """批量生成并保存推荐"""
        from sqlalchemy import create_engine
        from datetime import datetime
        
        db_config = {
            'server': 'localhost',
            'database': 'MusicRecommendationDB',
            'username': 'sa',
            'password': '123456',
            'driver': 'ODBC Driver 18 for SQL Server'
        }
        conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                    f"@{db_config['server']}/{db_config['database']}"
                    f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
        engine = create_engine(conn_str)
        
        print(f"\n开始批量生成并保存推荐（{len(user_ids)}个用户）...")
        start_time = datetime.now()
        
        success = 0
        fail = 0
        
        for i, uid in enumerate(user_ids, 1):
            try:
                recs = self.recommend(uid, n=n)
                if recs:
                    self.save_recommendations_to_sql(uid, recs, algorithm_type, engine=engine)
                    success += 1
                else:
                    fail += 1
                
                if i % 100 == 0 or i == len(user_ids):
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = i / elapsed if elapsed > 0 else 0
                    print(f"  进度: {i}/{len(user_ids)} ({i/len(user_ids)*100:.1f}%) | "
                        f"成功: {success} | 失败: {fail} | 速度: {rate:.2f}用户/秒")
            except Exception as e:
                print(f"  用户 {uid} 处理失败: {e}")
                fail += 1
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n批量保存完成:")
        print(f"  成功: {success} 用户")
        print(f"  失败: {fail} 用户")
        print(f"  总耗时: {elapsed:.1f}秒 ({elapsed/len(user_ids):.2f}秒/用户)")
    
    # ------------------------ 测试演示 ------------------------
    def test_recommendation(self, user_id=None, n=8):
        """测试推荐（显示历史与推荐）"""
        print("\n" + "-"*60)
        print("分离式推荐系统测试")
        print("-"*60)
        
        if user_id is None:
            if hasattr(self.internal_recommender, 'user_to_idx') and self.internal_recommender.user_to_idx:
                user_id = list(self.internal_recommender.user_to_idx.keys())[0]
            else:
                user_id = 'internal_1'
        
        user_type = self.get_user_type(user_id)
        recommender = self.internal_recommender if user_type == 'internal' else self.external_recommender
        
        print(f"用户ID: {user_id}")
        print(f"用户类型: {user_type}")
        
        history = self.get_user_history(user_id, 3)
        if history:
            print("\n历史收听:")
            for h in history:
                print(f"  - {h['song_name']} ({h['genre']}, 流行度{h['popularity']}, 来源:{h.get('source', '未知')})")
        
        recs = self.recommend(user_id, n)
        print(f"\n推荐结果 ({len(recs)}首):")
        genres, sources = [], []
        for i, (sid, score) in enumerate(recs, 1):
            info = self.get_song_info(sid)
            if info:
                genres.append(info['genre'])
                sources.append(info.get('source', '未知'))
                print(f"  {i}. {info['song_name']} ({info['genre']}, 流行度{info['popularity']}, 来源:{info.get('source', '未知')}) [{score:.3f}]")
        
        if recs:
            unique_genres = len(set(genres))
            internal_cnt = sources.count('internal')
            external_cnt = sources.count('external')
            total = internal_cnt + external_cnt
            print(f"\n统计: {unique_genres}个流派, 内部{internal_cnt/total*100:.1f}%, 外部{external_cnt/total*100:.1f}%")
        return recs
    
    # ------------------------ 权重调优接口 ------------------------
    def tune_weights_for_source(self, source_type='internal', n_val_users=200, n=10):
        recommender = getattr(self, f'{source_type}_recommender')
        val_users = random.sample(list(recommender.test_interactions['user_id'].unique()),
                                   min(n_val_users, len(recommender.test_interactions['user_id'].unique())))
        best_weights = recommender.tune_weights(val_users, n=n)
        print(f"{source_type} 最佳权重: {best_weights}")
        return best_weights


# ---------------------------- 离线评估器（优化版） ----------------------------
class SeparatedRecommenderEvaluator:
    """分离式推荐系统评估器（支持权重传入）"""
    
    def __init__(self, recommender, save_to_sql=False, db_config=None):
        self.rec = recommender
        self.save_to_sql = save_to_sql
        if save_to_sql:
            try:
                from sqlalchemy import create_engine
                if db_config is None:
                    db_config = {
                        'server': 'localhost',
                        'database': 'MusicRecommendationDB',
                        'username': 'sa',
                        'password': '123456',
                        'driver': 'ODBC Driver 18 for SQL Server'
                    }
                conn_str = (f"mssql+pyodbc://{db_config['username']}:{db_config['password']}"
                           f"@{db_config['server']}/{db_config['database']}"
                           f"?driver={db_config['driver'].replace(' ', '+')}&Encrypt=no")
                self.engine = create_engine(conn_str)
                print("  ✓ 已连接SQL Server（评估结果将保存）")
            except Exception as e:
                print(f"  ⚠️ 数据库连接失败: {e}")
                self.engine = None
                self.save_to_sql = False
        else:
            self.engine = None
    
    def evaluate(self, n_users=500, k=10, save_recs=False,
                internal_weights=(0.15,0.0,0.15,0.25,0.1,0.1,0.15),
                external_weights=(0.2,0.0,0.15,0.3,0.0,0.1,0.15),
                min_interactions=5):
        """
        评估推荐系统，使用指定权重
        internal_weights: (w_itemcf, w_usercf, w_content, w_mf, w_sentiment, w_artist, w_lightfm)
        external_weights: 同上
        min_interactions: 只评估训练集中交互次数 >= 该值的用户
        """
        print("\n" + "="*80)
        print(f"推荐系统评估 (n_users={n_users}, k={k})")
        print(f"  内部权重: ItemCF={internal_weights[0]}, UserCF={internal_weights[1]}, Content={internal_weights[2]}, MF={internal_weights[3]}, Sentiment={internal_weights[4]}, Artist={internal_weights[5]}, LightFM={internal_weights[6]}")
        print(f"  外部权重: ItemCF={external_weights[0]}, UserCF={external_weights[1]}, Content={external_weights[2]}, MF={external_weights[3]}, Sentiment={external_weights[4]}, Artist={external_weights[5]}, LightFM={external_weights[6]}")
        if save_recs and self.save_to_sql:
            print("  [模式：评估并保存推荐结果到SQL]")
        else:
            print("  [模式：仅评估，不保存]")
        print("="*80)
        
        import random
        from datetime import datetime
        
        np.random.seed(42)
        random.seed(42)
        
        results = {}
        overall_start = datetime.now()
        
        for source_type, weights in [('internal', internal_weights), ('external', external_weights)]:
            recommender = getattr(self.rec, f'{source_type}_recommender')
            
            # 筛选交互次数足够的用户
            user_counts = recommender.train_interactions.groupby('user_id').size()
            valid_users = set(user_counts[user_counts >= min_interactions].index)
            test_users = [u for u in recommender.test_interactions['user_id'].unique() if u in valid_users]
            if len(test_users) == 0:
                print(f"⚠️ {source_type} 无有效测试用户（交互数≥{min_interactions}），跳过")
                continue
            
            eval_users = random.sample(test_users, min(n_users, len(test_users)))
            total_eval = len(eval_users)
            print(f"\n▶ 开始评估 {source_type.upper()} 用户 (共{total_eval}人)")
            
            metrics = {
                'precision': [], 'recall': [], 'ndcg': [], 
                'hit': [], 'diversity': [], 'coverage': set(),
                'popularity': []
            }
            
            recs_buffer = []
            source_start = datetime.now()
            
            for i, uid in enumerate(eval_users, 1):
                test_songs = set(recommender.test_interactions[
                    recommender.test_interactions['user_id'] == uid
                ]['song_id'].tolist())
                if not test_songs:
                    continue
                
                try:
                    recs = recommender.hybrid_recommendation_parallel(
                        uid, n=k, use_mmr=False,
                        w_itemcf=weights[0], w_usercf=weights[1],
                        w_content=weights[2], w_mf=weights[3],
                        w_sentiment=weights[4], w_artist=weights[5],
                        w_lightfm=weights[6]
                    )
                except Exception as e:
                    print(f"    用户 {uid} 推荐失败: {e}")
                    continue
                if not recs:
                    continue
                rec_songs = [r[0] for r in recs]
                
                if save_recs and self.save_to_sql and self.engine:
                    recs_buffer.append((uid, recs))
                    if len(recs_buffer) >= 200:
                        self._flush_recs_buffer(recs_buffer, source_type)
                        recs_buffer = []
                
                hits = len(set(rec_songs) & test_songs)
                k_actual = len(rec_songs)
                metrics['precision'].append(hits / k_actual if k_actual>0 else 0)
                metrics['recall'].append(hits / len(test_songs) if test_songs else 0)
                metrics['hit'].append(1 if hits>0 else 0)
                
                dcg = sum(1 / np.log2(idx+2) for idx, song in enumerate(rec_songs) if song in test_songs)
                idcg = sum(1 / np.log2(idx+2) for idx in range(min(len(test_songs), k)))
                metrics['ndcg'].append(dcg / idcg if idcg>0 else 0)
                
                genres = []
                for song in rec_songs:
                    info = recommender.get_song_info(song)
                    if info and info.get('genre'):
                        genres.append(info['genre'])
                if genres:
                    metrics['diversity'].append(len(set(genres)) / len(genres))
                
                metrics['coverage'].update(rec_songs)
                
                pops = [recommender.song_popularity.get(song, 50) for song in rec_songs]
                metrics['popularity'].append(np.mean(pops) if pops else 50)
                
                if i % 50 == 0 or i == total_eval:
                    elapsed = (datetime.now() - source_start).total_seconds()
                    rate = i / elapsed if elapsed > 0 else 0
                    remaining = (total_eval - i) / rate if rate > 0 else 0
                    print(f"    {source_type.upper()} 进度: {i}/{total_eval} "
                        f"({i/total_eval*100:.1f}%) | 耗时: {elapsed:.1f}s | "
                        f"速度: {rate:.2f}用户/秒 | 预计剩余: {remaining:.1f}s")
            
            if save_recs and recs_buffer:
                self._flush_recs_buffer(recs_buffer, source_type)
                recs_buffer = []
            
            source_elapsed = (datetime.now() - source_start).total_seconds()
            results[source_type] = {
                'Precision@K': np.mean(metrics['precision']) if metrics['precision'] else 0,
                'Recall@K': np.mean(metrics['recall']) if metrics['recall'] else 0,
                'NDCG@K': np.mean(metrics['ndcg']) if metrics['ndcg'] else 0,
                'HitRate': np.mean(metrics['hit']) if metrics['hit'] else 0,
                'Diversity': np.mean(metrics['diversity']) if metrics['diversity'] else 0,
                'Coverage': len(metrics['coverage']) / recommender.n_songs if recommender.n_songs else 0,
                'AvgPopularity': np.mean(metrics['popularity']) if metrics['popularity'] else 0,
                'n_users': len(eval_users),
                'eval_time_sec': source_elapsed
            }
            
            print(f"\n📊 {source_type.upper()} 评估结果 (耗时 {source_elapsed:.1f}秒):")
            for key, val in results[source_type].items():
                if isinstance(val, float):
                    print(f"  {key}: {val:.4f}")
                else:
                    print(f"  {key}: {val}")
        
        overall_elapsed = (datetime.now() - overall_start).total_seconds()
        print(f"\n⏱️ 总评估耗时: {overall_elapsed:.1f}秒")
        return results
    
    def _flush_recs_buffer(self, buffer, source_type=None):
        """批量刷新推荐结果到数据库"""
        if not buffer or not self.engine:
            return
        
        try:
            import pandas as pd
            from datetime import datetime, timedelta
            from sqlalchemy import text
            
            with self.engine.connect() as conn:
                existing_users = pd.read_sql("SELECT user_id FROM enhanced_user_features", conn)
                valid_user_ids = set(existing_users['user_id'].astype(str))
            
            now = datetime.now()
            expire = now + timedelta(days=7)
            data = []
            user_ids_today = set()
            
            for user_id, recs in buffer:
                user_id_str = str(user_id)
                if user_id_str not in valid_user_ids:
                    continue
                user_ids_today.add(user_id_str)
                for rank, (song_id, score) in enumerate(recs, 1):
                    data.append({
                        'user_id': user_id_str,
                        'song_id': str(song_id),
                        'recommendation_score': float(score),
                        'algorithm_type': 'hybrid_eval',
                        'rank_position': rank,
                        'is_viewed': False,
                        'is_clicked': False,
                        'is_listened': False,
                        'created_at': now,
                        'expires_at': expire
                    })
            
            if not data:
                return
            
            df = pd.DataFrame(data)
            
            with self.engine.begin() as conn:
                for uid in user_ids_today:
                    conn.execute(text(
                        f"DELETE FROM recommendations WHERE user_id = '{uid}' "
                        f"AND CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)"
                    ))
            
            df.to_sql('recommendations', self.engine, if_exists='append', index=False)
            
            source_tag = f"[{source_type.upper()}] " if source_type else ""
            print(f"    {source_tag}已保存 {len(user_ids_today)} 用户的推荐 （共 {len(data)} 条）")
        
        except Exception as e:
            source_tag = f"[{source_type.upper()}] " if source_type else ""
            print(f"    {source_tag}保存推荐结果失败: {e}")


# ---------------------------- 主函数 ----------------------------
def main_separated():
    print("="*80)
    print("分离式音乐推荐系统（全算法保留 + LightFM）")
    print("="*80)
    
    recommender = SeparatedMusicRecommender(
        data_dir="separated_processed_data",
        cache_dir="recommender_cache"
    )
    
    # 可选：进行权重调优（会耗时，建议先跑少量用户测试）
    print("\n" + "="*80)
    print("开始权重调优（内部用户）...")
    print("="*80)
    internal_best = recommender.tune_weights_for_source('internal', n_val_users=200, n=10)
    
    print("\n" + "="*80)
    print("开始权重调优（外部用户）...")
    print("="*80)
    external_best = recommender.tune_weights_for_source('external', n_val_users=200, n=10)
    
    # 使用调优后的权重进行评估
    print("\n" + "="*80)
    print("开始离线评估（使用调优权重）...")
    print("="*80)
    evaluator = SeparatedRecommenderEvaluator(recommender, save_to_sql=True)
    # 将调优后的5个权重与固定的 artist/lightfm 权重组合
    internal_weights = internal_best + (0.1, 0.15)  # 注意 internal_best 是5元组
    external_weights = external_best + (0.1, 0.15)
    results = evaluator.evaluate(n_users=300, k=10, save_recs=True,
                                  internal_weights=internal_weights,
                                  external_weights=external_weights,
                                  min_interactions=5)
    
    # 测试单个用户
    print("\n" + "="*80)
    print("测试内部用户推荐...")
    print("="*80)
    internal_users = list(recommender.internal_recommender.user_to_idx.keys())
    if internal_users:
        recommender.test_recommendation(internal_users[0], 8)
    
    print("\n" + "="*80)
    print("测试外部用户推荐...")
    print("="*80)
    external_users = list(recommender.external_recommender.user_to_idx.keys())
    if external_users:
        recommender.test_recommendation(external_users[0], 8)
    
    return recommender

if __name__ == "__main__":
    recommender = main_separated()