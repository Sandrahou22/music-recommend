# file: analyze_data.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import os
import json
import warnings
warnings.filterwarnings('ignore')
from collections import Counter

plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']  # 设置中文字体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

class DataDistributionAnalyzer:
    """数据分布分析器"""
    
    def __init__(self, data_dir="processed_data_complete"):
        self.data_dir = data_dir
        self.results = {}
        
    def load_data(self):
        """加载所有数据"""
        print("="*80)
        print("加载数据...")
        
        # 加载歌曲特征
        self.song_features = pd.read_csv(
            os.path.join(self.data_dir, "song_features.csv")
        )
        print(f"歌曲特征: {self.song_features.shape}")
        
        # 加载交互矩阵
        self.interaction_matrix = pd.read_csv(
            os.path.join(self.data_dir, "interaction_matrix.csv")
        )
        print(f"交互矩阵: {self.interaction_matrix.shape}")
        
        # 加载用户特征
        self.user_features = pd.read_csv(
            os.path.join(self.data_dir, "user_features.csv")
        )
        print(f"用户特征: {self.user_features.shape}")
        
        # 确保user_id是字符串类型
        self.interaction_matrix['user_id'] = self.interaction_matrix['user_id'].astype(str)
        self.user_features['user_id'] = self.user_features['user_id'].astype(str)
        
        # 过滤内部用户
        # 使用安全的过滤方法，避免浮点数问题
        if 'user_id' in self.interaction_matrix.columns:
            # 方法1: 使用布尔索引
            self.internal_interactions = self.interaction_matrix[
                ~self.interaction_matrix['user_id'].astype(str).str.startswith('ext_')
            ].copy()
        else:
            # 如果没有user_id列，使用全部数据
            self.internal_interactions = self.interaction_matrix.copy()
            
        print(f"内部交互记录: {self.internal_interactions.shape}")
        
    def analyze_song_features(self):
        """分析歌曲特征分布"""
        print("\n" + "="*80)
        print("分析歌曲特征分布...")
        
        results = {}
        
        # 1. 流行度分析
        if 'popularity' in self.song_features.columns:
            print("1. 歌曲流行度分析:")
            popularity = self.song_features['popularity']
            
            # 基本统计
            results['popularity_stats'] = {
                'mean': float(popularity.mean()),
                'median': float(popularity.median()),
                'std': float(popularity.std()),
                'min': float(popularity.min()),
                'max': float(popularity.max())
            }
            
            print(f"   均值: {results['popularity_stats']['mean']:.2f}")
            print(f"   中位数: {results['popularity_stats']['median']:.2f}")
            print(f"   标准差: {results['popularity_stats']['std']:.2f}")
            
            # 分布情况
            value_counts = popularity.value_counts().sort_index()
            top_values = value_counts.head(10)
            print(f"\n   流行度值分布 (前10):")
            for value, count in top_values.items():
                percentage = count / len(popularity) * 100
                print(f"     {value}: {count} 首 ({percentage:.2f}%)")
            
            # 检查流行度=100的比例
            pop_100_count = (popularity == 100).sum()
            pop_100_ratio = pop_100_count / len(popularity) * 100
            results['pop_100_ratio'] = pop_100_ratio
            print(f"\n   流行度为100的歌曲: {pop_100_count} 首 ({pop_100_ratio:.2f}%)")
            
            # 绘制流行度分布图
            self.plot_popularity_distribution(popularity, results)
        
        # 2. 音频特征分析
        audio_features = ['danceability', 'energy', 'valence', 'tempo']
        available_audio = [f for f in audio_features if f in self.song_features.columns]
        
        if available_audio:
            print(f"\n2. 音频特征分析:")
            results['audio_features'] = {}
            
            for feature in available_audio:
                values = self.song_features[feature]
                # 统计缺失值
                missing = values.isnull().sum()
                missing_ratio = missing / len(values) * 100
                
                if missing < len(values):
                    # 只统计非缺失值
                    non_missing = values.dropna()
                    stats_dict = {
                        'mean': float(non_missing.mean()),
                        'median': float(non_missing.median()),
                        'std': float(non_missing.std()),
                        'missing': int(missing),
                        'missing_ratio': float(missing_ratio)
                    }
                    results['audio_features'][feature] = stats_dict
                    
                    print(f"   {feature}:")
                    print(f"     均值: {stats_dict['mean']:.4f}, 中位数: {stats_dict['median']:.4f}")
                    print(f"     缺失: {stats_dict['missing']} ({stats_dict['missing_ratio']:.2f}%)")
            
            # 绘制音频特征分布图
            self.plot_audio_features_distribution(available_audio)
        
        # 3. 歌曲时长分析
        if 'duration_ms' in self.song_features.columns:
            print(f"\n3. 歌曲时长分析:")
            duration_seconds = self.song_features['duration_ms'] / 1000
            
            results['duration_stats'] = {
                'mean_seconds': float(duration_seconds.mean()),
                'median_seconds': float(duration_seconds.median()),
                'std_seconds': float(duration_seconds.std())
            }
            
            print(f"   平均时长: {results['duration_stats']['mean_seconds']:.2f} 秒")
            print(f"   中位数时长: {results['duration_stats']['median_seconds']:.2f} 秒")
            
            # 转换为分钟
            duration_minutes = duration_seconds / 60
            self.plot_duration_distribution(duration_minutes)
        
        # 4. 流派分析
        if 'genre' in self.song_features.columns:
            print(f"\n4. 流派分布分析:")
            genre_counts = self.song_features['genre'].value_counts()
            results['genre_counts'] = genre_counts.head(20).to_dict()
            
            print(f"   总流派数: {len(genre_counts)}")
            print(f"   前10流派:")
            for i, (genre, count) in enumerate(genre_counts.head(10).items(), 1):
                percentage = count / len(self.song_features) * 100
                print(f"     {i:2d}. {genre}: {count} 首 ({percentage:.2f}%)")
            
            # 绘制流派分布图
            self.plot_genre_distribution(genre_counts)
        
        # 5. 歌曲年龄分析
        if 'publish_year' in self.song_features.columns:
            print(f"\n5. 歌曲年龄分析:")
            current_year = 2024  # 可根据实际情况调整
            self.song_features['song_age'] = current_year - self.song_features['publish_year']
            age_counts = self.song_features['song_age'].value_counts().sort_index()
            
            results['age_stats'] = {
                'mean_age': float(self.song_features['song_age'].mean()),
                'median_age': float(self.song_features['song_age'].median()),
                'oldest': int(self.song_features['song_age'].max()),
                'newest': int(self.song_features['song_age'].min())
            }
            
            print(f"   平均年龄: {results['age_stats']['mean_age']:.1f} 年")
            print(f"   最老歌曲: {results['age_stats']['oldest']} 年")
            print(f"   最新歌曲: {results['age_stats']['newest']} 年")
            
            # 绘制歌曲年龄分布
            self.plot_song_age_distribution(self.song_features['song_age'])
        
        self.results['song_features'] = results
        return results
    
    def analyze_interaction_matrix(self):
        """分析交互矩阵"""
        print("\n" + "="*80)
        print("分析交互矩阵...")
        
        results = {}
        
        # 1. 基本统计
        print("1. 交互矩阵基本统计:")
        results['basic_stats'] = {
            'total_interactions': len(self.internal_interactions),
            'unique_users': self.internal_interactions['user_id'].nunique(),
            'unique_songs': self.internal_interactions['song_id'].nunique()
        }
        
        print(f"   总交互数: {results['basic_stats']['total_interactions']:,}")
        print(f"   唯一用户数: {results['basic_stats']['unique_users']:,}")
        print(f"   唯一歌曲数: {results['basic_stats']['unique_songs']:,}")
        
        # 计算稀疏度
        total_possible = results['basic_stats']['unique_users'] * results['basic_stats']['unique_songs']
        sparsity = 1 - (results['basic_stats']['total_interactions'] / total_possible)
        results['basic_stats']['sparsity'] = sparsity
        print(f"   稀疏度: {sparsity:.6f}")
        
        # 2. 用户交互次数分布
        print("\n2. 用户交互次数分布:")
        user_interaction_counts = self.internal_interactions.groupby('user_id').size()
        
        results['user_interaction_stats'] = {
            'mean': float(user_interaction_counts.mean()),
            'median': float(user_interaction_counts.median()),
            'std': float(user_interaction_counts.std()),
            'max': int(user_interaction_counts.max()),
            'min': int(user_interaction_counts.min())
        }
        
        print(f"   平均每个用户交互数: {results['user_interaction_stats']['mean']:.2f}")
        print(f"   中位数: {results['user_interaction_stats']['median']}")
        print(f"   最多交互: {results['user_interaction_stats']['max']:,}")
        print(f"   最少交互: {results['user_interaction_stats']['min']}")
        
        # 用户交互次数分组
        bins = [0, 1, 5, 10, 20, 50, 100, float('inf')]
        labels = ['1次', '2-5次', '6-10次', '11-20次', '21-50次', '51-100次', '100+次']
        user_groups = pd.cut(user_interaction_counts, bins=bins, labels=labels, right=False)
        group_counts = user_groups.value_counts().sort_index()
        
        results['user_groups'] = group_counts.to_dict()
        
        print(f"\n   用户交互次数分组:")
        for group, count in group_counts.items():
            percentage = count / len(user_interaction_counts) * 100
            print(f"     {group}: {count} 用户 ({percentage:.2f}%)")
        
        # 绘制用户交互分布
        self.plot_user_interaction_distribution(user_interaction_counts)
        
        # 3. 歌曲被交互次数分布
        print("\n3. 歌曲被交互次数分布:")
        song_interaction_counts = self.internal_interactions.groupby('song_id').size()
        
        results['song_interaction_stats'] = {
            'mean': float(song_interaction_counts.mean()),
            'median': float(song_interaction_counts.median()),
            'std': float(song_interaction_counts.std()),
            'max': int(song_interaction_counts.max()),
            'min': int(song_interaction_counts.min())
        }
        
        print(f"   平均每首歌被交互数: {results['song_interaction_stats']['mean']:.2f}")
        print(f"   中位数: {results['song_interaction_stats']['median']}")
        print(f"   最多被交互: {results['song_interaction_stats']['max']:,}")
        print(f"   最少被交互: {results['song_interaction_stats']['min']}")
        
        # 歌曲交互次数分组
        song_groups = pd.cut(song_interaction_counts, bins=bins, labels=labels, right=False)
        song_group_counts = song_groups.value_counts().sort_index()
        
        results['song_groups'] = song_group_counts.to_dict()
        
        print(f"\n   歌曲被交互次数分组:")
        for group, count in song_group_counts.items():
            percentage = count / len(song_interaction_counts) * 100
            print(f"     {group}: {count} 歌曲 ({percentage:.2f}%)")
        
        # 绘制歌曲交互分布
        self.plot_song_interaction_distribution(song_interaction_counts)
        
        # 4. 交互权重分析
        print("\n4. 交互权重分析:")
        if 'total_weight' in self.internal_interactions.columns:
            weight_stats = self.internal_interactions['total_weight'].describe()
            results['weight_stats'] = {
                'mean': float(weight_stats['mean']),
                'std': float(weight_stats['std']),
                'min': float(weight_stats['min']),
                '25%': float(weight_stats['25%']),
                '50%': float(weight_stats['50%']),
                '75%': float(weight_stats['75%']),
                'max': float(weight_stats['max'])
            }
            
            print(f"   平均权重: {results['weight_stats']['mean']:.2f}")
            print(f"   标准差: {results['weight_stats']['std']:.2f}")
            print(f"   范围: [{results['weight_stats']['min']}, {results['weight_stats']['max']}]")
            
            # 绘制权重分布
            self.plot_weight_distribution(self.internal_interactions['total_weight'])
        
        self.results['interaction_matrix'] = results
        return results
    
    def analyze_user_features(self):
        """分析用户特征"""
        print("\n" + "="*80)
        print("分析用户特征...")
        
        results = {}
        
        # 1. 基本用户统计
        print("1. 用户基本统计:")
        results['basic_stats'] = {
            'total_users': len(self.user_features),
            'unique_users': self.user_features['user_id'].nunique()
        }
        
        print(f"   总用户数: {results['basic_stats']['total_users']:,}")
        print(f"   唯一用户数: {results['basic_stats']['unique_users']:,}")
        
        # 2. 年龄分析
        if 'age' in self.user_features.columns:
            print("\n2. 用户年龄分析:")
            age_data = pd.to_numeric(self.user_features['age'], errors='coerce')
            age_data = age_data.dropna()
            
            if len(age_data) > 0:
                results['age_stats'] = {
                    'mean': float(age_data.mean()),
                    'median': float(age_data.median()),
                    'std': float(age_data.std()),
                    'min': float(age_data.min()),
                    'max': float(age_data.max())
                }
                
                print(f"   平均年龄: {results['age_stats']['mean']:.1f} 岁")
                print(f"   中位数: {results['age_stats']['median']:.1f} 岁")
                print(f"   年龄范围: [{results['age_stats']['min']}, {results['age_stats']['max']}]")
                
                # 年龄分组
                bins = [0, 18, 25, 35, 45, 55, 100]
                labels = ['<18', '18-25', '26-35', '36-45', '46-55', '>55']
                age_groups = pd.cut(age_data, bins=bins, labels=labels, right=False)
                group_counts = age_groups.value_counts().sort_index()
                
                results['age_groups'] = group_counts.to_dict()
                
                print(f"\n   年龄分组分布:")
                for group, count in group_counts.items():
                    percentage = count / len(age_data) * 100
                    print(f"     {group}: {count} 用户 ({percentage:.2f}%)")
                
                # 绘制年龄分布
                self.plot_age_distribution(age_data, age_groups)
        
        # 3. 性别分析
        if 'gender' in self.user_features.columns:
            print("\n3. 用户性别分析:")
            gender_counts = self.user_features['gender'].value_counts()
            results['gender_counts'] = gender_counts.to_dict()
            
            print(f"   性别分布:")
            for gender, count in gender_counts.items():
                percentage = count / len(self.user_features) * 100
                print(f"     {gender}: {count} 用户 ({percentage:.2f}%)")
            
            # 绘制性别分布
            self.plot_gender_distribution(gender_counts)
        
        # 4. 用户活跃度分析
        if 'listen_songs' in self.user_features.columns:
            print("\n4. 用户活跃度分析:")
            listen_counts = self.user_features['listen_songs']
            
            results['listen_stats'] = {
                'mean': float(listen_counts.mean()),
                'median': float(listen_counts.median()),
                'std': float(listen_counts.std()),
                'max': int(listen_counts.max()),
                'min': int(listen_counts.min())
            }
            
            print(f"   平均听歌数: {results['listen_stats']['mean']:.1f}")
            print(f"   中位数: {results['listen_stats']['median']}")
            print(f"   最多听歌: {results['listen_stats']['max']:,}")
            print(f"   最少听歌: {results['listen_stats']['min']}")
            
            # 活跃度分组
            listen_bins = [0, 10, 50, 100, 500, 1000, float('inf')]
            listen_labels = ['<10', '10-50', '51-100', '101-500', '501-1000', '1000+']
            listen_groups = pd.cut(listen_counts, bins=listen_bins, labels=listen_labels, right=False)
            listen_group_counts = listen_groups.value_counts().sort_index()
            
            results['listen_groups'] = listen_group_counts.to_dict()
            
            print(f"\n   听歌数量分组:")
            for group, count in listen_group_counts.items():
                percentage = count / len(listen_counts) * 100
                print(f"     {group}: {count} 用户 ({percentage:.2f}%)")
            
            # 绘制听歌数分布
            self.plot_listen_distribution(listen_counts)
        
        self.results['user_features'] = results
        return results
    
    def analyze_data_quality(self):
        """分析数据质量问题"""
        print("\n" + "="*80)
        print("分析数据质量问题...")
        
        results = {}
        
        # 1. 检查歌曲特征缺失
        print("1. 歌曲特征缺失分析:")
        song_missing = self.song_features.isnull().sum()
        high_missing_cols = song_missing[song_missing > 0.3 * len(self.song_features)]
        
        results['song_missing_cols'] = high_missing_cols.to_dict()
        
        if len(high_missing_cols) > 0:
            print(f"   高缺失率列 (>30%):")
            for col, missing in high_missing_cols.items():
                ratio = missing / len(self.song_features) * 100
                print(f"     {col}: {missing} 缺失 ({ratio:.2f}%)")
        else:
            print(f"   没有高缺失率列")
        
        # 2. 检查用户特征缺失
        print("\n2. 用户特征缺失分析:")
        user_missing = self.user_features.isnull().sum()
        high_missing_cols = user_missing[user_missing > 0.3 * len(self.user_features)]
        
        results['user_missing_cols'] = high_missing_cols.to_dict()
        
        if len(high_missing_cols) > 0:
            print(f"   高缺失率列 (>30%):")
            for col, missing in high_missing_cols.items():
                ratio = missing / len(self.user_features) * 100
                print(f"     {col}: {missing} 缺失 ({ratio:.2f}%)")
        else:
            print(f"   没有高缺失率列")
        
        # 3. 检查交互矩阵异常值
        print("\n3. 交互矩阵异常值分析:")
        if 'total_weight' in self.internal_interactions.columns:
            weights = self.internal_interactions['total_weight']
            
            # 使用IQR方法检测异常值
            q1 = weights.quantile(0.25)
            q3 = weights.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            outliers = weights[(weights < lower_bound) | (weights > upper_bound)]
            outlier_ratio = len(outliers) / len(weights) * 100
            
            results['weight_outliers'] = {
                'count': int(len(outliers)),
                'ratio': float(outlier_ratio),
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound)
            }
            
            print(f"   权重异常值数量: {len(outliers)} ({outlier_ratio:.2f}%)")
            print(f"   异常值检测边界: [{lower_bound:.2f}, {upper_bound:.2f}]")
        
        # 4. 检查数据一致性
        print("\n4. 数据一致性检查:")
        
        # 检查交互矩阵中的歌曲是否都在歌曲特征中
        interaction_songs = set(self.internal_interactions['song_id'].unique())
        feature_songs = set(self.song_features['song_id'].unique())
        
        songs_in_interaction_not_feature = interaction_songs - feature_songs
        songs_in_feature_not_interaction = feature_songs - interaction_songs
        
        results['song_consistency'] = {
            'interaction_only': len(songs_in_interaction_not_feature),
            'feature_only': len(songs_in_feature_not_interaction),
            'common': len(interaction_songs.intersection(feature_songs))
        }
        
        print(f"   交互矩阵中歌曲数: {len(interaction_songs):,}")
        print(f"   特征表中歌曲数: {len(feature_songs):,}")
        print(f"   交集歌曲数: {results['song_consistency']['common']:,}")
        print(f"   只在交互矩阵中: {results['song_consistency']['interaction_only']:,}")
        print(f"   只在特征表中: {results['song_consistency']['feature_only']:,}")
        
        # 检查交互矩阵中的用户是否都在用户特征中
        interaction_users = set(self.internal_interactions['user_id'].unique())
        feature_users = set(self.user_features['user_id'].unique())
        
        users_in_interaction_not_feature = interaction_users - feature_users
        users_in_feature_not_interaction = feature_users - interaction_users
        
        results['user_consistency'] = {
            'interaction_only': len(users_in_interaction_not_feature),
            'feature_only': len(users_in_feature_not_interaction),
            'common': len(interaction_users.intersection(feature_users))
        }
        
        print(f"\n   交互矩阵中用户数: {len(interaction_users):,}")
        print(f"   特征表中用户数: {len(feature_users):,}")
        print(f"   交集用户数: {results['user_consistency']['common']:,}")
        print(f"   只在交互矩阵中: {results['user_consistency']['interaction_only']:,}")
        print(f"   只在特征表中: {results['user_consistency']['feature_only']:,}")
        
        self.results['data_quality'] = results
        return results
    
    def plot_popularity_distribution(self, popularity, results):
        """绘制流行度分布图"""
        plt.figure(figsize=(15, 10))
        
        # 子图1：直方图
        plt.subplot(2, 3, 1)
        plt.hist(popularity, bins=50, edgecolor='black', alpha=0.7)
        plt.title('歌曲流行度分布')
        plt.xlabel('流行度')
        plt.ylabel('歌曲数量')
        plt.grid(True, alpha=0.3)
        
        # 子图2：箱线图
        plt.subplot(2, 3, 2)
        plt.boxplot(popularity, vert=False)
        plt.title('歌曲流行度箱线图')
        plt.xlabel('流行度')
        plt.grid(True, alpha=0.3)
        
        # 子图3：累计分布图
        plt.subplot(2, 3, 3)
        sorted_pop = np.sort(popularity)
        y_vals = np.arange(1, len(sorted_pop) + 1) / len(sorted_pop)
        plt.plot(sorted_pop, y_vals, linewidth=2)
        plt.title('流行度累计分布')
        plt.xlabel('流行度')
        plt.ylabel('累计比例')
        plt.grid(True, alpha=0.3)
        
        # 子图4：流行度值分布（前20）
        plt.subplot(2, 3, 4)
        value_counts = popularity.value_counts().sort_index().head(20)
        plt.bar(value_counts.index.astype(str), value_counts.values)
        plt.title('流行度值分布（前20）')
        plt.xlabel('流行度值')
        plt.ylabel('歌曲数量')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # 子图5：流行度百分位图
        plt.subplot(2, 3, 5)
        percentiles = np.percentile(popularity, range(0, 101, 5))
        plt.plot(range(0, 101, 5), percentiles, marker='o')
        plt.title('流行度百分位图')
        plt.xlabel('百分位')
        plt.ylabel('流行度')
        plt.grid(True, alpha=0.3)
        
        # 子图6：流行度与缺失值
        plt.subplot(2, 3, 6)
        # 检查是否有其他特征与流行度相关
        if 'duration_ms' in self.song_features.columns:
            duration = self.song_features['duration_ms'] / 1000 / 60  # 转换为分钟
            plt.scatter(popularity, duration, alpha=0.5)
            plt.title('流行度 vs 时长')
            plt.xlabel('流行度')
            plt.ylabel('时长 (分钟)')
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('popularity_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"\n   流行度分布图已保存为: popularity_distribution.png")
    
    def plot_audio_features_distribution(self, audio_features):
        """绘制音频特征分布图"""
        n_features = len(audio_features)
        n_cols = min(3, n_features)
        n_rows = (n_features + n_cols - 1) // n_cols
        
        plt.figure(figsize=(5 * n_cols, 4 * n_rows))
        
        for i, feature in enumerate(audio_features, 1):
            plt.subplot(n_rows, n_cols, i)
            values = self.song_features[feature].dropna()
            
            if len(values) > 0:
                plt.hist(values, bins=30, edgecolor='black', alpha=0.7)
                plt.title(f'{feature} 分布')
                plt.xlabel(feature)
                plt.ylabel('歌曲数量')
                plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('audio_features_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   音频特征分布图已保存为: audio_features_distribution.png")
    
    def plot_duration_distribution(self, duration_minutes):
        """绘制歌曲时长分布图"""
        plt.figure(figsize=(12, 5))
        
        plt.subplot(1, 2, 1)
        plt.hist(duration_minutes, bins=50, edgecolor='black', alpha=0.7)
        plt.title('歌曲时长分布')
        plt.xlabel('时长 (分钟)')
        plt.ylabel('歌曲数量')
        plt.grid(True, alpha=0.3)
        
        plt.subplot(1, 2, 2)
        plt.boxplot(duration_minutes, vert=False)
        plt.title('歌曲时长箱线图')
        plt.xlabel('时长 (分钟)')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('duration_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   歌曲时长分布图已保存为: duration_distribution.png")
    
    def plot_genre_distribution(self, genre_counts):
        """绘制流派分布图"""
        top_n = min(20, len(genre_counts))
        top_genres = genre_counts.head(top_n)
        
        plt.figure(figsize=(12, 8))
        
        # 条形图
        plt.subplot(2, 1, 1)
        colors = plt.cm.tab20c(np.arange(len(top_genres)) % 20)
        bars = plt.barh(range(len(top_genres)), top_genres.values, color=colors)
        plt.yticks(range(len(top_genres)), top_genres.index)
        plt.xlabel('歌曲数量')
        plt.title(f'流派分布 (前{top_n})')
        plt.gca().invert_yaxis()
        plt.grid(True, alpha=0.3, axis='x')
        
        # 在条形上添加数量标签
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width + max(top_genres.values) * 0.01, bar.get_y() + bar.get_height()/2,
                    f'{width:,}', ha='left', va='center')
        
        # 饼图（前10）
        plt.subplot(2, 1, 2)
        top_10 = genre_counts.head(10)
        other = genre_counts[10:].sum() if len(genre_counts) > 10 else 0
        
        if other > 0:
            pie_data = list(top_10.values) + [other]
            pie_labels = list(top_10.index) + ['其他']
        else:
            pie_data = top_10.values
            pie_labels = top_10.index
        
        plt.pie(pie_data, labels=pie_labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
        plt.title('流派占比 (前10)')
        
        plt.tight_layout()
        plt.savefig('genre_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   流派分布图已保存为: genre_distribution.png")
    
    def plot_song_age_distribution(self, song_age):
        """绘制歌曲年龄分布图"""
        plt.figure(figsize=(12, 5))
        
        plt.subplot(1, 2, 1)
        plt.hist(song_age, bins=30, edgecolor='black', alpha=0.7)
        plt.title('歌曲年龄分布')
        plt.xlabel('年龄 (年)')
        plt.ylabel('歌曲数量')
        plt.grid(True, alpha=0.3)
        
        plt.subplot(1, 2, 2)
        # 按年代分组
        decade_bins = [0, 1, 2, 5, 10, 20, 50, 100]
        decade_labels = ['<1年', '1-2年', '3-5年', '6-10年', '11-20年', '21-50年', '50+年']
        decade_groups = pd.cut(song_age, bins=decade_bins, labels=decade_labels, right=False)
        decade_counts = decade_groups.value_counts().sort_index()
        
        colors = plt.cm.viridis(np.linspace(0, 1, len(decade_counts)))
        plt.bar(range(len(decade_counts)), decade_counts.values, color=colors)
        plt.xticks(range(len(decade_counts)), decade_counts.index, rotation=45)
        plt.title('歌曲年代分布')
        plt.xlabel('年代')
        plt.ylabel('歌曲数量')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('song_age_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   歌曲年龄分布图已保存为: song_age_distribution.png")
    
    def plot_user_interaction_distribution(self, user_interaction_counts):
        """绘制用户交互分布图"""
        plt.figure(figsize=(15, 10))
        
        # 子图1：直方图（对数尺度）
        plt.subplot(2, 3, 1)
        log_counts = np.log10(user_interaction_counts + 1)
        plt.hist(log_counts, bins=30, edgecolor='black', alpha=0.7)
        plt.title('用户交互次数分布 (对数)')
        plt.xlabel('log10(交互次数 + 1)')
        plt.ylabel('用户数量')
        plt.grid(True, alpha=0.3)
        
        # 子图2：箱线图
        plt.subplot(2, 3, 2)
        plt.boxplot(user_interaction_counts, vert=False)
        plt.title('用户交互次数箱线图')
        plt.xlabel('交互次数')
        plt.grid(True, alpha=0.3)
        
        # 子图3：累计分布图
        plt.subplot(2, 3, 3)
        sorted_counts = np.sort(user_interaction_counts)
        y_vals = np.arange(1, len(sorted_counts) + 1) / len(sorted_counts)
        plt.plot(sorted_counts, y_vals, linewidth=2)
        plt.title('用户交互累计分布')
        plt.xlabel('交互次数')
        plt.ylabel('累计比例')
        plt.xscale('log')
        plt.grid(True, alpha=0.3)
        
        # 子图4：长尾分布可视化
        plt.subplot(2, 3, 4)
        rank = np.arange(1, len(sorted_counts) + 1)
        plt.loglog(rank, sorted_counts, 'o', markersize=2, alpha=0.5)
        plt.title('用户交互长尾分布')
        plt.xlabel('用户排名')
        plt.ylabel('交互次数')
        plt.grid(True, alpha=0.3)
        
        # 子图5：用户分组条形图
        plt.subplot(2, 3, 5)
        bins = [0, 1, 5, 10, 20, 50, 100, float('inf')]
        labels = ['1次', '2-5次', '6-10次', '11-20次', '21-50次', '51-100次', '100+次']
        user_groups = pd.cut(user_interaction_counts, bins=bins, labels=labels, right=False)
        group_counts = user_groups.value_counts().sort_index()
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(group_counts)))
        bars = plt.bar(range(len(group_counts)), group_counts.values, color=colors)
        plt.xticks(range(len(group_counts)), group_counts.index, rotation=45)
        plt.title('用户交互次数分组')
        plt.xlabel('交互次数分组')
        plt.ylabel('用户数量')
        plt.grid(True, alpha=0.3, axis='y')
        
        # 在条形上添加数量标签
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, height + max(group_counts.values)*0.01,
                    f'{int(height):,}', ha='center', va='bottom')
        
        # 子图6：帕累托图
        plt.subplot(2, 3, 6)
        sorted_desc = sorted_counts[::-1]
        cumulative = np.cumsum(sorted_desc)
        cumulative_percent = cumulative / cumulative[-1] * 100
        
        fig, ax1 = plt.subplots()
        color = 'tab:blue'
        ax1.bar(range(len(sorted_desc)), sorted_desc, color=color, alpha=0.6)
        ax1.set_xlabel('用户排序')
        ax1.set_ylabel('交互次数', color=color)
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.set_yscale('log')
        
        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.plot(range(len(sorted_desc)), cumulative_percent, color=color, linewidth=2)
        ax2.set_ylabel('累计百分比', color=color)
        ax2.tick_params(axis='y', labelcolor=color)
        ax2.grid(True, alpha=0.3)
        
        plt.title('用户交互帕累托图')
        plt.tight_layout()
        
        plt.tight_layout()
        plt.savefig('user_interaction_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   用户交互分布图已保存为: user_interaction_distribution.png")
    
    def plot_song_interaction_distribution(self, song_interaction_counts):
        """绘制歌曲交互分布图"""
        plt.figure(figsize=(12, 5))
        
        plt.subplot(1, 2, 1)
        log_counts = np.log10(song_interaction_counts + 1)
        plt.hist(log_counts, bins=30, edgecolor='black', alpha=0.7)
        plt.title('歌曲被交互次数分布 (对数)')
        plt.xlabel('log10(被交互次数 + 1)')
        plt.ylabel('歌曲数量')
        plt.grid(True, alpha=0.3)
        
        plt.subplot(1, 2, 2)
        # 长尾分布可视化
        sorted_counts = np.sort(song_interaction_counts)[::-1]
        rank = np.arange(1, len(sorted_counts) + 1)
        
        plt.loglog(rank, sorted_counts, 'o', markersize=2, alpha=0.5)
        plt.title('歌曲被交互长尾分布')
        plt.xlabel('歌曲排名')
        plt.ylabel('被交互次数')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('song_interaction_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   歌曲交互分布图已保存为: song_interaction_distribution.png")
    
    def plot_weight_distribution(self, weights):
        """绘制权重分布图"""
        plt.figure(figsize=(12, 5))
        
        plt.subplot(1, 2, 1)
        plt.hist(weights, bins=50, edgecolor='black', alpha=0.7)
        plt.title('交互权重分布')
        plt.xlabel('权重')
        plt.ylabel('频率')
        plt.grid(True, alpha=0.3)
        
        plt.subplot(1, 2, 2)
        plt.boxplot(weights, vert=False)
        plt.title('交互权重箱线图')
        plt.xlabel('权重')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('weight_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   权重分布图已保存为: weight_distribution.png")
    
    def plot_age_distribution(self, age_data, age_groups):
        """绘制用户年龄分布图"""
        plt.figure(figsize=(12, 5))
        
        plt.subplot(1, 2, 1)
        plt.hist(age_data, bins=20, edgecolor='black', alpha=0.7)
        plt.title('用户年龄分布')
        plt.xlabel('年龄')
        plt.ylabel('用户数量')
        plt.grid(True, alpha=0.3)
        
        plt.subplot(1, 2, 2)
        group_counts = age_groups.value_counts().sort_index()
        colors = plt.cm.viridis(np.linspace(0, 1, len(group_counts)))
        plt.bar(range(len(group_counts)), group_counts.values, color=colors)
        plt.xticks(range(len(group_counts)), group_counts.index, rotation=45)
        plt.title('用户年龄分组')
        plt.xlabel('年龄组')
        plt.ylabel('用户数量')
        plt.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig('user_age_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   用户年龄分布图已保存为: user_age_distribution.png")
    
    def plot_gender_distribution(self, gender_counts):
        """绘制用户性别分布图"""
        plt.figure(figsize=(10, 5))
        
        plt.subplot(1, 2, 1)
        colors = ['#ff9999', '#66b3ff', '#99ff99']
        plt.pie(gender_counts.values, labels=gender_counts.index, autopct='%1.1f%%', 
                colors=colors[:len(gender_counts)], startangle=90)
        plt.axis('equal')
        plt.title('用户性别分布')
        
        plt.subplot(1, 2, 2)
        plt.bar(range(len(gender_counts)), gender_counts.values, 
                color=colors[:len(gender_counts)], alpha=0.7)
        plt.xticks(range(len(gender_counts)), gender_counts.index)
        plt.title('用户性别分布 (条形图)')
        plt.xlabel('性别')
        plt.ylabel('用户数量')
        plt.grid(True, alpha=0.3, axis='y')
        
        # 在条形上添加数量标签
        for i, count in enumerate(gender_counts.values):
            plt.text(i, count + max(gender_counts.values)*0.01, 
                    f'{count:,}', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('user_gender_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   用户性别分布图已保存为: user_gender_distribution.png")
    
    def plot_listen_distribution(self, listen_counts):
        """绘制用户听歌数分布图"""
        plt.figure(figsize=(12, 5))
        
        plt.subplot(1, 2, 1)
        log_counts = np.log10(listen_counts + 1)
        plt.hist(log_counts, bins=30, edgecolor='black', alpha=0.7)
        plt.title('用户听歌数分布 (对数)')
        plt.xlabel('log10(听歌数 + 1)')
        plt.ylabel('用户数量')
        plt.grid(True, alpha=0.3)
        
        plt.subplot(1, 2, 2)
        plt.boxplot(listen_counts, vert=False)
        plt.title('用户听歌数箱线图')
        plt.xlabel('听歌数')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('user_listen_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"   用户听歌数分布图已保存为: user_listen_distribution.png")
    
    def save_analysis_results(self, filename="data_analysis_results.json"):
        """保存分析结果"""
        with open(filename, 'w', encoding='utf-8') as f:
            # 转换numpy类型为Python基本类型
            def convert(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, pd.Series):
                    return obj.to_dict()
                elif isinstance(obj, pd.DataFrame):
                    return obj.to_dict('records')
                elif isinstance(obj, dict):
                    return {k: convert(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert(item) for item in obj]
                else:
                    return obj
            
            json.dump(convert(self.results), f, ensure_ascii=False, indent=2)
        
        print(f"\n分析结果已保存为: {filename}")
    
    def generate_summary_report(self):
        """生成分析总结报告"""
        print("\n" + "="*80)
        print("数据分析总结报告")
        print("="*80)
        
        if not self.results:
            print("尚未进行分析，请先运行分析函数")
            return
        
        # 从结果中提取关键信息
        summary = {
            "数据规模": {
                "歌曲总数": self.song_features.shape[0],
                "用户总数": self.user_features.shape[0],
                "交互记录总数": self.internal_interactions.shape[0]
            },
            "关键问题": []
        }
        
        # 检查关键问题
        if 'song_features' in self.results:
            song_results = self.results['song_features']
            
            # 流行度问题
            if 'pop_100_ratio' in song_results:
                if song_results['pop_100_ratio'] > 50:
                    summary["关键问题"].append(
                        f"流行度偏差严重: {song_results['pop_100_ratio']:.1f}%的歌曲流行度为100"
                    )
            
            # 音频特征缺失问题
            if 'audio_features' in song_results:
                for feature, stats in song_results['audio_features'].items():
                    if stats['missing_ratio'] > 20:
                        summary["关键问题"].append(
                            f"音频特征'{feature}'缺失严重: {stats['missing_ratio']:.1f}%缺失"
                        )
        
        if 'interaction_matrix' in self.results:
            interaction_results = self.results['interaction_matrix']
            
            # 稀疏度问题
            if 'basic_stats' in interaction_results:
                sparsity = interaction_results['basic_stats']['sparsity']
                if sparsity > 0.999:
                    summary["关键问题"].append(
                        f"数据极度稀疏: 稀疏度达到{sparsity:.6f}"
                    )
            
            # 长尾分布问题
            if 'user_interaction_stats' in interaction_results:
                mean_interactions = interaction_results['user_interaction_stats']['mean']
                if mean_interactions < 10:
                    summary["关键问题"].append(
                        f"用户交互稀疏: 平均每个用户仅{mean_interactions:.1f}次交互"
                    )
        
        if 'data_quality' in self.results:
            quality_results = self.results['data_quality']
            
            # 数据一致性问题
            if 'song_consistency' in quality_results:
                song_consistency = quality_results['song_consistency']
                if song_consistency['interaction_only'] > 0:
                    summary["关键问题"].append(
                        f"数据不一致: {song_consistency['interaction_only']}首歌曲在交互矩阵中但不在特征表中"
                    )
        
        # 打印总结
        print("\n1. 数据规模总结:")
        for key, value in summary["数据规模"].items():
            print(f"   {key}: {value:,}")
        
        print("\n2. 关键问题发现:")
        if summary["关键问题"]:
            for i, problem in enumerate(summary["关键问题"], 1):
                print(f"   {i}. {problem}")
        else:
            print("   未发现明显数据质量问题")
        
        print("\n3. 优化建议:")
        if summary["关键问题"]:
            print("   建议按以下顺序解决问题:")
            print("   a. 重新计算歌曲流行度，解决流行度偏差问题")
            print("   b. 处理音频特征缺失值")
            print("   c. 重新设计数据过滤策略，解决稀疏性问题")
            print("   d. 检查并修复数据一致性问题")
        else:
            print("   数据质量良好，可直接进行下一步处理")
        
        # 保存总结报告
        with open("data_analysis_summary.txt", 'w', encoding='utf-8') as f:
            f.write("数据分析总结报告\n")
            f.write("="*80 + "\n\n")
            
            f.write("1. 数据规模总结:\n")
            for key, value in summary["数据规模"].items():
                f.write(f"   {key}: {value:,}\n")
            
            f.write("\n2. 关键问题发现:\n")
            if summary["关键问题"]:
                for i, problem in enumerate(summary["关键问题"], 1):
                    f.write(f"   {i}. {problem}\n")
            else:
                f.write("   未发现明显数据质量问题\n")
            
            f.write("\n3. 优化建议:\n")
            if summary["关键问题"]:
                f.write("   建议按以下顺序解决问题:\n")
                f.write("   a. 重新计算歌曲流行度，解决流行度偏差问题\n")
                f.write("   b. 处理音频特征缺失值\n")
                f.write("   c. 重新设计数据过滤策略，解决稀疏性问题\n")
                f.write("   d. 检查并修复数据一致性问题\n")
            else:
                f.write("   数据质量良好，可直接进行下一步处理\n")
        
        print(f"\n总结报告已保存为: data_analysis_summary.txt")
    
    def run_complete_analysis(self):
        """运行完整的数据分析流程"""
        print("开始完整的数据分析流程...")
        
        # 1. 加载数据
        self.load_data()
        
        # 2. 分析歌曲特征
        self.analyze_song_features()
        
        # 3. 分析交互矩阵
        self.analyze_interaction_matrix()
        
        # 4. 分析用户特征
        self.analyze_user_features()
        
        # 5. 分析数据质量
        self.analyze_data_quality()
        
        # 6. 保存分析结果
        self.save_analysis_results()
        
        # 7. 生成总结报告
        self.generate_summary_report()
        
        print("\n数据分析完成！")


def main():
    """主函数"""
    print("="*80)
    print("音乐推荐系统 - 数据分布分析")
    print("="*80)
    
    # 创建分析器实例
    analyzer = DataDistributionAnalyzer(data_dir="processed_data_complete")
    
    try:
        # 运行完整分析
        analyzer.run_complete_analysis()
        
        print("\n" + "="*80)
        print("分析完成！生成的文件:")
        print("  - 数据分析结果: data_analysis_results.json")
        print("  - 总结报告: data_analysis_summary.txt")
        print("  - 各种图表: *.png")
        print("\n下一步建议:")
        print("  1. 查看总结报告了解数据问题")
        print("  2. 根据分析结果进行数据预处理")
        print("  3. 参考图表理解数据分布")
        
    except Exception as e:
        print(f"\n分析过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()