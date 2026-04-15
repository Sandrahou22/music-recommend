"""
权重调优结果验证测试脚本
对比默认权重与调优后权重
内部用户最优权重: (0.4, 0.1, 0.1, 0.1, 0.1)  # (itemcf, usercf, content, mf, sentiment)
外部用户最优权重: (0.4, 0.1, 0.3, 0.0, 0.0)
"""

import sys
import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from config import Config
DATASET_DIR = str(Config.DATASET_DIR)
if DATASET_DIR not in sys.path:
    sys.path.insert(0, DATASET_DIR)

from separated_music_recommender import SeparatedMusicRecommender
import random
import numpy as np

def set_random_seed(seed=42):
    random.seed(seed)
    np.random.seed(seed)

def evaluate_weights(sub_recommender, users, weights, n=10):
    """
    评估指定权重组合的性能
    weights: (w_itemcf, w_usercf, w_content, w_mf, w_sentiment)
    注意: artist 和 lightfm 权重固定为 0.1 和 0.0（或按默认）
    """
    # 固定权重
    w_artist = 0.1
    w_lightfm = 0.0  # LightFM 未启用
    
    w_itemcf, w_usercf, w_content, w_mf, w_sentiment = weights
    
    metrics = {'precision': [], 'recall': [], 'ndcg': [], 'hit': [], 'diversity': [], 'popularity': []}
    
    for uid in users:
        # 获取测试集交互
        test_songs = set(sub_recommender.test_interactions[sub_recommender.test_interactions['user_id'] == uid]['song_id'].tolist())
        if not test_songs:
            continue
        
        # 调用混合推荐（不启用 MMR，以便单独评估权重影响）
        recs = sub_recommender.hybrid_recommendation_parallel(
            uid, n=n, use_mmr=False,
            w_itemcf=w_itemcf, w_usercf=w_usercf,
            w_content=w_content, w_mf=w_mf,
            w_sentiment=w_sentiment,
            w_artist=w_artist, w_lightfm=w_lightfm
        )
        
        if not recs:
            continue
        
        rec_songs = [r[0] for r in recs]
        hits = len(set(rec_songs) & test_songs)
        k_actual = len(rec_songs)
        
        # Precision@K
        metrics['precision'].append(hits / k_actual)
        # Recall@K
        metrics['recall'].append(hits / len(test_songs))
        # HitRate
        metrics['hit'].append(1 if hits > 0 else 0)
        
        # NDCG@K
        dcg = sum(1 / np.log2(idx+2) for idx, s in enumerate(rec_songs) if s in test_songs)
        idcg = sum(1 / np.log2(idx+2) for idx in range(min(len(test_songs), n)))
        metrics['ndcg'].append(dcg / idcg if idcg > 0 else 0)
        
        # Diversity: 不同流派数量 / K
        genres = set()
        for s in rec_songs:
            info = sub_recommender.get_song_info(s)
            if info and info.get('genre'):
                genres.add(info['genre'])
        metrics['diversity'].append(len(genres) / k_actual)
        
        # AvgPopularity
        pops = [sub_recommender.song_popularity.get(s, 50) for s in rec_songs]
        metrics['popularity'].append(np.mean(pops))
    
    # 计算平均值
    avg_metrics = {}
    for key in metrics:
        if metrics[key]:
            avg_metrics[key] = np.mean(metrics[key])
        else:
            avg_metrics[key] = 0
    return avg_metrics

def get_test_users(sub_recommender, n_users=200):
    """获取测试集用户"""
    users = list(sub_recommender.test_interactions['user_id'].unique())
    if len(users) > n_users:
        users = random.sample(users, n_users)
    return users

def main():
    set_random_seed(42)
    
    print("初始化推荐引擎...")
    data_dir = str(Config.DATASET_DIR / "separated_processed_data")
    cache_dir = str(Config.DATASET_DIR / "recommender_cache")
    recommender = SeparatedMusicRecommender(data_dir=data_dir, cache_dir=cache_dir)
    
    # 默认权重
    default_weights_internal = (0.3, 0.2, 0.1, 0.1, 0.1)   # (itemcf, usercf, content, mf, sentiment)
    default_weights_external = (0.3, 0.2, 0.1, 0.1, 0.1)
    
    # 调优后权重
    tuned_weights_internal = (0.4, 0.1, 0.1, 0.1, 0.1)
    tuned_weights_external = (0.4, 0.1, 0.3, 0.0, 0.0)
    
    # 获取测试用户
    internal_users = get_test_users(recommender.internal_recommender, n_users=200)
    external_users = get_test_users(recommender.external_recommender, n_users=200)
    
    print("\n===== 内部用户权重对比 =====")
    print(f"默认权重: {default_weights_internal}")
    res_default_int = evaluate_weights(recommender.internal_recommender, internal_users, default_weights_internal, n=10)
    print(f"调优权重: {tuned_weights_internal}")
    res_tuned_int = evaluate_weights(recommender.internal_recommender, internal_users, tuned_weights_internal, n=10)
    
    print("\n指标对比:")
    print(f"{'指标':<15} {'默认权重':<15} {'调优后权重':<15} {'提升'}")
    for metric in ['precision', 'recall', 'ndcg', 'diversity']:
        default_val = res_default_int[metric]
        tuned_val = res_tuned_int[metric]
        if default_val == 0:
            improvement = "N/A"
        else:
            improvement = f"{(tuned_val - default_val)/default_val*100:+.1f}%"
        print(f"{metric.capitalize():<15} {default_val:.4f}          {tuned_val:.4f}          {improvement}")
    
    print("\n===== 外部用户权重对比 =====")
    print(f"默认权重: {default_weights_external}")
    res_default_ext = evaluate_weights(recommender.external_recommender, external_users, default_weights_external, n=10)
    print(f"调优权重: {tuned_weights_external}")
    res_tuned_ext = evaluate_weights(recommender.external_recommender, external_users, tuned_weights_external, n=10)
    
    print("\n指标对比:")
    print(f"{'指标':<15} {'默认权重':<15} {'调优后权重':<15} {'提升'}")
    for metric in ['precision', 'recall', 'ndcg', 'diversity']:
        default_val = res_default_ext[metric]
        tuned_val = res_tuned_ext[metric]
        if default_val == 0:
            improvement = "N/A"
        else:
            improvement = f"{(tuned_val - default_val)/default_val*100:+.1f}%"
        print(f"{metric.capitalize():<15} {default_val:.4f}          {tuned_val:.4f}          {improvement}")
    
    # 输出表格格式（便于复制到论文）
    print("\n\n========== 论文表格数据 ==========")
    print("用户类型 | 权重配置 | Precision@10 | Recall@10 | NDCG@10 | Diversity")
    print(f"内部用户 | 默认权重 | {res_default_int['precision']:.4f} | {res_default_int['recall']:.4f} | {res_default_int['ndcg']:.4f} | {res_default_int['diversity']:.4f}")
    print(f"内部用户 | 调优后   | {res_tuned_int['precision']:.4f} | {res_tuned_int['recall']:.4f} | {res_tuned_int['ndcg']:.4f} | {res_tuned_int['diversity']:.4f}")
    print(f"外部用户 | 默认权重 | {res_default_ext['precision']:.4f} | {res_default_ext['recall']:.4f} | {res_default_ext['ndcg']:.4f} | {res_default_ext['diversity']:.4f}")
    print(f"外部用户 | 调优后   | {res_tuned_ext['precision']:.4f} | {res_tuned_ext['recall']:.4f} | {res_tuned_ext['ndcg']:.4f} | {res_tuned_ext['diversity']:.4f}")
    print("=================================")

if __name__ == "__main__":
    main()