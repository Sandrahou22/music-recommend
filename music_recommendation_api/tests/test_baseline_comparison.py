"""
基线模型对比测试脚本
对比算法: ItemCF, UserCF, Content, MF, Hybrid, Hybrid+MMR
"""

import sys
import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from config import Config
DATASET_DIR = str(Config.DATASET_DIR)
if DATASET_DIR not in sys.path:
    sys.path.insert(0, DATASET_DIR)

from separated_music_recommender import SeparatedMusicRecommender, SeparatedRecommenderEvaluator
import random
import numpy as np

def set_random_seed(seed=42):
    random.seed(seed)
    np.random.seed(seed)

def run_baseline_evaluation():
    """运行基线模型对比"""
    set_random_seed(42)
    
    print("初始化推荐引擎...")
    data_dir = str(Config.DATASET_DIR / "separated_processed_data")
    cache_dir = str(Config.DATASET_DIR / "recommender_cache")
    recommender = SeparatedMusicRecommender(data_dir=data_dir, cache_dir=cache_dir)
    
    # 定义权重配置
    # 单一算法权重: 将对应算法权重设为1，其余为0
    weights_itemcf = (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)  # ItemCF
    weights_usercf = (0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0)  # UserCF
    weights_content = (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0)  # Content
    weights_mf = (0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0)       # MF
    weights_hybrid = (0.4, 0.1, 0.2, 0.2, 0.0, 0.1, 0.0)   # Hybrid (与您代码一致)
    # Hybrid+MMR 使用相同权重，但内部会启用 MMR
    
    # 注意: 对于外部用户，情感权重应强制为0，但评估器内部会根据 source_type 自动处理
    # 我们传入的权重将作为 internal 和 external 的统一初始值，外部用户会自动重新归一化并忽略情感
    
    results = {}
    
    # 定义要评估的算法配置
    algorithms = {
        'ItemCF': weights_itemcf,
        'UserCF': weights_usercf,
        'Content': weights_content,
        'MF': weights_mf,
        'Hybrid': weights_hybrid,
    }
    
    # 评估器: 不保存推荐结果到数据库（避免干扰）
    evaluator = SeparatedRecommenderEvaluator(recommender, save_to_sql=False)
    
    for algo_name, weights in algorithms.items():
        print(f"\n===== 评估算法: {algo_name} =====")
        # 对于 Hybrid+MMR，需要单独处理（MMR 在内部通过 use_mmr 控制）
        # 但 evaluate 方法中 Hybrid 默认 use_mmr=False，要测试 MMR 需要单独调用
        # 我们先用 evaluate 方法测试非 MMR 版本
        if algo_name == 'Hybrid':
            # 先测试不带 MMR 的 Hybrid
            res = evaluator.evaluate(
                n_users=300, k=10, save_recs=False,
                internal_weights=weights,
                external_weights=weights,
                min_interactions=5
            )
            results[algo_name] = res
        else:
            res = evaluator.evaluate(
                n_users=300, k=10, save_recs=False,
                internal_weights=weights,
                external_weights=weights,
                min_interactions=5
            )
            results[algo_name] = res
    
    # 单独测试 Hybrid+MMR (需要自定义评估，因为 evaluate 方法中 use_mmr 是 False)
    print(f"\n===== 评估算法: Hybrid+MMR =====")
    # 手动调用 evaluate 并强制启用 MMR (需修改 evaluator 或直接调用子推荐器)
    # 简便方法: 重新定义一个临时评估函数，内部调用时设置 use_mmr=True
    def eval_with_mmr(source_type, weights, n_users=300, k=10):
        sub_rec = recommender.internal_recommender if source_type == 'internal' else recommender.external_recommender
        # 获取测试用户
        test_users = list(sub_rec.test_interactions['user_id'].unique())
        if len(test_users) > n_users:
            test_users = random.sample(test_users, n_users)
        metrics = {'precision': [], 'recall': [], 'ndcg': [], 'hit': [], 'diversity': [], 'coverage': set(), 'popularity': []}
        for uid in test_users:
            test_songs = set(sub_rec.test_interactions[sub_rec.test_interactions['user_id'] == uid]['song_id'].tolist())
            if not test_songs:
                continue
            # 调用 hybrid_recommendation_parallel 并启用 MMR
            recs = sub_rec.hybrid_recommendation_parallel(
                uid, n=k, use_mmr=True,
                w_itemcf=weights[0], w_usercf=weights[1], w_content=weights[2],
                w_mf=weights[3], w_sentiment=weights[4], w_artist=weights[5], w_lightfm=weights[6]
            )
            if not recs:
                continue
            rec_songs = [r[0] for r in recs]
            hits = len(set(rec_songs) & test_songs)
            k_actual = len(rec_songs)
            metrics['precision'].append(hits / k_actual)
            metrics['recall'].append(hits / len(test_songs))
            metrics['hit'].append(1 if hits > 0 else 0)
            # NDCG
            dcg = sum(1 / np.log2(idx+2) for idx, s in enumerate(rec_songs) if s in test_songs)
            idcg = sum(1 / np.log2(idx+2) for idx in range(min(len(test_songs), k)))
            metrics['ndcg'].append(dcg / idcg if idcg > 0 else 0)
            # Diversity
            genres = set()
            for s in rec_songs:
                info = sub_rec.get_song_info(s)
                if info and info.get('genre'):
                    genres.add(info['genre'])
            metrics['diversity'].append(len(genres) / k_actual)
            # Coverage
            metrics['coverage'].update(rec_songs)
            # Popularity
            pops = [sub_rec.song_popularity.get(s, 50) for s in rec_songs]
            metrics['popularity'].append(np.mean(pops))
        avg_metrics = {}
        for key in ['precision', 'recall', 'ndcg', 'hit', 'diversity', 'popularity']:
            avg_metrics[key] = np.mean(metrics[key]) if metrics[key] else 0
        avg_metrics['coverage'] = len(metrics['coverage']) / sub_rec.n_songs if sub_rec.n_songs else 0
        return avg_metrics
    
    internal_weights_hybrid = weights_hybrid
    external_weights_hybrid = weights_hybrid
    # 外部用户情感权重会被内部置0，无需修改
    
    res_internal_mmr = eval_with_mmr('internal', internal_weights_hybrid, n_users=300, k=10)
    res_external_mmr = eval_with_mmr('external', external_weights_hybrid, n_users=300, k=10)
    
    results['Hybrid+MMR'] = {'internal': res_internal_mmr, 'external': res_external_mmr}
    
    # 输出结果表格
    print("\n\n================== 基线模型对比结果 ==================")
    print("算法\t\t用户类型\tPrecision@10\tRecall@10\tHitRate@10\tNDCG@10\tDiversity\tCoverage\tAvgPopularity")
    for algo_name, res in results.items():
        if algo_name == 'Hybrid+MMR':
            for src in ['internal', 'external']:
                m = res[src]
                print(f"{algo_name}\t{src.upper()}\t{m['precision']:.4f}\t\t{m['recall']:.4f}\t\t{m['hit']:.4f}\t\t{m['ndcg']:.4f}\t{m['diversity']:.4f}\t{m['coverage']:.4f}\t{m['popularity']:.1f}")
        else:
            for src in ['internal', 'external']:
                m = res[src]
                print(f"{algo_name}\t{src.upper()}\t{m['precision']:.4f}\t\t{m['recall']:.4f}\t\t{m['hit']:.4f}\t\t{m['ndcg']:.4f}\t{m['diversity']:.4f}\t{m['coverage']:.4f}\t{m['popularity']:.1f}")
    print("========================================================")

if __name__ == "__main__":
    run_baseline_evaluation()