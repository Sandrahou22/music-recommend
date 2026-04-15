"""
多样性分析测试脚本
对比 MMR 开启前后的推荐列表多样性
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

def get_active_users(recommender, source_type='internal', min_interactions=10, n_users=50):
    """获取活跃用户（交互次数不少于 min_interactions）"""
    if source_type == 'internal':
        sub_rec = recommender.internal_recommender
    else:
        sub_rec = recommender.external_recommender
    # 从 user_to_idx 中获取用户列表
    users = list(sub_rec.user_to_idx.keys())
    # 随机采样
    if len(users) > n_users:
        users = random.sample(users, n_users)
    return users

def evaluate_diversity(sub_recommender, users, n=10):
    """
    评估 MMR 开启前后的多样性
    返回: (diversity_no_mmr, diversity_with_mmr)
    """
    div_no_mmr = []
    div_with_mmr = []
    
    for uid in users:
        # 不启用 MMR
        recs_no = sub_recommender.hybrid_recommendation_parallel(
            uid, n=n, use_mmr=False,
            w_itemcf=0.4, w_usercf=0.1, w_content=0.2, w_mf=0.2, w_sentiment=0.0, w_artist=0.1, w_lightfm=0.0
        )
        # 启用 MMR
        recs_mmr = sub_recommender.hybrid_recommendation_parallel(
            uid, n=n, use_mmr=True,
            w_itemcf=0.4, w_usercf=0.1, w_content=0.2, w_mf=0.2, w_sentiment=0.0, w_artist=0.1, w_lightfm=0.0
        )
        
        # 计算多样性：推荐列表中不同流派的数量 / n
        genres_no = set()
        for sid, _ in recs_no:
            info = sub_recommender.get_song_info(sid)
            if info and info.get('genre'):
                genres_no.add(info['genre'])
        div_no_mmr.append(len(genres_no) / n)
        
        genres_mmr = set()
        for sid, _ in recs_mmr:
            info = sub_recommender.get_song_info(sid)
            if info and info.get('genre'):
                genres_mmr.add(info['genre'])
        div_with_mmr.append(len(genres_mmr) / n)
    
    avg_no = sum(div_no_mmr) / len(div_no_mmr) if div_no_mmr else 0
    avg_mmr = sum(div_with_mmr) / len(div_with_mmr) if div_with_mmr else 0
    return avg_no, avg_mmr

def main():
    print("初始化推荐引擎...")
    data_dir = str(Config.DATASET_DIR / "separated_processed_data")
    cache_dir = str(Config.DATASET_DIR / "recommender_cache")
    recommender = SeparatedMusicRecommender(data_dir=data_dir, cache_dir=cache_dir)
    
    # 内部用户多样性对比
    print("\n===== 内部用户多样性分析 (MMR 对比) =====")
    internal_users = get_active_users(recommender, 'internal', min_interactions=10, n_users=50)
    if internal_users:
        div_no, div_mmr = evaluate_diversity(recommender.internal_recommender, internal_users, n=10)
        print(f"未启用 MMR 平均多样性: {div_no:.4f}")
        print(f"启用 MMR 平均多样性: {div_mmr:.4f}")
        print(f"多样性提升: {(div_mmr - div_no)/div_no*100:.1f}%")
    else:
        print("未找到内部活跃用户")
    
    # 外部用户多样性对比
    print("\n===== 外部用户多样性分析 (MMR 对比) =====")
    external_users = get_active_users(recommender, 'external', min_interactions=10, n_users=50)
    if external_users:
        div_no, div_mmr = evaluate_diversity(recommender.external_recommender, external_users, n=10)
        print(f"未启用 MMR 平均多样性: {div_no:.4f}")
        print(f"启用 MMR 平均多样性: {div_mmr:.4f}")
        print(f"多样性提升: {(div_mmr - div_no)/div_no*100:.1f}%")
    else:
        print("未找到外部活跃用户")

if __name__ == "__main__":
    main()