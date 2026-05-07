import pickle
import os

# 正确路径
cache_file = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\recommender_cache\internal\internal\user_sim.pkl"

if not os.path.exists(cache_file):
    print(f"文件不存在: {cache_file}")
else:
    with open(cache_file, 'rb') as f:
        user_similarities = pickle.load(f)

    first_user = list(user_similarities.keys())[0]
    neighbors = user_similarities[first_user]

    print(f"用户ID: {first_user}")
    print(f"相似邻居数: {len(neighbors)}\n")
    for i, (nid, score) in enumerate(list(neighbors.items())[:15], 1):
        print(f"{i:>2}. {nid}   {score:.4f}")