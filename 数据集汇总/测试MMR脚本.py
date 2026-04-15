import sys
sys.path.append('你的项目路径')
from separated_music_recommender import SeparatedMusicRecommender

recommender = SeparatedMusicRecommender(data_dir="separated_processed_data", cache_dir="recommender_cache")
user_id = "U000001"   # 替换为一个真实内部用户ID

# 不启用MMR
recs_no_mmr = recommender.internal_recommender.hybrid_recommendation_parallel(
    user_id, n=10, use_mmr=False,
    w_itemcf=0.4, w_usercf=0.1, w_content=0.2, w_mf=0.2, w_sentiment=0.0, w_artist=0.1, w_lightfm=0.0
)

# 启用MMR
recs_mmr = recommender.internal_recommender.hybrid_recommendation_parallel(
    user_id, n=10, use_mmr=True,
    w_itemcf=0.4, w_usercf=0.1, w_content=0.2, w_mf=0.2, w_sentiment=0.0, w_artist=0.1, w_lightfm=0.0
)

print("未启用MMR推荐列表：")
for i, (sid, score) in enumerate(recs_no_mmr[:10], 1):
    info = recommender.get_song_info(sid)
    print(f"{i}. {info['song_name']} - {info['artists']} ({info['genre']})")

print("\n启用MMR推荐列表：")
for i, (sid, score) in enumerate(recs_mmr[:10], 1):
    info = recommender.get_song_info(sid)
    print(f"{i}. {info['song_name']} - {info['artists']} ({info['genre']})")