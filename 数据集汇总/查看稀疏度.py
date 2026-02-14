import pandas as pd
import numpy as np
import os

def diagnose_data_sparsity():
    """诊断数据稀疏度"""
    print("="*80)
    print("📊 数据稀疏度诊断")
    print("="*80)
    
    # 1. 加载内部数据集
    print("\n1. 内部数据集（网易云）:")
    try:
        internal_songs = pd.read_csv("all_songs.csv")
        internal_users = pd.read_csv("用户数据_20260124_200012.csv")
        internal_interactions = pd.read_csv("user_play_history_20260120_132245.csv")
        
        print(f"   歌曲数: {len(internal_songs):,}")
        print(f"   用户数: {len(internal_users):,}")
        print(f"   交互数: {len(internal_interactions):,}")
        
        # 计算稀疏度
        n_users = internal_users['user_id'].nunique()
        n_songs = internal_songs['song_id'].nunique()
        sparsity = 1 - len(internal_interactions) / (n_users * n_songs)
        print(f"   理论最大交互: {n_users * n_songs:,}")
        print(f"   实际交互/最大交互: {len(internal_interactions)/(n_users*n_songs):.4%}")
        print(f"   稀疏度: {sparsity:.4f}")
        
        # 用户行为统计
        user_counts = internal_interactions.groupby('user_id').size()
        print(f"   平均每用户交互数: {user_counts.mean():.2f}")
        print(f"   中位数: {user_counts.median()}")
        print(f"   <5首的用户比例: {(user_counts < 5).sum()/len(user_counts):.2%}")
        
        # 歌曲被交互统计
        song_counts = internal_interactions.groupby('song_id').size()
        print(f"   平均每歌曲交互数: {song_counts.mean():.2f}")
        print(f"   <3个用户的歌曲比例: {(song_counts < 3).sum()/len(song_counts):.2%}")
        
    except Exception as e:
        print(f"   内部数据加载失败: {e}")
    
    # 2. 加载外部数据集
    print("\n2. 外部数据集（Spotify/Last.fm）:")
    try:
        external_music = pd.read_csv("Music Info.csv")
        external_history = pd.read_csv("User Listening History.csv")
        
        print(f"   歌曲数: {len(external_music):,}")
        print(f"   用户数: {external_history['user_id'].nunique():,}")
        print(f"   交互数: {len(external_history):,}")
        
        # 采样检查
        if len(external_history) > 1000000:
            external_history = external_history.sample(1000000, random_state=42)
        
        n_users_ext = external_history['user_id'].nunique()
        n_songs_ext = external_history['track_id'].nunique()
        sparsity_ext = 1 - len(external_history) / (n_users_ext * n_songs_ext)
        print(f"   理论最大交互: {n_users_ext * n_songs_ext:,}")
        print(f"   实际交互/最大交互: {len(external_history)/(n_users_ext*n_songs_ext):.4%}")
        print(f"   稀疏度: {sparsity_ext:.4f}")
        
    except Exception as e:
        print(f"   外部数据加载失败: {e}")
    
    print("\n" + "="*80)
    print("🎯 诊断完成")
    print("="*80)

if __name__ == "__main__":
    diagnose_data_sparsity()