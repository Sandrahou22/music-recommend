#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户相似度预计算脚本（方案B）
运行此脚本生成缓存文件，避免Flask启动时计算
"""

import sys
import os
import gc
import pickle
import numpy as np
from datetime import datetime

# 添加数据集目录到路径
dataset_dir = r'C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总'
sys.path.insert(0, dataset_dir)

# 设置环境变量避免编码问题
os.environ['PYTHONUTF8'] = '1'

def main():
    print("="*60)
    print("用户相似度预计算工具")
    print("="*60)
    print(f"开始时间: {datetime.now().strftime('%H:%M:%S')}")
    print("提示: 此过程需要5-10分钟，请耐心等待...")
    print("-"*60)
    
    try:
        # 导入推荐类
        from music_recommender_system_final_improved import OptimizedMusicRecommender
        
        print("[1/4] 初始化推荐系统...")
        print("    正在加载数据（可能需要1-2分钟）...")
        
        # 初始化（会自动加载矩阵和MF）
        rec = OptimizedMusicRecommender(data_dir="aligned_data_optimized")
        
        print(f"[2/4] 系统初始化完成")
        print(f"    用户数: {rec.n_users:,}")
        print(f"    歌曲数: {rec.n_songs:,}")
        
        # 确保MF已计算
        if not hasattr(rec, 'user_factors') or rec.user_factors is None:
            print("[错误] MF向量未计算，无法继续")
            input("按回车键退出...")
            return
        
        print(f"[3/4] 开始计算用户相似度...")
        print(f"    使用MF向量维度: {rec.user_factors.shape}")
        print(f"    计算方式: 余弦相似度（分批处理）")
        
        n_users = rec.user_factors.shape[0]
        user_similarities = {}
        
        # 使用MF向量计算相似度（内存友好）
        # 归一化向量
        print("    正在归一化向量...")
        norms = np.linalg.norm(rec.user_factors, axis=1, keepdims=True)
        norms[norms == 0] = 1
        normalized = rec.user_factors / norms
        
        batch_size = 500  # 每批处理500用户
        total_batches = (n_users + batch_size - 1) // batch_size
        
        start_time = datetime.now()
        
        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, n_users)
            
            # 显示进度
            if batch_idx % 2 == 0 or batch_idx == total_batches - 1:
                progress = (batch_idx / total_batches) * 100
                elapsed = (datetime.now() - start_time).total_seconds()
                eta = (elapsed / (batch_idx + 1)) * (total_batches - batch_idx - 1)
                print(f"\r    进度: {batch_idx+1}/{total_batches} ({progress:.1f}%) | "
                      f"已用: {elapsed:.0f}s | 预计剩余: {eta:.0f}s", end="", flush=True)
            
            # 批量计算余弦相似度
            batch_vectors = normalized[batch_start:batch_end]
            batch_sim = np.dot(batch_vectors, normalized.T)
            
            # 为每个用户保存Top15相似用户
            for local_idx, global_idx in enumerate(range(batch_start, batch_end)):
                user_id = rec.idx_to_user[global_idx]
                scores = batch_sim[local_idx]
                
                # 获取Top15（排除自己）
                # 使用argpartition提高效率
                top_indices = np.argpartition(scores, -15)[-15:]
                top_indices = top_indices[np.argsort(-scores[top_indices])]
                
                neighbors = {}
                for idx in top_indices:
                    if idx != global_idx and scores[idx] > 0.05:  # 阈值过滤
                        neighbor_id = rec.idx_to_user[idx]
                        neighbors[neighbor_id] = float(scores[idx])
                
                if neighbors:
                    user_similarities[user_id] = neighbors
            
            # 每10批强制垃圾回收
            if batch_idx % 10 == 0:
                gc.collect()
        
        print(f"\n[4/4] 计算完成！")
        print(f"    共计算 {len(user_similarities)} 个用户的相似度")
        print(f"    平均每用户邻居数: {np.mean([len(v) for v in user_similarities.values()]):.1f}")
        
        # 保存缓存
        cache_file = os.path.join(rec.cache_dir, "user_sim.pkl")
        print(f"\n正在保存缓存到: {cache_file}")
        
        with open(cache_file, 'wb') as f:
            pickle.dump(user_similarities, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        # 验证文件
        file_size = os.path.getsize(cache_file) / (1024*1024)  # MB
        print(f"✓ 保存成功！文件大小: {file_size:.2f} MB")
        
        # 测试读取
        print("验证缓存文件...")
        with open(cache_file, 'rb') as f:
            test_data = pickle.load(f)
        print(f"✓ 验证通过，包含 {len(test_data)} 条记录")
        
        print("-"*60)
        print("预计算完成！现在可以启动Flask服务了。")
        print("服务启动时会自动加载此缓存，无需等待。")
        
    except Exception as e:
        print(f"\n[错误] 计算失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("="*60)
    input("\n按回车键退出...")

if __name__ == '__main__':
    main()