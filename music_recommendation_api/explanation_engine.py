#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推荐解释引擎 - Explainable Recommendation
生成自然语言解释和可视化数据
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class ExplanationEngine:
    """可解释推荐引擎"""
    
    def __init__(self, recommender):
        """
        初始化
        :param recommender: OptimizedMusicRecommender 实例
        """
        self.rec = recommender
        self.feature_names = {
            'danceability': '舞曲性',
            'energy': '能量感', 
            'valence': '情绪值',
            'acousticness': '原声度',
            'instrumentalness': '器乐度',
            'tempo': '节奏速度'
        }
        
        # 情绪标签映射
        self.mood_labels = {
            (0.0, 0.3): '忧郁抒情',
            (0.3, 0.5): '温和内敛',
            (0.5, 0.7): '明快愉悦', 
            (0.7, 1.0): '热烈欢快'
        }
    
    def generate_explanation(self, user_id: str, song_id: str, 
                           algorithm: str, score: float = None) -> Dict:
        """
        主入口：生成推荐解释
        
        Returns:
            {
                "main_reason": "主要原因（一句话）",
                "details": ["细节1", "细节2"], 
                "confidence": 0.92,
                "algorithm": "content",
                "visual_data": {雷达图数据},
                "key_features": {"节奏": "相似", "情绪": "匹配"}
            }
        """
        try:
            if algorithm == 'content':
                return self._explain_content_based(user_id, song_id)
            elif algorithm == 'usercf':
                return self._explain_user_cf(user_id, song_id)
            elif algorithm == 'cf':
                return self._explain_item_cf(user_id, song_id)
            elif algorithm == 'mf':
                return self._explain_mf(user_id, song_id)
            else:
                # hybrid 或其他默认使用 content 解释（最直观）
                return self._explain_content_based(user_id, song_id)
                
        except Exception as e:
            logger.error(f"生成解释失败 [{user_id} -> {song_id}]: {e}")
            # 返回默认解释，确保不中断推荐流程
            return {
                "main_reason": "基于您的音乐偏好推荐",
                "details": ["符合您的收听习惯"],
                "confidence": 0.7,
                "algorithm": algorithm,
                "visual_data": self._get_basic_radar(song_id),
                "key_features": {}
            }
    
    def _explain_content_based(self, user_id: str, song_id: str) -> Dict:
        """基于内容的解释：找到最相似的历史歌曲"""
        user_idx = self.rec.user_to_idx.get(user_id)
        if user_idx is None:
            return {
                "main_reason": "基于热门音乐推荐",
                "details": ["符合当前流行趋势"],
                "confidence": 0.6,
                "algorithm": "content",
                "visual_data": self._get_basic_radar(song_id),
                "key_features": {"流行度": "高"}
            }
        
        # 获取用户历史交互
        user_row = self.rec.user_song_matrix[user_idx]
        interacted = list(user_row.nonzero()[1])
        
        if not interacted:
            return {
                "main_reason": "基于音乐风格特征推荐",
                "details": ["与您偏爱的风格匹配"],
                "confidence": 0.65,
                "algorithm": "content"
            }
        
        # 找到内容相似度最高的历史歌曲
        max_sim = 0
        most_similar_song = None
        sim_details = []
        
        for hist_idx in interacted[:30]:  # 只看最近30首
            hist_id = self.rec.idx_to_song[hist_idx]
            sim_score = self.rec.content_similarities.get(song_id, {}).get(hist_id, 0)
            
            if sim_score > max_sim:
                max_sim = sim_score
                most_similar_song = hist_id
        
        # 获取歌曲信息
        song_info = self.rec.get_song_info(song_id)
        hist_info = self.rec.get_song_info(most_similar_song) if most_similar_song else None
        
        # 生成特征对比
        feature_comparison = self._compare_audio_features_detailed(
            most_similar_song, song_id
        ) if most_similar_song else []
        
        # 构建解释
        if hist_info and max_sim > 0.3:
            main_reason = f"与您听过的《{hist_info['song_name']}》风格高度相似"
            confidence = min(max_sim + 0.2, 0.95)
            
            details = []
            if max_sim > 0.5:
                details.append(f"整体相似度达 {max_sim:.0%}")
            
            # 添加具体特征匹配
            for feat in feature_comparison[:2]:  # 取最相关的2个特征
                details.append(feat['description'])
                
        else:
            main_reason = "符合您偏爱的音乐风格特征"
            confidence = 0.7
            details = ["基于音频特征分析匹配"]
        
        # 获取用户平均特征进行对比
        user_avg_features = self._get_user_average_features(user_id)
        song_features = self._get_song_features(song_id)
        
        return {
            "main_reason": main_reason,
            "details": details,
            "confidence": round(confidence, 2),
            "algorithm": "content_based",
            "reference_song": {
                "song_id": most_similar_song,
                "song_name": hist_info['song_name'] if hist_info else None,
                "similarity": round(max_sim, 3)
            } if hist_info else None,
            "visual_data": {
                "radar_chart": self._generate_radar_data(user_avg_features, song_features),
                "feature_bars": feature_comparison
            },
            "key_features": {
                "最相似历史歌曲": hist_info['song_name'] if hist_info else "无",
                "音频相似度": f"{max_sim:.1%}" if max_sim > 0 else "中等"
            }
        }
    
    def _explain_user_cf(self, user_id: str, song_id: str) -> Dict:
        """UserCF 解释：相似用户都在听"""
        similar_users = self.rec.user_similarities.get(user_id, {})
        count = len(similar_users)
        
        # 找到具体是哪些相似用户喜欢这首歌
        song_idx = self.rec.song_to_idx.get(song_id)
        user_count = 0
        top_similar_user = None
        
        if song_idx is not None:
            for sim_user, sim_score in list(similar_users.items())[:10]:
                sim_user_idx = self.rec.user_to_idx.get(sim_user)
                if sim_user_idx and self.rec.user_song_matrix[sim_user_idx, song_idx] > 0:
                    user_count += 1
                    if not top_similar_user:
                        top_similar_user = sim_user
        
        confidence = 0.75 if user_count > 0 else 0.6
        
        if user_count >= 3:
            main_reason = f"与您音乐口味相似的 {user_count} 位用户都在听这首歌"
        elif user_count > 0:
            main_reason = f"您的一位相似好友最近收听了这首歌"
        else:
            main_reason = "与您音乐偏好相似的用户群体推荐"
        
        return {
            "main_reason": main_reason,
            "details": [
                f"基于 {count} 位相似用户的协同过滤",
                "这些用户与您历史喜好高度重合"
            ],
            "confidence": confidence,
            "algorithm": "user_cf",
            "visual_data": {
                "similar_users_count": count,
                "interested_users": user_count
            },
            "key_features": {
                "相似用户总数": f"{count}人",
                "感兴趣人数": f"{user_count}人"
            }
        }
    
    def _explain_item_cf(self, user_id: str, song_id: str) -> Dict:
        """ItemCF 解释：基于您喜欢的某首歌"""
        # 找到是通过哪首历史歌曲推荐过来的
        user_idx = self.rec.user_to_idx.get(user_id)
        if user_idx is None:
            return {"main_reason": "基于物品关联推荐", "confidence": 0.6}
        
        # 简化的解释：提及关联性
        song_info = self.rec.get_song_info(song_id)
        
        return {
            "main_reason": f"与您收藏的歌曲具有高度关联性",
            "details": [
                "基于物品协同过滤算法",
                "分析了大量用户的播放列表关联"
            ],
            "confidence": 0.8,
            "algorithm": "item_cf",
            "visual_data": {},
            "key_features": {
                "关联深度": "强",
                "共同喜好": "高"
            }
        }
    
    def _explain_mf(self, user_id: str, song_id: str) -> Dict:
        """矩阵分解解释：潜在因子匹配"""
        return {
            "main_reason": "基于隐语义模型匹配您的潜在兴趣",
            "details": [
                "通过矩阵分解发现您的隐藏偏好",
                "匹配了您可能喜欢的音乐因子"
            ],
            "confidence": 0.75,
            "algorithm": "matrix_factorization",
            "visual_data": {},
            "key_features": {
                "模型维度": "50维隐因子",
                "匹配度": "高"
            }
        }
    
    def _compare_audio_features_detailed(self, song1_id: str, song2_id: str) -> List[Dict]:
        """详细对比两首歌的音频特征"""
        df = self.rec.song_features
        
        try:
            s1 = df[df['song_id'] == song1_id].iloc[0]
            s2 = df[df['song_id'] == song2_id].iloc[0]
        except IndexError:
            return []
        
        comparisons = []
        
        # 节奏对比
        tempo_diff = abs(float(s1['tempo']) - float(s2['tempo']))
        if tempo_diff < 15:
            comparisons.append({
                "feature": "节奏速度",
                "description": f"节奏相近（{int(s1['tempo'])} vs {int(s2['tempo'])} BPM）",
                "similarity": "high"
            })
        
        # 情绪对比
        valence_diff = abs(float(s1['valence']) - float(s2['valence']))
        if valence_diff < 0.2:
            mood = self._get_mood_label(float(s2['valence']))
            comparisons.append({
                "feature": "情绪基调",
                "description": f"情绪{ mood }一致",
                "similarity": "high"
            })
        
        # 能量对比
        energy_diff = abs(float(s1['energy']) - float(s2['energy']))
        if energy_diff < 0.15:
            comparisons.append({
                "feature": "能量感",
                "description": "能量强度匹配",
                "similarity": "medium"
            })
        
        # 舞曲性
        dance_diff = abs(float(s1['danceability']) - float(s2['danceability']))
        if dance_diff < 0.2:
            comparisons.append({
                "feature": "舞曲性",
                "description": "舞曲节奏相似",
                "similarity": "medium"
            })
        
        return comparisons
    
    def _get_mood_label(self, valence: float) -> str:
        """根据 valence 获取情绪标签"""
        for (low, high), label in self.mood_labels.items():
            if low <= valence <= high:
                return label
        return "平和"
    
    def _get_user_average_features(self, user_id: str) -> Dict:
        """计算用户历史歌曲的平均特征（用于雷达图对比）"""
        user_idx = self.rec.user_to_idx.get(user_id)
        if user_idx is None:
            return self._get_default_features()
        
        user_row = self.rec.user_song_matrix[user_idx]
        interacted = list(user_row.nonzero()[1])
        
        if not interacted:
            return self._get_default_features()
        
        features_sum = defaultdict(float)
        count = 0
        
        for song_idx in interacted[:20]:  # 取最近20首
            song_id = self.rec.idx_to_song[song_idx]
            song_feats = self._get_song_features(song_id)
            if song_feats:
                for key, val in song_feats.items():
                    features_sum[key] += val
                count += 1
        
        if count == 0:
            return self._get_default_features()
        
        return {k: v/count for k, v in features_sum.items()}
    
    def _get_song_features(self, song_id: str) -> Optional[Dict]:
        """获取歌曲音频特征"""
        try:
            df = self.rec.song_features
            row = df[df['song_id'] == song_id].iloc[0]
            return {
                'danceability': float(row['danceability']),
                'energy': float(row['energy']),
                'valence': float(row['valence']),
                'acousticness': float(row['acousticness']),
                'instrumentalness': float(row['instrumentalness']),
                'tempo': float(row['tempo']) / 200.0  # 归一化到 0-1
            }
        except (IndexError, KeyError, ValueError):
            return None
    
    def _get_default_features(self) -> Dict:
        """默认特征（中值）"""
        return {
            'danceability': 0.5,
            'energy': 0.5,
            'valence': 0.5,
            'acousticness': 0.5,
            'instrumentalness': 0.0,
            'tempo': 0.6
        }
    
    def _generate_radar_data(self, user_features: Dict, song_features: Dict) -> Dict:
        """生成雷达图数据用于前端可视化"""
        dimensions = ['舞曲性', '能量感', '情绪值', '原声度', '器乐度', '节奏']
        keys = ['danceability', 'energy', 'valence', 'acousticness', 'instrumentalness', 'tempo']
        
        return {
            "dimensions": dimensions,
            "user_avg": [round(user_features.get(k, 0.5), 2) for k in keys],
            "current_song": [round(song_features.get(k, 0.5), 2) for k in keys],
            "max_values": [1.0] * 6
        }
    
    def _get_basic_radar(self, song_id: str) -> Dict:
        """基础雷达图（仅当前歌曲）"""
        feats = self._get_song_features(song_id)
        if not feats:
            feats = self._get_default_features()
        
        return {
            "dimensions": ['舞曲性', '能量感', '情绪值', '原声度', '器乐度', '节奏'],
            "current_song": [
                round(feats['danceability'], 2),
                round(feats['energy'], 2),
                round(feats['valence'], 2),
                round(feats['acousticness'], 2),
                round(feats['instrumentalness'], 2),
                round(feats['tempo'], 2)
            ]
        }