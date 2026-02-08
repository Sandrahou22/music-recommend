# utils/sentiment_analyzer.py
import re
import jieba
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

class MusicSentimentAnalyzer:
    """专门针对音乐评论的情感分析器"""
    
    def __init__(self):
        self.music_positive_phrases = [
            # 核心正面短语
            '太好听了', '好听好听', '真好听', '很好听', '非常好听',
            '超级好听', '特别好听', '太好听', '最好了', '最好听',
            '超级棒', '非常棒', '特别棒', '太棒了', '很棒',
            # 强度+正面
            '非常喜欢', '特别喜欢', '超级喜欢', '太喜欢了', '很喜欢',
            '非常爱', '特别爱', '超级爱', '太爱了', '很爱',
            # 专业评价
            '旋律优美', '节奏感强', '音色很美', '歌声动人', '唱功很好',
            '编曲精彩', '制作精良', '录音很好', '混音很棒',
            # 情感表达
            '感动哭了', '听得想哭', '触动心灵', '深入人心', '震撼人心',
            '回味无穷', '余音绕梁', '如痴如醉', '陶醉其中',
            # 行为表达
            '单曲循环', '循环播放', '无限循环', '收藏了', '加入歌单',
            '分享给朋友', '推荐给大家', '必须收藏', '永久收藏'
        ]
        
        self.general_positive_words = set([
            '喜欢', '爱', '棒', '好', '美', '赞', '优', '佳', '精', '彩',
            '妙', '强', '牛', '顶', '帅', '酷', '爽', 'high', 'nice'
        ])
        
        self.negative_phrases = [
            '难听', '不好听', '难听死了', '太难听', '特别难听',
            '超级难听', '非常难听', '不好听', '不喜欢', '讨厌',
            '垃圾', '差劲', '糟糕', '失望', '无聊', '恶心'
        ]
        
        self.intensity_words = {
            '非常': 2.0, '特别': 2.0, '超级': 2.0, '极其': 2.0, '极度': 2.0,
            '十分': 1.8, '相当': 1.8, '很': 1.5, '太': 1.5, '挺': 1.3,
            '有点': 1.2, '略微': 1.1, '稍微': 1.1
        }
        
        self.negation_words = set(['不', '没', '无', '非', '未', '否', '莫', '勿'])
        
        # 构建正则模式
        self.positive_patterns = [
            re.compile(r'太.*好听了?'),
            re.compile(r'最.*好听了?'),
            re.compile(r'(非常|特别|超级|极其).*好听了?'),
            re.compile(r'(真|确实|真的).*好听了?'),
            re.compile(r'好听到.*哭'),
            re.compile(r'听.*遍都不腻'),
            re.compile(r'百听不厌'),
            re.compile(r'耳朵怀孕'),
            re.compile(r'单曲循环.*天'),
        ]
        
        # 初始化jieba分词
        for phrase in self.music_positive_phrases + self.negative_phrases:
            jieba.add_word(phrase)
    
    def analyze(self, text: str) -> Tuple[float, bool]:
        """分析文本情感，返回情感分数和是否正面"""
        if not text or not text.strip():
            return 0.5, None
        
        text = text.strip().lower()
        
        # 1. 检查特殊正向模式
        special_positive_score = 0
        for pattern in self.positive_patterns:
            if pattern.search(text):
                special_positive_score += 2.0
        
        # 2. 分词
        words = list(jieba.cut(text))
        
        # 3. 情感分析
        positive_score = 0
        negative_score = 0
        
        i = 0
        while i < len(words):
            word = words[i]
            
            # 检查否定词
            is_negated = word in self.negation_words
            
            # 检查组合短语
            if i < len(words) - 1:
                two_word = word + words[i+1]
                if two_word in self.music_positive_phrases:
                    if is_negated:
                        negative_score += 1.0
                    else:
                        positive_score += 2.0
                    i += 2
                    continue
                elif two_word in self.negative_phrases:
                    if is_negated:
                        positive_score += 1.0
                    else:
                        negative_score += 2.0
                    i += 2
                    continue
            
            # 检查单个词
            if word in self.general_positive_words:
                if is_negated:
                    negative_score += 0.5
                else:
                    positive_score += 1.0
            elif word in self.negative_phrases:
                if is_negated:
                    positive_score += 0.5
                else:
                    negative_score += 1.0
            
            i += 1
        
        # 添加特殊模式分数
        positive_score += special_positive_score
        
        # 计算最终分数
        total = positive_score + negative_score + 0.01  # 避免除零
        
        if total == 0:
            return 0.5, None
        
        sentiment = positive_score / total
        
        # 判断极性
        is_positive = True if sentiment > 0.6 else (False if sentiment < 0.4 else None)
        
        return round(sentiment, 3), is_positive

# 全局实例
sentiment_analyzer = MusicSentimentAnalyzer()

def analyze_music_sentiment(text: str) -> Tuple[float, bool]:
    """分析音乐评论情感"""
    return sentiment_analyzer.analyze(text)