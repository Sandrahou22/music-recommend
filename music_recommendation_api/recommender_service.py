import os
import sys
import logging
import threading  # 【添加这一行】
import time
from functools import wraps, lru_cache
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any  # 【添加 Any】
from enum import Enum
import importlib.util

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# 导入配置
from config import Config

logger = logging.getLogger(__name__)

class InitStatus(Enum):
    """初始化状态机"""
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    FAILED = "failed"
    DEGRADED = "degraded"  # 降级模式（使用兜底推荐）

class CircuitBreaker:
    """熔断器：防止级联故障"""
    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "closed"  # closed, open, half-open
        self._lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        """执行函数，监控失败"""
        with self._lock:
            if self.state == "open":
                if datetime.now() - self.last_failure_time > timedelta(seconds=self.timeout):
                    self.state = "half-open"
                    logger.info("熔断器进入半开状态，尝试恢复")
                else:
                    raise RuntimeError("服务熔断中，请稍后重试")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        with self._lock:
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
                logger.info("熔断器关闭，服务恢复正常")
    
    def _on_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            if self.failure_count >= self.threshold:
                self.state = "open"
                logger.error(f"熔断器打开！连续失败{self.failure_count}次")

def singleton_with_lock(cls):
    """带线程锁的单例装饰器"""
    instances = {}
    locks = {}
    
    @wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            if cls not in locks:
                locks[cls] = threading.RLock()
            with locks[cls]:
                if cls not in instances:
                    instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return get_instance

@singleton_with_lock
class RecommenderService:
    """
    推荐系统服务包装类 - 生产级优化版本
    
    特性：
    1. 线程安全的延迟初始化
    2. 熔断器模式防止雪崩
    3. 多级降级策略
    4. 连接池管理
    5. 内存缓存（可扩展Redis）
    """
    
    def __init__(self):
        self._recommender = None
        self._engine = None
        self._status = InitStatus.UNINITIALIZED
        self._init_lock = threading.RLock()
        self._init_error: Optional[str] = None
        self._valid_users: set = set()
        self._module = None
        self._circuit_breaker = CircuitBreaker(
            threshold=Config.CIRCUIT_BREAKER_THRESHOLD,
            timeout=Config.CIRCUIT_BREAKER_TIMEOUT
        )
        
        # 兜底数据（当推荐引擎完全失败时使用）
        self._fallback_hot_songs: List[Dict] = []
        self._last_fallback_update = 0

    
        
    @property
    def is_healthy(self) -> bool:
        """健康状态检查"""
        return self._status in (InitStatus.INITIALIZED, InitStatus.DEGRADED)
    
    @property
    def is_ready(self) -> bool:
        """是否可提供服务（包括降级模式）"""
        return self._status in (
            InitStatus.INITIALIZED, 
            InitStatus.DEGRADED, 
            InitStatus.FAILED
        ) and len(self._fallback_hot_songs) > 0
    
    def initialize(self, force: bool = False) -> bool:
        """
        线程安全的初始化
        
        Args:
            force: 是否强制重新初始化
        """
        if self._status == InitStatus.INITIALIZED and not force:
            return True
        
        if self._status == InitStatus.INITIALIZING:
            # 等待其他线程初始化完成（最多30秒）
            for _ in range(30):
                if self._status != InitStatus.INITIALIZING:
                    return self._status == InitStatus.INITIALIZED
                time.sleep(1)
            return False
        
        with self._init_lock:
            if self._status == InitStatus.INITIALIZING:
                return False
            
            self._status = InitStatus.INITIALIZING
            logger.info("="*60)
            logger.info("【开始初始化推荐系统】")
            start_time = datetime.now()
            
            try:
                # 1. 先建立数据库连接池（推荐引擎依赖）
                self._setup_database()
                
                # 2. 动态加载推荐模块
                self._load_recommender_module()
                
                # 3. 初始化推荐引擎
                self._initialize_engine()
                
                # 4. 预加载兜底数据
                self._refresh_fallback_data()
                
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"✓ 初始化完成，耗时: {elapsed:.2f}秒")
                self._status = InitStatus.INITIALIZED
                return True
                
            # 在 recommender_service.py 的 initialize 方法中，修改 except 块：
            except Exception as e:
                import traceback
                self._init_error = str(e)
                logger.error(f"✗ 初始化失败: {e}\n{traceback.format_exc()}")  # 增加堆栈打印
                
                # 尝试加载本地缓存作为降级
                self._try_degraded_mode()
                return False
    
    def _setup_database(self):
        """建立数据库连接池"""
        if self._engine is None:
            self._engine = create_engine(
                Config.get_db_connection_string(),
                poolclass=QueuePool,
                pool_size=Config.DB_CONFIG['pool_size'],
                max_overflow=Config.DB_CONFIG['max_overflow'],
                pool_recycle=Config.DB_CONFIG['pool_recycle'],
                pool_pre_ping=True,  # 自动检测断开的连接
                echo=False
            )
            logger.info("数据库连接池已建立")
    
    def _load_recommender_module(self):
        """动态加载原始推荐代码"""
        code_path = Config.RECOMMENDER_CODE_PATH
        
        if not code_path.exists():
            raise FileNotFoundError(f"找不到推荐系统代码: {code_path}")
        
        # 添加目录到路径以便导入依赖
        if str(Config.DATASET_DIR) not in sys.path:
            sys.path.insert(0, str(Config.DATASET_DIR))
        
        spec = importlib.util.spec_from_file_location(
            "music_recommender_original", 
            str(code_path)
        )
        self._module = importlib.util.module_from_spec(spec)
        
        # 重定向工作目录（原代码可能依赖相对路径）
        original_cwd = os.getcwd()
        try:
            os.chdir(Config.DATASET_DIR)
            spec.loader.exec_module(self._module)
        finally:
            os.chdir(original_cwd)
        
        logger.info(f"成功加载推荐模块: {code_path.name}")
    
    def _initialize_engine(self):
        """初始化推荐引擎核心"""
        OptimizedMusicRecommender = self._module.OptimizedMusicRecommender
        
        # 使用已存在的对齐数据目录，通过数据库连接
        self._recommender = OptimizedMusicRecommender(
            data_dir="aligned_data_optimized"  # 相对路径，基于DATASET_DIR
        )
        
        # 预构建用户ID集合（O(1)查询）
        self._valid_users = set(self._recommender.user_to_idx.keys())
        
        logger.info(f"推荐引擎就绪: {len(self._valid_users)}用户, "
                   f"{self._recommender.n_songs}歌曲")
    
    def _try_degraded_mode(self):
        """尝试进入降级模式（使用预计算的静态数据）"""
        try:
            # 尝试从本地缓存加载热门歌曲
            cache_file = Config.DATASET_DIR / 'fallback_hot_songs.json'
            if cache_file.exists():
                import json
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self._fallback_hot_songs = json.load(f)
                self._status = InitStatus.DEGRADED
                logger.warning("已进入降级模式：使用静态热门推荐")
        except Exception as e:
            logger.error(f"降级模式加载失败: {e}")
    
    def _refresh_fallback_data(self):
        """刷新兜底数据（定时任务或由初始化调用）"""
        try:
            # 从当前引擎获取热门歌曲
            if self._recommender:
                hot_songs = self.get_hot_songs(tier='all', n=100)
                self._fallback_hot_songs = hot_songs
                self._last_fallback_update = time.time()
                
                # 持久化到本地（供降级模式使用）
                import json
                cache_file = Config.DATASET_DIR / 'fallback_hot_songs.json'
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(hot_songs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"刷新兜底数据失败: {e}")
    
    def _check_initialized(self):
        """检查初始化，自动重试一次"""
        if self._status != InitStatus.INITIALIZED:
            if self._status == InitStatus.FAILED:
                # 失败后自动重试一次
                logger.info("检测到未初始化，尝试自动初始化...")
                if not self.initialize():
                    raise RuntimeError(f"推荐系统初始化失败: {self._init_error}")
            elif self._status == InitStatus.UNINITIALIZED:
                self.initialize()
            
            if self._status not in (InitStatus.INITIALIZED, InitStatus.DEGRADED):
                raise RuntimeError("推荐系统暂不可用，请稍后重试")
    
    def health_check(self) -> Dict[str, Any]:
        """详细健康检查"""
        status = {
            "status": self._status.value,
            "healthy": self.is_healthy,
            "ready": self.is_ready,
            "timestamp": datetime.now().isoformat(),
            "fallback_songs_count": len(self._fallback_hot_songs),
            "circuit_breaker": self._circuit_breaker.state
        }
        
        if self._recommender:
            status.update({
                "users": len(self._valid_users),
                "songs": self._recommender.n_songs
            })
        
        if self._init_error:
            status["last_error"] = self._init_error
            
        return status
    
    @lru_cache(maxsize=128)
    def get_user_profile_cached(self, user_id: str) -> Optional[Dict]:
        """带缓存的用户画像查询（LRU缓存128个用户）"""
        if not self._recommender:
            return None
        return self._recommender.get_user_profile(user_id)
    
    get_user_profile = get_user_profile_cached
    
    def get_recommendations(self, user_id, n=10, algorithm='hybrid'):
        try:
            # 确保系统已初始化
            self._check_initialized()
            
            # 直接调用内部推荐方法，绕过缓存
            result = self._get_recommendations_internal(user_id, n, algorithm)
            return result
            
        except Exception as e:
            logger.error(f"获取推荐失败: {e}")
            # 返回兜底数据
            return self._get_fallback_recommendations(n)
        
    def invalidate_user_cache(self, user_id: str):
        """当用户行为更新时，清除该用户的所有缓存"""
        with self._recommendation_cache._lock:
            keys_to_delete = [k for k in self._recommendation_cache._cache.keys() 
                            if k.startswith(f"{user_id}:")]
            for key in keys_to_delete:
                del self._recommendation_cache._cache[key]
            logger.info(f"清除用户缓存 | user={user_id}, count={len(keys_to_delete)}")
    
    def _get_recommendations_internal(self, user_id: str, n: int, 
                                 algorithm: str) -> List[Dict]:
        """内部推荐逻辑"""
        user_id_str = str(user_id)
        
        # 判断冷启动（O(1)复杂度）
        is_cold_start = user_id_str not in self._valid_users
        
        # 获取用户画像
        profile = self._recommender.get_user_profile(user_id_str) if is_cold_start else \
                self.get_user_profile_cached(user_id_str)
        
        recs = []
        
        if is_cold_start:
            logger.info(f"用户 {user_id} 为冷启动用户")
            recs = self._recommender.get_cold_start_recs(profile, n=n)
            is_cold = True
        else:
            # 【修改】单独处理 usercf 算法
            if algorithm == 'usercf':
                logger.info(f"用户 {user_id} 使用 UserCF 算法")
                # 检查是否有用户相似度数据
                if hasattr(self._recommender, 'user_similarities') and self._recommender.user_similarities:
                    recs = self._recommender.user_based_cf(user_id_str, n=n)
                    is_cold = False
                else:
                    logger.warning("UserCF 相似度未计算，回退到 hybrid")
                    recs = self._recommender.hybrid_recommendation(user_id_str, n=n)
                    is_cold = False
            else:
                # 其他算法走正常分发
                recs = self._get_normal_recommendations(user_id_str, n, algorithm, profile)
                is_cold = False
            
            # 如果个性化推荐为空，回退到冷启动
            if not recs:
                logger.warning(f"用户 {user_id} 个性化推荐为空，回退到冷启动")
                recs = self._recommender.get_cold_start_recs(profile, n=n)
                is_cold = True
        
        # 封装结果
        results = self._format_recommendations(recs, is_cold)
        
        # 如果结果不足，用热门补齐
        if len(results) < n:
            results = self._fill_with_hot_songs(results, n)
        
        return results
    def get_recommendations_with_audio_priority(self, user_id, n=10, algorithm='hybrid'):
        """
        获取推荐，优先返回有音频文件的歌曲（后处理排序，不修改算法分数）
        """
        from datetime import datetime
        
        # 1. 获取正常推荐（使用原始算法逻辑，多取一些作为候选池）
        # 注意：_get_recommendations_internal 返回的是字典列表，不是元组列表！
        candidates = self._get_recommendations_internal(user_id, n=n*3, algorithm=algorithm)
        
        if not candidates:
            return self._get_fallback_recommendations(n)
        
        # 2. 查询这些候选歌曲中哪些有音频（仅用于排序，不改变算法权重）
        try:
            song_ids = [c['song_id'] for c in candidates]  # 从字典中提取 song_id
            # 分批查询避免SQL过长
            audio_songs = set()
            batch_size = 100
            for i in range(0, len(song_ids), batch_size):
                batch = song_ids[i:i+batch_size]
                placeholders = ','.join([f"'{sid}'" for sid in batch])
                query = text(f"""
                    SELECT song_id 
                    FROM enhanced_song_features 
                    WHERE song_id IN ({placeholders}) 
                    AND track_id IS NOT NULL
                """)
                
                with self._engine.connect() as conn:
                    result = conn.execute(query)
                    audio_songs.update({row[0] for row in result})
        except Exception as e:
            logger.warning(f"查询音频状态失败: {e}")
            audio_songs = set()
        
        # 3. 后处理排序：有音频的排在前面，但保持算法内部的相对顺序
        # 注意：candidates 是字典列表，不是元组列表！
        with_audio = [c for c in candidates if c['song_id'] in audio_songs]
        without_audio = [c for c in candidates if c['song_id'] not in audio_songs]
        
        # 组合：先排有音频的（按原算法分数排序），再补无音频的
        final_candidates = (with_audio + without_audio)[:n]
        
        # 4. 添加 has_audio 标记（因为前端需要这个字段）
        for c in final_candidates:
            c['has_audio'] = c['song_id'] in audio_songs
        
        return final_candidates
    
    def _get_normal_recommendations(self, user_id: str, n: int, 
                                algorithm: str, profile: Dict) -> List[Tuple]:
        """正常用户的算法路由"""
        n_songs = profile.get('n_songs', 0) if profile else 0
        
        # 【修改】添加 usercf 支持
        if algorithm == 'usercf':
            # UserCF：基于用户的协同过滤
            if hasattr(self._recommender, 'user_based_cf'):
                return self._recommender.user_based_cf(user_id, n=n)
            else:
                logger.error("原始代码中未找到 user_based_cf 方法")
                return []
        
        elif algorithm == 'cf':
            # ItemCF：基于物品的协同过滤
            return self._recommender.item_based_cf(user_id, n=n)
        
        elif algorithm == 'content':
            # 基于内容的推荐
            return self._recommender.content_based(user_id, n=n)
        
        elif algorithm == 'mf':
            # 矩阵分解
            return self._recommender.matrix_factorization_rec(user_id, n=n)
        
        elif algorithm == 'cold':
            # 强制冷启动
            return self._recommender.get_cold_start_recs(profile, n=n)
        
        else:  # hybrid 或 auto
            # 混合推荐（包含UserCF+ItemCF+CB+MF）
            return self._recommender.hybrid_recommendation(user_id, n=n)
    
    def _format_recommendations(self, recs: List[Tuple], is_cold: bool) -> List[Dict]:
        """格式化推荐结果（包含音频状态）"""
        results = []
        
        # 批量获取歌曲ID
        song_ids = [str(song_id) for song_id, _ in recs]
        
        # 批量查询音频状态
        audio_status_map = self._get_audio_status_batch(song_ids)
        
        for song_id, score in recs:
            try:
                info = self._recommender.get_song_info(song_id)
                if info:
                    song_id_str = str(song_id)
                    has_audio = audio_status_map.get(song_id_str, False)
                    
                    results.append({
                        'song_id': song_id_str,
                        'score': round(float(score), 4),
                        'song_name': info.get('song_name', '未知歌曲'),
                        'artists': info.get('artists', '未知艺术家'),
                        'genre': info.get('genre', 'unknown'),
                        'popularity': int(info.get('popularity', 50)),
                        'cold_start': is_cold,
                        'has_audio': has_audio,  # 【关键】添加音频状态
                        'timestamp': datetime.now().isoformat()
                    })
            except Exception as e:
                logger.warning(f"格式化歌曲 {song_id} 失败: {e}")
                continue
        return results
    
    def _get_audio_status_batch(self, song_ids: List[str]) -> Dict[str, bool]:
        """批量查询歌曲音频状态"""
        if not song_ids or not self._engine:
            return {}
        
        try:
            # 分批查询，避免SQL过长
            batch_size = 50
            audio_status = {}
            
            for i in range(0, len(song_ids), batch_size):
                batch = song_ids[i:i+batch_size]
                placeholders = ', '.join([f"'{sid}'" for sid in batch])
                
                query = f"""
                SELECT song_id, 
                    CASE 
                        WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 
                        ELSE 0 
                    END as has_audio
                FROM enhanced_song_features 
                WHERE song_id IN ({placeholders})
                """
                
                with self._engine.connect() as conn:
                    result = conn.execute(text(query))
                    for row in result:
                        audio_status[row.song_id] = bool(row.has_audio)
            
            return audio_status
        except Exception as e:
            logger.error(f"批量查询音频状态失败: {e}")
            return {}
    
    def _fill_with_hot_songs(self, existing: List[Dict], n: int) -> List[Dict]:
        """用热门歌曲补全推荐列表"""
        existing_ids = {r['song_id'] for r in existing}
        needed = n - len(existing)
        
        if needed <= 0:
            return existing
        
        # 从兜底数据中取
        for song in self._fallback_hot_songs:
            if song['song_id'] not in existing_ids and needed > 0:
                song['cold_start'] = True
                song['fallback'] = True
                existing.append(song)
                needed -= 1
        
        return existing
    
    def _get_fallback_recommendations(self, n: int) -> List[Dict]:
        """最终兜底推荐（当所有方法都失败时）"""
        logger.warning("使用兜底推荐数据")
        return self._fallback_hot_songs[:n] if self._fallback_hot_songs else []
    
    def get_hot_songs(self, tier: str = 'all', n: int = 20) -> List[Dict]:
        """获取热门歌曲（带异常处理）"""
        self._check_initialized()
        
        try:
            if tier == 'hit':
                songs = self._recommender.tiered_songs.get('hit', [])[:n]
            elif tier == 'popular':
                songs = self._recommender.tiered_songs.get('popular', [])[:n]
            elif tier == 'normal':
                songs = self._recommender.tiered_songs.get('normal', [])[:n]
            else:  # all
                songs = (self._recommender.tiered_songs.get('hit', [])[:n//3] +
                        self._recommender.tiered_songs.get('popular', [])[:n//3] +
                        self._recommender.tiered_songs.get('normal', [])[:n//3])
            
            results = []
            for sid in songs[:n]:
                try:
                    info = self.get_song_details(sid)
                    if info:
                        results.append(info)
                except Exception as e:
                    logger.warning(f"获取歌曲详情失败 {sid}: {e}")
                    continue
            return results
            
        except Exception as e:
            logger.error(f"获取热门歌曲失败: {e}")
            # 返回兜底数据
            if tier == 'all':
                return self._fallback_hot_songs[:n]
            return []
    
    def get_song_details(self, song_id: str) -> Optional[Dict]:
        """获取歌曲详情（带缓存）"""
        self._check_initialized()
        
        try:
            info = self._recommender.get_song_info(song_id)
            if not info:
                return None
            
            # 尝试获取音频特征
            features = {}
            try:
                if hasattr(self._recommender, 'song_features'):
                    df = self._recommender.song_features
                    row = df[df['song_id'] == song_id]
                    if not row.empty:
                        r = row.iloc[0]
                        features = {
                            'danceability': float(r.get('danceability', 0.5)),
                            'energy': float(r.get('energy', 0.5)),
                            'valence': float(r.get('valence', 0.5)),
                            'tempo': float(r.get('tempo', 120)),
                            'final_popularity': float(r.get('final_popularity', 50))
                        }
            except Exception:
                pass
            
            return {
                'song_id': str(song_id),
                'song_name': info.get('song_name', '未知'),
                'artists': info.get('artists', '未知'),
                'genre': info.get('genre', 'unknown'),
                'popularity': int(info.get('popularity', 50)),
                'audio_features': features,
                'retrieved_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"获取歌曲详情失败 {song_id}: {e}")
            return None
    
    def record_feedback(self, user_id: str, song_id: str, 
                       action: str, context: Dict = None) -> bool:
        """记录用户反馈（异步，不阻塞）"""
        try:
            # 这里可以入库或发送到消息队列
            logger.info(f"用户反馈 | user={user_id}, song={song_id}, "
                       f"action={action}, context={context}")
            
            # TODO: 可以添加到内存队列，后台线程批量写入数据库
            
            return True
        except Exception as e:
            logger.error(f"记录反馈失败: {e}")
            return False

# 全局实例（单例）
recommender_service = RecommenderService()

class RecommenderService:
    """
    推荐系统服务包装类 - 生产级优化版本
    """
    
    def __init__(self):
        self._recommender = None
        self._engine = None
        self._status = InitStatus.UNINITIALIZED
        self._init_lock = threading.RLock()
        self._init_error: Optional[str] = None
        self._valid_users: set = set()
        self._module = None
        self._circuit_breaker = CircuitBreaker(
            threshold=Config.CIRCUIT_BREAKER_THRESHOLD,
            timeout=Config.CIRCUIT_BREAKER_TIMEOUT
        )
        
        # 【修复1】添加缓存相关属性初始化
        self._recommendation_cache = RecommendationCache()  # 创建缓存实例
        self._cache_ttl = Config.CACHE_RECOMMENDATIONS_TTL  # 缓存过期时间
        
        # 兜底数据（当推荐引擎完全失败时使用）
        self._fallback_hot_songs: List[Dict] = []
        self._last_fallback_update = 0
    
    def get(self, key: str, ttl_seconds: int = 1800) -> Optional[Any]:
        """获取缓存，如果过期返回None"""
        with self._lock:
            if key not in self._cache:
                return None
            
            timestamp, data = self._cache[key]
            if time.time() - timestamp > ttl_seconds:
                # 过期清理
                del self._cache[key]
                return None
            return data
    
    def set(self, key: str, data: Any) -> None:
        """设置缓存"""
        with self._lock:
            self._cache[key] = (time.time(), data)
    
    def clear(self) -> None:
        """清空缓存"""
        with self._lock:
            self._cache.clear()
    
    def get_stats(self) -> dict:
        """获取缓存统计（用于调试）"""
        with self._lock:
            now = time.time()
            total = len(self._cache)
            expired = sum(1 for ts, _ in self._cache.values() if now - ts > 1800)
            return {
                "total_keys": total,
                "expired_keys": expired,
                "active_keys": total - expired
            }