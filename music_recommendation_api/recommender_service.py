import os
import sys
import logging
import threading
import time
from functools import lru_cache
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum
import importlib.util
import json
import pickle

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

from config import Config

logger = logging.getLogger(__name__)


class InitStatus(Enum):
    UNINITIALIZED = "uninitialized"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    FAILED = "failed"
    DEGRADED = "degraded"


class CircuitBreaker:
    """熔断器（保留原实现）"""
    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "closed"
        self._lock = threading.Lock()

    def call(self, func, *args, **kwargs):
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
    instances = {}
    locks = {}
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
    推荐服务 - 适配 SeparatedMusicRecommender（优化版）
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

        # 兜底热门歌曲缓存
        self._fallback_hot_songs: List[Dict] = []
        self._last_fallback_update = 0

        # ===== 新增：最佳权重配置（根据最近调优结果）=====
        # 内部用户权重 (itemcf, usercf, content, mf, sentiment)
        self._internal_weights = (0.4, 0.1, 0.2, 0.1, 0.0)
        # 外部用户权重
        self._external_weights = (0.4, 0.1, 0.3, 0.0, 0.0)
        # Artist 和 LightFM 的固定权重（可根据需要调整）
        self._artist_weight = 0.1
        self._lightfm_weight = 0.15
        # =================================================

    @property
    def is_healthy(self) -> bool:
        return self._status in (InitStatus.INITIALIZED, InitStatus.DEGRADED)

    @property
    def is_ready(self) -> bool:
        return self._status in (
            InitStatus.INITIALIZED,
            InitStatus.DEGRADED,
            InitStatus.FAILED
        ) and len(self._fallback_hot_songs) > 0

    # ------------------------------------------------------------------
    # 初始化核心方法
    # ------------------------------------------------------------------
    def initialize(self, force: bool = False) -> bool:
        if self._status == InitStatus.INITIALIZED and not force:
            return True

        if self._status == InitStatus.INITIALIZING:
            for _ in range(30):
                if self._status != InitStatus.INITIALIZING:
                    return self._status == InitStatus.INITIALIZED
                time.sleep(1)
            return False

        with self._init_lock:
            if self._status == InitStatus.INITIALIZING:
                return False

            self._status = InitStatus.INITIALIZING
            logger.info("=" * 60)
            logger.info("【开始初始化分离式推荐系统】")
            start_time = datetime.now()

            try:
                self._setup_database()
                self._load_recommender_module()
                self._initialize_engine()
                self._refresh_fallback_data()

                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"✓ 初始化完成，耗时: {elapsed:.2f}秒")
                self._status = InitStatus.INITIALIZED
                return True

            except Exception as e:
                import traceback
                self._init_error = str(e)
                logger.error(f"✗ 初始化失败: {e}\n{traceback.format_exc()}")
                self._try_degraded_mode()
                return False

    def _setup_database(self):
        if self._engine is None:
            self._engine = create_engine(
                Config.get_db_connection_string(),
                poolclass=QueuePool,
                pool_size=Config.DB_CONFIG['pool_size'],
                max_overflow=Config.DB_CONFIG['max_overflow'],
                pool_recycle=Config.DB_CONFIG['pool_recycle'],
                pool_pre_ping=True,
                echo=False
            )
            logger.info("数据库连接池已建立")

    def _load_recommender_module(self):
        code_path = Config.RECOMMENDER_CODE_PATH
        if not code_path.exists():
            raise FileNotFoundError(f"找不到推荐系统代码: {code_path}")

        if str(Config.DATASET_DIR) not in sys.path:
            sys.path.insert(0, str(Config.DATASET_DIR))

        spec = importlib.util.spec_from_file_location(
            "separated_recommender",
            str(code_path)
        )
        self._module = importlib.util.module_from_spec(spec)

        original_cwd = os.getcwd()
        try:
            os.chdir(Config.DATASET_DIR)
            spec.loader.exec_module(self._module)
        finally:
            os.chdir(original_cwd)

        logger.info(f"成功加载分离式推荐模块: {code_path.name}")

    def _initialize_engine(self):
        """实例化 SeparatedMusicRecommender"""
        SeparatedMusicRecommender = self._module.SeparatedMusicRecommender

        data_dir = str(Config.DATASET_DIR / "separated_processed_data")
        cache_dir = str(Config.DATASET_DIR / "recommender_cache")

        if not os.path.exists(data_dir):
            raise FileNotFoundError(
                f"分离式数据目录不存在: {data_dir}\n"
                "请确保已运行 separated_preprocessor.py 生成预处理数据"
            )

        self._recommender = SeparatedMusicRecommender(
            data_dir=data_dir,
            cache_dir=cache_dir
        )

        internal_users = set(self._recommender.internal_recommender.user_to_idx.keys())
        external_users = set(self._recommender.external_recommender.user_to_idx.keys())
        self._valid_users = internal_users | external_users

        logger.info(
            f"推荐引擎就绪: 内部用户={len(internal_users)}, "
            f"外部用户={len(external_users)}, 总用户={len(self._valid_users)}"
        )

    def _try_degraded_mode(self):
        try:
            cache_file = Config.DATASET_DIR / 'fallback_hot_songs.json'
            if cache_file.exists():
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self._fallback_hot_songs = json.load(f)
                self._status = InitStatus.DEGRADED
                logger.warning("已进入降级模式：使用静态热门推荐")
        except Exception as e:
            logger.error(f"降级模式加载失败: {e}")

    def _refresh_fallback_data(self):
        """刷新兜底热门歌曲（持久化到JSON）"""
        try:
            if self._recommender:
                hot_songs = self.get_hot_songs(tier='all', n=100)
                self._fallback_hot_songs = hot_songs
                self._last_fallback_update = time.time()

                cache_file = Config.DATASET_DIR / 'fallback_hot_songs.json'
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(hot_songs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"刷新兜底数据失败: {e}")

    # ------------------------------------------------------------------
    # 内部推荐逻辑（核心适配 - 优化版）
    # ------------------------------------------------------------------
    def _get_recommendations_internal(self, user_id: str, n: int, algorithm: str) -> List[Tuple]:
        """
        根据用户ID和算法返回推荐列表，格式为 [(song_id, score), ...]
        优化：混合推荐使用并行版本，并传入调优后的7个权重
        """
        user_id_str = str(user_id)
        is_cold = user_id_str not in self._valid_users

        # 冷启动：直接使用对应推荐器的冷启动方法
        if is_cold:
            logger.info(f"用户 {user_id} 冷启动")
            user_type = self._recommender.get_user_type(user_id_str)
            recommender = (self._recommender.internal_recommender
                           if user_type == 'internal' else self._recommender.external_recommender)
            recs = recommender.get_cold_start_recs(n=n)
            return recs if recs else []

        # 个性化用户：获取对应子推荐器
        user_type = self._recommender.get_user_type(user_id_str)
        recommender = (self._recommender.internal_recommender
                       if user_type == 'internal' else self._recommender.external_recommender)

        # 算法路由
        if algorithm in ('usercf', 'cf', 'content', 'mf', 'cold'):
            if algorithm == 'usercf':
                recs = recommender.user_based_cf(user_id_str, n=n)
            elif algorithm == 'cf':
                recs = recommender.item_based_cf(user_id_str, n=n)
            elif algorithm == 'content':
                recs = recommender.content_based(user_id_str, n=n)
            elif algorithm == 'mf':
                recs = recommender.matrix_factorization_rec(user_id_str, n=n)
            elif algorithm == 'cold':
                recs = recommender.get_cold_start_recs(n=n)
            else:
                recs = []
        else:  # 'hybrid' 或 'auto' 使用并行混合推荐
            if user_type == 'internal':
                w_itemcf, w_usercf, w_content, w_mf, w_sentiment = self._internal_weights
            else:
                w_itemcf, w_usercf, w_content, w_mf, w_sentiment = self._external_weights
            # 调用并行混合推荐，传入7个权重
            recs = recommender.hybrid_recommendation_parallel(
                user_id_str, n=n, use_mmr=True,
                w_itemcf=w_itemcf, w_usercf=w_usercf,
                w_content=w_content, w_mf=w_mf,
                w_sentiment=w_sentiment,
                w_artist=self._artist_weight,
                w_lightfm=self._lightfm_weight
            )

        # 若推荐为空，回退冷启动
        if not recs:
            logger.warning(f"用户 {user_id} 算法 '{algorithm}' 无结果，回退冷启动")
            recs = recommender.get_cold_start_recs(n=n)

        return recs

    # ------------------------------------------------------------------
    # 对外接口（保持原签名不变）
    # ------------------------------------------------------------------
    def get_recommendations(self, user_id: str, n: int = 10, algorithm: str = 'hybrid') -> List[Dict]:
        """主推荐接口"""
        try:
            self._check_initialized()
            recs = self._get_recommendations_internal(str(user_id), n, algorithm)
            is_cold = str(user_id) not in self._valid_users
            results = self._format_recommendations(recs, is_cold)
            if len(results) < n:
                results = self._fill_with_hot_songs(results, n)
            return results
        except Exception as e:
            logger.error(f"获取推荐失败: {e}", exc_info=True)
            return self._get_fallback_recommendations(n)

    def _format_recommendations(self, recs: List[Tuple], is_cold: bool) -> List[Dict]:
        """将 (song_id, score) 格式化为前端所需字典，并查询音频状态"""
        results = []
        if not recs:
            return results

        song_ids = [str(sid) for sid, _ in recs]
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
                        'has_audio': has_audio,
                        'source': info.get('source', 'unknown'),
                        'timestamp': datetime.now().isoformat()
                    })
            except Exception as e:
                logger.warning(f"格式化歌曲 {song_id} 失败: {e}")
                continue
        return results

    def _get_audio_status_batch(self, song_ids: List[str]) -> Dict[str, bool]:
        """批量查询音频文件是否存在"""
        if not song_ids or not self._engine:
            return {}
        try:
            batch_size = 50
            audio_status = {}
            for i in range(0, len(song_ids), batch_size):
                batch = song_ids[i:i + batch_size]
                placeholders = ', '.join([f"'{sid}'" for sid in batch])
                query = f"""
                SELECT song_id,
                       CASE WHEN audio_path IS NOT NULL AND audio_path != '' THEN 1 ELSE 0 END as has_audio
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
        """用热门歌曲补全"""
        existing_ids = {r['song_id'] for r in existing}
        needed = n - len(existing)
        if needed <= 0:
            return existing
        for song in self._fallback_hot_songs:
            if song['song_id'] not in existing_ids and needed > 0:
                song['cold_start'] = True
                song['fallback'] = True
                existing.append(song)
                needed -= 1
        return existing

    def _get_fallback_recommendations(self, n: int) -> List[Dict]:
        logger.warning("使用兜底推荐数据")
        return self._fallback_hot_songs[:n] if self._fallback_hot_songs else []

    # ------------------------------------------------------------------
    # 用户画像（带缓存）
    # ------------------------------------------------------------------
    @lru_cache(maxsize=128)
    def get_user_profile_cached(self, user_id: str) -> Optional[Dict]:
        return self.get_user_profile(user_id)

    def get_user_profile(self, user_id: str) -> Optional[Dict]:
        """获取用户画像"""
        self._check_initialized()
        user_id_str = str(user_id)
        user_type = self._recommender.get_user_type(user_id_str)
        recommender = (self._recommender.internal_recommender
                       if user_type == 'internal' else self._recommender.external_recommender)
        profile = recommender.get_user_profile(user_id_str)
        if profile:
            profile['is_cold_start'] = user_id_str not in self._valid_users
        return profile

    # ------------------------------------------------------------------
    # 热门歌曲
    # ------------------------------------------------------------------
    def get_hot_songs(self, tier: str = 'all', n: int = 20) -> List[Dict]:
        """使用内部推荐器的热门分层数据（外部歌曲较少，统一使用内部）"""
        self._check_initialized()
        recommender = self._recommender.internal_recommender

        try:
            if tier == 'hit':
                songs = recommender.tiered_songs.get('hit', [])[:n]
            elif tier == 'popular':
                songs = recommender.tiered_songs.get('popular', [])[:n]
            elif tier == 'normal':
                songs = recommender.tiered_songs.get('normal', [])[:n]
            else:  # all
                songs = (recommender.tiered_songs.get('hit', [])[:n // 3] +
                         recommender.tiered_songs.get('popular', [])[:n // 3] +
                         recommender.tiered_songs.get('normal', [])[:n // 3])

            results = []
            for sid in songs[:n]:
                info = self.get_song_details(sid)
                if info:
                    results.append(info)
            return results
        except Exception as e:
            logger.error(f"获取热门歌曲失败: {e}")
            return self._fallback_hot_songs[:n]

    # ------------------------------------------------------------------
    # 歌曲详情（从数据库补充音频特征）
    # ------------------------------------------------------------------
    def get_song_details(self, song_id: str) -> Optional[Dict]:
        self._check_initialized()
        try:
            info = self._recommender.get_song_info(song_id)
            if not info:
                return None

            result = {
                'song_id': str(song_id),
                'song_name': info.get('song_name', '未知'),
                'artists': info.get('artists', '未知'),
                'genre': info.get('genre', 'unknown'),
                'popularity': int(info.get('popularity', 50)),
                'source': info.get('source', 'unknown'),
                'retrieved_at': datetime.now().isoformat()
            }

            # 从数据库补充音频特征
            if self._engine:
                query = text("""
                    SELECT danceability, energy, valence, tempo, loudness,
                           speechiness, acousticness, instrumentalness, liveness,
                           final_popularity
                    FROM enhanced_song_features
                    WHERE song_id = :sid
                """)
                with self._engine.connect() as conn:
                    row = conn.execute(query, {"sid": str(song_id)}).fetchone()
                    if row:
                        result['audio_features'] = {
                            'danceability': float(row.danceability or 0.5),
                            'energy': float(row.energy or 0.5),
                            'valence': float(row.valence or 0.5),
                            'tempo': float(row.tempo or 120),
                            'loudness': float(row.loudness or -10),
                            'speechiness': float(row.speechiness or 0.1),
                            'acousticness': float(row.acousticness or 0.5),
                            'instrumentalness': float(row.instrumentalness or 0.1),
                            'liveness': float(row.liveness or 0.2),
                            'final_popularity': float(row.final_popularity or 50)
                        }
            return result
        except Exception as e:
            logger.error(f"获取歌曲详情失败 {song_id}: {e}")
            return None

    # ------------------------------------------------------------------
    # 健康检查 & 状态
    # ------------------------------------------------------------------
    def health_check(self) -> Dict[str, Any]:
        status = {
            "status": self._status.value,
            "healthy": self.is_healthy,
            "ready": self.is_ready,
            "timestamp": datetime.now().isoformat(),
            "fallback_songs_count": len(self._fallback_hot_songs),
            "circuit_breaker": self._circuit_breaker.state
        }
        if self._recommender:
            internal = self._recommender.internal_recommender
            external = self._recommender.external_recommender
            status.update({
                "internal_users": len(internal.user_to_idx) if internal.user_to_idx else 0,
                "external_users": len(external.user_to_idx) if external.user_to_idx else 0,
                "internal_songs": len(internal.source_songs) if internal.source_songs is not None else 0,
                "external_songs": len(external.source_songs) if external.source_songs is not None else 0,
                "total_users": len(self._valid_users)
            })
        if self._init_error:
            status["last_error"] = self._init_error
        return status

    def _check_initialized(self):
        if self._status != InitStatus.INITIALIZED:
            if self._status == InitStatus.FAILED:
                logger.info("检测到未初始化，尝试自动初始化...")
                if not self.initialize():
                    raise RuntimeError(f"推荐系统初始化失败: {self._init_error}")
            elif self._status == InitStatus.UNINITIALIZED:
                self.initialize()
            if self._status not in (InitStatus.INITIALIZED, InitStatus.DEGRADED):
                raise RuntimeError("推荐系统暂不可用，请稍后重试")

    # ------------------------------------------------------------------
    # 反馈记录（保持原样）
    # ------------------------------------------------------------------
    def record_feedback(self, user_id: str, song_id: str,
                        action: str, context: Dict = None) -> bool:
        try:
            logger.info(f"用户反馈 | user={user_id}, song={song_id}, action={action}")
            return True
        except Exception as e:
            logger.error(f"记录反馈失败: {e}")
            return False

    # ------------------------------------------------------------------
    # 缓存失效
    # ------------------------------------------------------------------
    def invalidate_user_cache(self, user_id: str):
        """清除用户缓存（仅用于兼容旧接口）"""
        self.get_user_profile_cached.cache_clear()
        logger.info(f"清除用户画像缓存 | user={user_id}")


# 全局单例
recommender_service = RecommenderService()