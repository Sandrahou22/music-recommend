# progress_manager.py - 进度管理器（修改版：每次随机选择）
import json
import os
from datetime import datetime, timedelta
import random

class ProgressManager:
    def __init__(self):
        self.progress_file = "crawl_progress.json"
        self.init_progress()
    
    def init_progress(self):
        """初始化进度文件（只保留统计信息，不记录歌单历史）"""
        if not os.path.exists(self.progress_file):
            default_progress = {
                "total_days": 0,
                "total_songs": 0,
                "total_playlists": 0,
                "daily_records": {},
                "last_crawl_date": "",
                # 不保存歌单历史，每次重新随机选择
            }
            self.save_progress(default_progress)
        else:
            # 如果已有进度文件，清除歌单历史记录
            progress = self.load_progress()
            # 保留统计信息，但清除歌单历史
            if "used_playlists" in progress:
                del progress["used_playlists"]
            if "next_playlists" in progress:
                del progress["next_playlists"]
            self.save_progress(progress)
    
    def load_progress(self):
        """加载进度"""
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    
    def save_progress(self, progress):
        """保存进度"""
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def get_today_playlists(self, playlist_pool, daily_count=20):
        """获取今天要爬取的歌单 - 每次都重新随机选择"""
        print(f"\n🎯 正在为今天随机选择 {daily_count} 个歌单...")
        print(f"  歌单池总数: {len(playlist_pool)} 个")
        
        # 每次都从整个歌单池中随机选择
        if len(playlist_pool) <= daily_count:
            today_playlists = playlist_pool.copy()
            print(f"  歌单池数量不足，选择了所有 {len(today_playlists)} 个歌单")
        else:
            today_playlists = random.sample(playlist_pool, daily_count)
            print(f"  随机选择了 {len(today_playlists)} 个歌单")
        
        # 显示前几个选择的歌单
        print(f"  今日歌单ID（前10个）: {today_playlists[:10]}")
        if len(today_playlists) > 10:
            print(f"  ... 还有 {len(today_playlists)-10} 个歌单")
        
        return today_playlists
    
    def update_daily_progress(self, date, songs_count, playlists_count):
        """更新每日进度（只更新统计，不记录歌单）"""
        progress = self.load_progress()
        
        # 更新总体统计
        progress["total_days"] = progress.get("total_days", 0) + 1
        progress["total_songs"] = progress.get("total_songs", 0) + songs_count
        progress["total_playlists"] = progress.get("total_playlists", 0) + playlists_count
        progress["last_crawl_date"] = date
        
        # 更新每日记录
        if "daily_records" not in progress:
            progress["daily_records"] = {}
        
        progress["daily_records"][date] = {
            "songs": songs_count,
            "playlists": playlists_count,
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 限制每日记录大小（保留最近30天）
        if len(progress["daily_records"]) > 30:
            oldest_dates = sorted(progress["daily_records"].keys())[:-30]
            for old_date in oldest_dates:
                del progress["daily_records"][old_date]
        
        self.save_progress(progress)
    
    def get_statistics(self):
        """获取统计信息"""
        progress = self.load_progress()
        
        stats = {
            "total_days": progress.get("total_days", 0),
            "total_songs": progress.get("total_songs", 0),
            "total_playlists": progress.get("total_playlists", 0),
            "last_crawl": progress.get("last_crawl_date", "从未爬取"),
            "daily_average": 0
        }
        
        if stats["total_days"] > 0:
            stats["daily_average"] = stats["total_songs"] / stats["total_days"]
        
        return stats
    
    def print_statistics(self):
        """打印统计信息"""
        stats = self.get_statistics()
        
        print("\n" + "="*60)
        print("📊 爬取统计")
        print("="*60)
        print(f"爬取天数: {stats['total_days']} 天")
        print(f"总歌曲数: {stats['total_songs']} 首")
        print(f"总歌单数: {stats['total_playlists']} 个")
        print(f"平均每日: {stats['daily_average']:.0f} 首")
        print(f"上次爬取: {stats['last_crawl']}")
        print("="*60)