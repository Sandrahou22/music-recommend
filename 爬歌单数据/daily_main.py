# daily_main.py - 主程序
import sys
import os
import time
import pandas as pd
from datetime import datetime

# 修改导入方式
from daily_config import (
    BASE_URL, HEADERS, DAILY_TARGET_SONGS, DAILY_PLAYLISTS, 
    MAX_SONGS_PER_PLAYLIST, PLAYLIST_POOL, REQUEST_DELAY_MIN, 
    REQUEST_DELAY_MAX, RETRY_TIMES, DATA_DIR, LOG_DIR, TODAY
)

from daily_crawler import GenreMusicCrawler
from progress_manager import ProgressManager

# 创建一个配置类来封装所有配置
class Config:
    def __init__(self):
        self.BASE_URL = BASE_URL
        self.HEADERS = HEADERS
        self.DAILY_TARGET_SONGS = DAILY_TARGET_SONGS
        self.DAILY_PLAYLISTS = DAILY_PLAYLISTS
        self.MAX_SONGS_PER_PLAYLIST = MAX_SONGS_PER_PLAYLIST
        self.PLAYLIST_POOL = PLAYLIST_POOL
        self.REQUEST_DELAY_MIN = REQUEST_DELAY_MIN
        self.REQUEST_DELAY_MAX = REQUEST_DELAY_MAX
        self.RETRY_TIMES = RETRY_TIMES
        self.DATA_DIR = DATA_DIR
        self.LOG_DIR = LOG_DIR
        self.TODAY = TODAY

def setup_environment():
    """设置环境"""
    # 创建必要目录
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    
    print("✅ 环境检查完成")
    print(f"📁 数据目录: {os.path.abspath(DATA_DIR)}")
    print(f"📁 日志目录: {os.path.abspath(LOG_DIR)}")

def check_today_status():
    """检查今日状态，返回模式字符串：'overwrite'、'append'，或None表示取消"""
    today = TODAY
    
    # 检查今天是否已经爬取过
    daily_file = os.path.join(DATA_DIR, f"{today}_songs.csv")
    if os.path.exists(daily_file):
        try:
            df = pd.read_csv(daily_file, encoding='utf-8-sig')
            print(f"📅 今天 ({today}) 已经爬取过 {len(df)} 首歌曲")
            
            print("\n请选择操作：")
            print("  1. 追加数据到现有文件（保留原有数据，新增数据去重）")
            print("  2. 覆盖现有文件（重新爬取，将删除原有数据）")
            print("  3. 取消")
            
            choice = input("\n请选择 (1/2/3): ").strip()
            
            if choice == '1':
                return 'append'
            elif choice == '2':
                return 'overwrite'
            elif choice == '3':
                print("已取消")
                return None
            else:
                print("无效选择，默认使用追加模式")
                return 'append'
        except Exception as e:
            print(f"读取现有文件时出错: {e}")
            # 如果读取失败，可能是文件损坏，询问是否覆盖
            choice = input("文件可能已损坏，是否覆盖？(y/n): ").strip().lower()
            if choice == 'y':
                return 'overwrite'
            else:
                print("已取消")
                return None
    else:
        # 文件不存在，直接使用覆盖模式（即创建新文件）
        return 'overwrite'

def save_daily_data(songs_data, date_str, mode='append'):
    """保存每日数据到文件，支持追加或覆盖模式"""
    if not songs_data:
        print("⚠️  没有数据可保存")
        return 0
    
    # 确保目录存在
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # 生成文件名
    filename = os.path.join(DATA_DIR, f"{date_str}_songs.csv")
    
    try:
        # 转换为DataFrame
        new_df = pd.DataFrame(songs_data)
        
        # 数据清洗
        for col in new_df.columns:
            if new_df[col].dtype == 'float64':
                new_df[col] = new_df[col].fillna(0)
        
        # 根据模式处理
        if mode == 'append' and os.path.exists(filename):
            try:
                # 读取现有数据
                existing_df = pd.read_csv(filename, encoding='utf-8-sig')
                print(f"📂 现有数据: {len(existing_df)} 条记录")
                
                # 合并新旧数据
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # 去重（基于song_id）
                before_dedup = len(combined_df)
                combined_df = combined_df.drop_duplicates(subset=['song_id'], keep='first')
                after_dedup = len(combined_df)
                
                print(f"📊 合并后: {before_dedup} 条，去重后: {after_dedup} 条")
                print(f"📈 新增: {after_dedup - len(existing_df)} 条唯一记录")
                
                # 保存合并后的数据
                combined_df.to_csv(filename, index=False, encoding='utf-8-sig')
                final_df = combined_df
                
            except Exception as e:
                print(f"❌ 追加数据失败: {e}，将覆盖保存")
                new_df.to_csv(filename, index=False, encoding='utf-8-sig')
                final_df = new_df
                
        else:
            # 覆盖模式或文件不存在
            new_df.to_csv(filename, index=False, encoding='utf-8-sig')
            final_df = new_df
        
        print(f"✅ 数据已保存到: {os.path.abspath(filename)}")
        print(f"📊 最终数据量: {len(final_df)} 首歌曲")
        
        # 显示新增歌曲
        if mode == 'append' and 'song_id' in new_df.columns:
            print(f"📥 本次新增歌曲: {len(new_df)} 首")
            print("\n📋 本次新增歌曲样本:")
            for i, song in enumerate(new_df.head(5).to_dict('records'), 1):
                song_name = str(song.get('song_name', 'N/A'))[:20]
                artists = str(song.get('artists', 'N/A'))[:15]
                print(f"  {i}. {song_name:<20} - {artists:<15}")
        
        return len(final_df)
        
    except Exception as e:
        print(f"❌ 保存文件失败: {e}")
        return 0

def main():
    """主函数"""
    print("="*70)
    print("🎵 网易云音乐每日增量爬取系统")
    print("="*70)
    
    # 设置环境
    setup_environment()
    
    # 检查今日状态，获取保存模式
    save_mode = check_today_status()
    if save_mode is None:
        return  # 用户取消
    
    # 显示目标
    print(f"\n🎯 每日目标:")
    print(f"  歌单数: {DAILY_PLAYLISTS} 个")
    print(f"  歌曲数: {DAILY_TARGET_SONGS} 首")
    print(f"  预计时间: 约 {DAILY_TARGET_SONGS * 2.5 / 60:.1f} 分钟")
    
    # 确认开始
    print("\n⚠️  注意事项:")
    print("  1. 确保网络连接稳定")
    print("  2. 程序会自动控制爬取速度")
    print("  3. 按 Ctrl+C 可以中断爬取")
    print(f"  4. 数据将使用 {save_mode} 模式保存")
    
    confirm = input("\n是否开始今天的爬取？(y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消")
        return
    
    # 初始化组件
    print("\n🚀 初始化爬虫...")
    
    # 创建配置对象和进度管理器
    config = Config()
    progress_manager = ProgressManager()
    
    # 传递配置和进度管理器给爬虫
    crawler = GenreMusicCrawler(config, progress_manager)
    
    # 显示历史统计
    print("\n📊 历史统计:")
    progress_manager.print_statistics()
    
    # 开始爬取
    print(f"\n{'='*70}")
    print(f"🕒 开始时间: {datetime.now().strftime('%H:%M:%S')}")
    print("="*70)
    
    try:
        start_time = time.time()
        
        # 获取今天要爬取的歌单
        playlist_ids = progress_manager.get_today_playlists(
            PLAYLIST_POOL, 
            DAILY_PLAYLISTS
        )
        print(f"\n📋 今日爬取歌单 ({len(playlist_ids)} 个):")
        for i, pid in enumerate(playlist_ids[:10], 1):
            print(f"  {i}. 歌单ID: {pid}")
        if len(playlist_ids) > 10:
            print(f"  ... 还有 {len(playlist_ids) - 10} 个歌单")
        
        # 运行爬取
        print(f"\n🚀 开始爬取歌曲...")
        
        # 修改：调用正确的方法名
        songs_data = crawler.run_daily_crawl()
        
        # 保存数据
        if len(crawler.songs_data) > 0:
            saved_count = save_daily_data(crawler.songs_data, TODAY, save_mode)
            
            # 更新进度
            if saved_count > 0:
                # 更新今天的爬取记录
                progress_manager.update_daily_progress(TODAY, len(crawler.songs_data), len(playlist_ids))
                
                # 打印数据摘要
                print(f"\n📊 数据摘要:")
                print(f"  总歌曲数: {saved_count}")
                
                # 流派分布
                df = pd.DataFrame(crawler.songs_data)
                if 'genre' in df.columns:
                    genre_counts = df['genre'].value_counts()
                    print(f"  本次爬取流派分布: {len(genre_counts)} 种")
                    for genre, count in genre_counts.head(5).items():
                        percentage = count/len(df)*100
                        print(f"    {genre}: {count} 首 ({percentage:.1f}%)")
        
        else:
            print("⚠️  没有爬取到任何歌曲")
        
        # 计算耗时
        end_time = time.time()
        total_time = end_time - start_time
        minutes = int(total_time // 60)
        seconds = int(total_time % 60)
        
        print(f"\n⏰ 爬取耗时: {minutes:02d}:{seconds:02d}")
        
        if len(crawler.songs_data) > 0:
            print(f"📈 爬取速度: {len(crawler.songs_data)/total_time*60:.1f} 首/分钟 ({len(crawler.songs_data)/total_time*3600:.1f} 首/小时)")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户中断爬取")
        
        # 保存已获取的数据
        if crawler.songs_data:
            print("正在保存已获取的数据...")
            # 中断时，使用追加模式，避免覆盖已有数据
            saved_count = save_daily_data(crawler.songs_data, TODAY, 'append')
            if saved_count > 0:
                print(f"✅ 已保存 {saved_count} 首歌曲到部分文件")
        
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n" + "="*70)
        print("🏁 程序结束")
        
        # 显示最终统计
        if 'crawler' in locals() and crawler.songs_data:
            df = pd.DataFrame(crawler.songs_data)
            print(f"📊 最终统计:")
            print(f"  本次爬取歌曲: {len(df)} 首")
            print(f"  处理歌单: {len(playlist_ids)} 个")
            
            # 显示保存的文件
            daily_file = os.path.join(DATA_DIR, f"{TODAY}_songs.csv")
            if os.path.exists(daily_file):
                file_size = os.path.getsize(daily_file)
                print(f"  数据文件: {daily_file} ({file_size:,} 字节)")
        
        print(f"🕒 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

if __name__ == "__main__":
    main()