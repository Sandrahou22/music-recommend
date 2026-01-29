# daily_crawler.py - 每日爬虫
import requests
import time
import random
import logging
import json
from bs4 import BeautifulSoup
import os
import re

class GenreMusicCrawler:
    def __init__(self, config, progress_manager):
        self.config = config
        self.progress_manager = progress_manager
        self.session = requests.Session()
        self.session.headers.update(self.config.HEADERS)
        
        # 初始化
        self.songs_data = []
        self.visited_songs = set()  # 当天已处理的歌曲ID
        self.crawl_start_time = time.time()
        
        # 标签到流派的映射表（用于从网页标签推断流派）
        self.tag_genre_map = {
            # 华语流行
            '流行': '华语流行', '华语': '华语流行', '国语': '华语流行', 
            '内地': '华语流行', '大陆': '华语流行', '中国风': '华语流行',
            'Mandopop': '华语流行', 'Chinese Pop': '华语流行',
            
            # 日本音乐
            '日语': '日本流行', 'J-Pop': '日本流行', 'Jpop': '日本流行',
            '日本': '日本流行', '日系': '日本流行', 'JPOP': '日本流行',
            '动漫': '动漫歌曲', '动画': '动漫歌曲', 'Anime': '动漫歌曲',
            'アニメ': '动漫歌曲', 'ACG': '动漫歌曲', '二次元': '动漫歌曲',
            'Vocaloid': 'Vocaloid', '初音': 'Vocaloid', 'ミク': 'Vocaloid',
            'ボカロ': 'Vocaloid', '虚拟歌手': 'Vocaloid',
            '游戏原声': '游戏原声', 'ゲーム': '游戏原声', 'BGM': '游戏原声',
            
            # 韩国音乐
            '韩语': 'K-Pop', '韩文': 'K-Pop', '韩国': 'K-Pop',
            'K-Pop': 'K-Pop', 'Kpop': 'K-Pop', 'KPOP': 'K-Pop',
            '韩流': 'K-Pop',
            
            # 欧美音乐
            '英语': '欧美流行', '英文': '欧美流行', '欧美': '欧美流行',
            'Pop': '欧美流行', '流行摇滚': '欧美流行', '美国': '欧美流行',
            'UK': '欧美流行', 'US': '欧美流行', 'English': '欧美流行',
            
            # 摇滚
            '摇滚': '摇滚', 'Rock': '摇滚', '金属': '摇滚',
            '重金属': '摇滚', '朋克': '摇滚', '硬核': '摇滚',
            '后摇': '摇滚', '独立摇滚': '摇滚', '英伦摇滚': '摇滚',
            
            # 说唱/嘻哈
            '说唱': '说唱', 'Rap': '说唱', 'Hip-Hop': '说唱',
            '嘻哈': '说唱', 'hiphop': '说唱', '饶舌': '说唱',
            'Trap': '说唱', 'R&B': '说唱', '节奏布鲁斯': '说唱',
            
            # 电子
            '电子': '电子', '电音': '电子', 'EDM': '电子',
            'House': '电子', 'Techno': '电子', 'Trance': '电子',
            'Dubstep': '电子', 'Future Bass': '电子', '电子舞曲': '电子',
            
            # 民谣
            '民谣': '民谣', 'Folk': '民谣', '乡村': '民谣',
            '城市民谣': '民谣', '民谣摇滚': '民谣', '独立民谣': '民谣',
            
            # R&B/灵魂
            'R&B': 'R&B', '节奏布鲁斯': 'R&B', 'Soul': 'R&B',
            '灵魂乐': 'R&B', 'Neo-Soul': 'R&B', '蓝调': 'R&B',
            
            # 其他
            '爵士': '爵士', 'Jazz': '爵士', '古典': '古典',
            'Classical': '古典', '轻音乐': '轻音乐', '纯音乐': '轻音乐',
            '新世纪': '新世纪', '世界音乐': '世界音乐', '民族': '民族音乐',
            '影视原声': '影视原声', 'OST': '影视原声', '原声带': '影视原声',
            '现场': '现场', 'Live': '现场', '演唱会': '现场',
            '翻唱': '翻唱', 'Cover': '翻唱',
        }
        
        # 知名歌手流派映射
        self.known_artists_genre = {
            # 华语流行歌手
            '周杰伦': '华语流行', '林俊杰': '华语流行', '陈奕迅': '华语流行',
            '王菲': '华语流行', '孙燕姿': '华语流行', '蔡依林': '华语流行',
            '五月天': '华语流行', '邓紫棋': '华语流行', '张惠妹': '华语流行',
            '刘德华': '华语流行', '张学友': '华语流行', '李荣浩': '华语流行',
            '薛之谦': '华语流行', '毛不易': '华语流行', '华晨宇': '华语流行',
            
            # 摇滚乐队/歌手
            'Beyond': '摇滚', '崔健': '摇滚', '汪峰': '摇滚',
            '黑豹乐队': '摇滚', '唐朝乐队': '摇滚', '新裤子': '摇滚',
            '逃跑计划': '摇滚', '痛仰乐队': '摇滚', '万能青年旅店': '摇滚',
            
            # 民谣歌手
            '赵雷': '民谣', '李志': '民谣', '陈粒': '民谣',
            '好妹妹': '民谣', '宋冬野': '民谣', '马頔': '民谣',
            '尧十三': '民谣', '程璧': '民谣', '陈鸿宇': '民谣',
            
            # 说唱歌手
            'GAI': '说唱', 'PG One': '说唱', '艾热': '说唱',
            '那吾克热': '说唱', '王以太': '说唱', '马思唯': '说唱',
            'Higher Brothers': '说唱', '谢帝': '说唱', '法老': '说唱',
            
            # 电子音乐人
            '徐梦圆': '电子', 'Panta.Q': '电子', 'Anti-General': '电子',
            'Curtis': '电子', '冷炫忱': '电子',
        }
        
        # 设置日志
        self.setup_logging()
    
    def setup_logging(self):
        """设置日志"""
        os.makedirs(self.config.LOG_DIR, exist_ok=True)
        
        log_file = os.path.join(self.config.LOG_DIR, f"crawl_{self.config.TODAY}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def safe_request(self, url, retries=3, delay=2):
        """安全的HTTP请求"""
        for attempt in range(retries):
            try:
                # 随机等待
                wait_time = random.uniform(self.config.REQUEST_DELAY_MIN, 
                                          self.config.REQUEST_DELAY_MAX)
                time.sleep(wait_time)
                
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    self.logger.warning(f"请求被拒绝，等待重试... ({attempt+1}/{retries})")
                    time.sleep(delay * 2)
                else:
                    self.logger.warning(f"请求失败，状态码: {response.status_code}")
                    
            except Exception as e:
                self.logger.warning(f"请求异常: {e} (尝试 {attempt+1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(delay)
        
        return None
    
    def get_playlist_songs(self, playlist_id, max_songs=100):
        """获取歌单中的歌曲"""
        url = f"https://music.163.com/playlist?id={playlist_id}"
        
        self.logger.info(f"获取歌单 {playlist_id} 的歌曲...")
        response = self.safe_request(url)
        
        if not response:
            self.logger.error(f"无法获取歌单: {playlist_id}")
            return []
        
        try:
            soup = BeautifulSoup(response.text, 'lxml')
            song_links = soup.select('ul.f-hide a')
            
            songs = []
            for link in song_links[:max_songs]:
                href = link.get('href', '')
                if '/song?id=' in href:
                    song_id = href.split('=')[-1].split('&')[0]
                    song_name = link.text.strip()
                    
                    # 清理歌曲名
                    song_name = ''.join(char for char in song_name if ord(char) < 65536)
                    
                    # 检查是否已处理（当天去重）
                    if song_id not in self.visited_songs and song_name:
                        songs.append({
                            'song_id': song_id,
                            'song_name': song_name,
                            'playlist_id': playlist_id
                        })
                        self.visited_songs.add(song_id)
            
            self.logger.info(f"从歌单 {playlist_id} 获取到 {len(songs)} 首新歌曲")
            return songs
            
        except Exception as e:
            self.logger.error(f"解析歌单失败: {e}")
            return []
    
    def get_song_detail(self, song_id):
        """获取歌曲详情（简版，只获取基本信息）"""
        api_url = f"https://music.163.com/api/song/detail/?ids=[{song_id}]"
        
        response = self.safe_request(api_url, retries=2, delay=1)
        if not response:
            return None
        
        try:
            data = response.json()
            if data.get('code') == 200 and data.get('songs'):
                return data['songs'][0]
        except:
            pass
        
        return None
    
    def detect_language(self, text):
        """检测语言"""
        if not text:
            return 'unknown'
        
        # 日文
        if re.search(r'[\u3040-\u309F\u30A0-\u30FF]', str(text)):
            return 'japanese'
        
        # 韩文
        if re.search(r'[\uAC00-\uD7AF]', str(text)):
            return 'korean'
        
        # 中文
        if re.search(r'[\u4E00-\u9FFF]', str(text)):
            return 'chinese'
        
        # 英文
        if re.search(r'[a-zA-Z]', str(text)):
            return 'english'
        
        return 'unknown'
    
    # ========== 新的流派推断方法 ==========
    
    def normalize_genre(self, genre):
        """标准化流派名称"""
        if not genre:
            return '华语流行'
        
        genre_lower = str(genre).lower()
        
        # 映射常见变体到标准名称
        genre_map = {
            'pop': '欧美流行',
            'mandopop': '华语流行',
            'c-pop': '华语流行',
            'chinese pop': '华语流行',
            'j-pop': '日本流行',
            'jpop': '日本流行',
            'k-pop': 'K-Pop',
            'kpop': 'K-Pop',
            'rap': '说唱',
            'hiphop': '说唱',
            'hip-hop': '说唱',
            'r&b': 'R&B',
            'rnb': 'R&B',
            'electronic': '电子',
            'edm': '电子',
            'rock': '摇滚',
            'folk': '民谣',
            'jazz': '爵士',
            'classical': '古典',
            'country': '乡村',
            'blues': '蓝调',
        }
        
        for key, value in genre_map.items():
            if key in genre_lower:
                return value
        
        # 如果包含中文流派名
        if '流行' in genre:
            # 判断是哪种流行
            if '华语' in genre or '国语' in genre or '中文' in genre:
                return '华语流行'
            elif '日本' in genre or '日语' in genre or '日系' in genre:
                return '日本流行'
            elif '欧美' in genre or '英文' in genre or '英语' in genre:
                return '欧美流行'
            elif '韩国' in genre or '韩语' in genre or '韩文' in genre:
                return 'K-Pop'
            else:
                return '华语流行'  # 默认
        
        return '华语流行'  # 默认
    
    def infer_genre_from_artists(self, artists_str):
        """根据歌手推断流派"""
        if not artists_str:
            return None
        
        artists_lower = artists_str.lower()
        
        # 根据知名歌手推断流派
        for artist, genre in self.known_artists_genre.items():
            if artist in artists_str:
                return genre
        
        # 根据艺术家名字中的关键词
        if any(keyword in artists_lower for keyword in ['乐队', '乐团', 'band', '组合']):
            return '摇滚'
        elif any(keyword in artists_lower for keyword in ['rapper', '说唱', 'rap', '嘻哈']):
            return '说唱'
        elif any(keyword in artists_lower for keyword in ['dj', '电音', 'electronic']):
            return '电子'
        
        return None
    
    def infer_genre_from_title(self, song_name):
        """根据歌名关键词推断流派"""
        if not song_name:
            return None
        
        song_lower = song_name.lower()
        
        # 检查是否是翻唱
        if any(keyword in song_lower for keyword in ['cover', '翻唱', '版)', '版本']):
            return '翻唱'
        
        # 检查是否是现场版
        if any(keyword in song_lower for keyword in ['live', '现场', '演唱会']):
            return '现场'
        
        # 检查是否是OST
        if any(keyword in song_lower for keyword in ['ost', '主题曲', '片尾曲', '插曲', '电视剧', '电影']):
            return '影视原声'
        
        # 检查是否是儿童歌曲
        if any(keyword in song_lower for keyword in ['儿歌', '童谣', '宝宝', '儿童']):
            return '儿童音乐'
        
        return None
    
    def infer_genre_from_language(self, song_name, artists):
        """根据语言推断流派（原有的方法，保留作为后备）"""
        language = self.detect_language(song_name)
        
        if language == 'japanese':
            # 检查是否为动漫/Vocaloid
            if any(keyword in song_name for keyword in ['初音', 'ミク', 'Vocaloid', 'ボカロ']):
                return 'Vocaloid'
            elif any(keyword in song_name.lower() for keyword in ['anime', 'op', 'ed', '主题曲', '插入歌']):
                return '动漫歌曲'
            else:
                return '日本流行'
        elif language == 'korean':
            return 'K-Pop'
        elif language == 'chinese':
            return '华语流行'
        elif language == 'english':
            return '欧美流行'
        
        return '华语流行'  # 默认
    
    def infer_popular_genre(self, song_name, artists_str):
        """根据热度推断流行音乐的亚流派"""
        # 先检测语言
        language = self.detect_language(song_name)
        
        if language == 'chinese':
            # 检查是否是网络神曲（通常是流行或电子）
            if any(keyword in song_name for keyword in ['抖音', '快手', '热门', '神曲']):
                return '华语流行'  # 或 '电子'
            
            # 检查歌名是否包含情感词汇（可能是情歌）
            emotional_words = ['爱', '情', '恋', '想', '念', '痛', '伤', '泪']
            if any(word in song_name for word in emotional_words):
                return '华语流行'
            
            return '华语流行'
        elif language == 'japanese':
            return '日本流行'
        elif language == 'korean':
            return 'K-Pop'
        elif language == 'english':
            return '欧美流行'
        
        return '华语流行'
    
    def get_song_genre(self, song_id, song_name, artists_str, album_name, song_detail):
        """获取歌曲流派（主要方法）"""
        self.logger.info(f"获取歌曲流派: {song_name} ({song_id})")
        
        # 1. 如果API提供了流派信息，优先使用
        if song_detail and 'genre' in song_detail:
            api_genre = song_detail.get('genre')
            if api_genre and api_genre != '未知':
                normalized_genre = self.normalize_genre(api_genre)
                if normalized_genre != '华语流行':  # 如果不是默认值
                    self.logger.info(f"从API获取流派: {normalized_genre}")
                    return normalized_genre
        
        # 2. 根据歌手推断
        genre_from_artist = self.infer_genre_from_artists(artists_str)
        if genre_from_artist:
            self.logger.info(f"根据歌手推断流派: {genre_from_artist}")
            return genre_from_artist
        
        # 3. 根据歌名推断
        genre_from_title = self.infer_genre_from_title(song_name)
        if genre_from_title:
            self.logger.info(f"根据歌名推断流派: {genre_from_title}")
            return genre_from_title
        
        # 4. 根据专辑名推断
        if album_name:
            album_lower = album_name.lower()
            if any(keyword in album_lower for keyword in ['ost', '原声', 'soundtrack', '电视剧', '电影', '剧集']):
                self.logger.info("根据专辑名推断为影视原声")
                return '影视原声'
            elif any(keyword in album_lower for keyword in ['live', '演唱会', '音乐会']):
                self.logger.info("根据专辑名推断为现场")
                return '现场'
        
        # 5. 根据热度判断是否热门歌曲
        if song_detail and 'popularity' in song_detail:
            popularity = song_detail.get('popularity', 0)
            if popularity > 80:  # 高热度歌曲
                self.logger.info("根据热度推断为流行音乐")
                return self.infer_popular_genre(song_name, artists_str)
        
        # 6. 最后根据语言特征推断（原有的方法）
        genre = self.infer_genre_from_language(song_name, artists_str)
        self.logger.info(f"根据语言推断流派: {genre}")
        return genre
    
    def parse_song_info(self, song_id, song_name, playlist_id=None):
        """解析歌曲信息"""
        # 获取基本信息
        song_detail = self.get_song_detail(song_id)
        if not song_detail:
            self.logger.warning(f"无法获取歌曲详情: {song_id}")
            return None
        
        try:
            # 提取信息
            artists = []
            for artist in song_detail.get('artists', []):
                artists.append(artist.get('name', ''))
            
            artists_str = ','.join(artists)
            album_name = song_detail.get('album', {}).get('name', '')
            
            # 使用新的流派推断方法
            genre = self.get_song_genre(song_id, song_name, artists_str, album_name, song_detail)
            
            # 修复明显的误判
            genre = self.fix_genre_mistakes(genre, song_name, artists_str, album_name)
            
            # 处理发布时间
            publish_time = song_detail.get('album', {}).get('publishTime', 0)
            if publish_time:
                publish_date = time.strftime('%Y-%m-%d', time.localtime(publish_time/1000))
            else:
                publish_date = ''
            
            # 构建数据
            song_data = {
                'song_id': song_id,
                'song_name': song_name,
                'artists': artists_str,
                'album': album_name,
                'album_id': song_detail.get('album', {}).get('id', ''),
                'duration': song_detail.get('duration', 0) // 1000,  # 秒
                'publish_date': publish_date,
                'genre': genre,
                'popularity': song_detail.get('popularity', 0),
                'language': self.detect_language(song_name),
                'crawl_date': time.strftime('%Y-%m-%d'),
                'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'playlist_id': playlist_id,
            }
            
            self.logger.info(f"处理歌曲: {song_name[:20]:<20} | 流派: {genre}")
            return song_data
            
        except Exception as e:
            self.logger.error(f"解析歌曲信息失败: {e}")
            return None
    
    def fix_genre_mistakes(self, genre, song_name, artists_str, album_name):
        """修复明显的流派分类错误"""
        # 如果是Vocaloid但实际上是中文歌曲
        if genre == 'Vocaloid':
            # 检查是否是真正的中文歌曲
            song_lang = self.detect_language(song_name)
            artists_lang = self.detect_language(artists_str)
            
            # 如果歌曲名或歌手名是中文，且不包含日文假名
            if (song_lang == 'chinese' or artists_lang == 'chinese'):
                # 检查是否包含日文假名
                has_japanese = re.search(r'[\u3040-\u309f\u30a0-\u30ff]', song_name) is not None
                
                if not has_japanese:
                    self.logger.info(f"修复Vocaloid误判: {song_name} -> 华语流行")
                    
                    # 进一步判断具体类型
                    # 检查是否是影视歌曲
                    if album_name and any(keyword in album_name for keyword in ['电视剧', '电影', 'OST', '原声']):
                        return '影视原声'
                    
                    # 检查歌手是否包含乐队关键词
                    if artists_str and any(keyword in artists_str for keyword in ['乐队', '乐团']):
                        return '摇滚'
                    
                    return '华语流行'
        
        return genre
    
    def crawl_playlist(self, playlist_id):
        """爬取单个歌单"""
        self.logger.info(f"开始爬取歌单: {playlist_id}")
        
        # 获取歌单中的歌曲
        songs = self.get_playlist_songs(playlist_id, max_songs=100)
        
        if not songs:
            self.logger.warning(f"歌单 {playlist_id} 没有获取到歌曲")
            return 0
        
        # 处理每首歌曲
        processed_count = 0
        for song_info in songs:
            # 检查是否已达到当日目标
            if len(self.songs_data) >= self.config.DAILY_TARGET_SONGS:
                self.logger.info(f"已达到当日目标 {self.config.DAILY_TARGET_SONGS} 首，停止爬取")
                break
            
            song_data = self.parse_song_info(
                song_info['song_id'],
                song_info['song_name'],
                playlist_id
            )
            
            if song_data:
                self.songs_data.append(song_data)
                processed_count += 1
                
                # 每处理10首显示一次进度
                if processed_count % 10 == 0:
                    self.show_progress()
        
        self.logger.info(f"完成歌单 {playlist_id}，处理了 {processed_count} 首歌曲")
        return processed_count
    
    def show_progress(self):
        """显示进度"""
        elapsed = time.time() - self.crawl_start_time
        current_count = len(self.songs_data)
        target = self.config.DAILY_TARGET_SONGS
        
        if elapsed > 0:
            speed = current_count / elapsed * 3600  # 首/小时
            remaining = (target - current_count) / (current_count / elapsed) if current_count > 0 else 0
        else:
            speed = 0
            remaining = 0
        
        print(f"\r进度: {current_count}/{target} | 速度: {speed:.1f}首/小时 | "
              f"预计剩余: {remaining:.0f}秒", end="", flush=True)
    
    def save_daily_data(self):
        """保存当日数据"""
        if not self.songs_data:
            self.logger.warning("没有数据需要保存")
            return None
        
        # 创建数据目录
        os.makedirs(self.config.DATA_DIR, exist_ok=True)
        
        # 保存到CSV
        import pandas as pd
        df = pd.DataFrame(self.songs_data)
        
        # 每日数据文件
        daily_file = os.path.join(self.config.DATA_DIR, f"{self.config.TODAY}_songs.csv")
        df.to_csv(daily_file, index=False, encoding='utf-8-sig')
        
        # 更新总数据文件
        total_file = os.path.join(self.config.DATA_DIR, "all_songs.csv")
        if os.path.exists(total_file):
            total_df = pd.read_csv(total_file, encoding='utf-8-sig')
            total_df = pd.concat([total_df, df], ignore_index=True)
            total_df.drop_duplicates(subset=['song_id'], keep='last', inplace=True)
        else:
            total_df = df
        
        total_df.to_csv(total_file, index=False, encoding='utf-8-sig')
        
        self.logger.info(f"数据已保存: {daily_file}")
        self.logger.info(f"总数据已更新: {total_file} ({len(total_df)} 首歌曲)")
        
        return daily_file
    
    def run_daily_crawl(self):
        """运行每日爬取"""
        print("="*60)
        print(f"🎵 网易云音乐每日爬取 - {self.config.TODAY}")
        print("="*60)
        
        # 获取今天的歌单
        today_playlists = self.progress_manager.get_today_playlists(
            self.config.PLAYLIST_POOL,
            self.config.DAILY_PLAYLISTS
        )
        
        if not today_playlists:
            print("❌ 没有可用的歌单")
            return
        
        print(f"📋 今天将爬取 {len(today_playlists)} 个歌单")
        print(f"🎯 目标: {self.config.DAILY_TARGET_SONGS} 首歌曲")
        print("-"*60)
        
        # 开始爬取
        total_processed = 0
        playlist_processed = 0
        
        for i, playlist_id in enumerate(today_playlists, 1):
            # 检查是否已达到目标
            if len(self.songs_data) >= self.config.DAILY_TARGET_SONGS:
                print(f"\n✅ 已达到当日目标，停止爬取")
                break
            
            print(f"\n🎵 处理歌单 {i}/{len(today_playlists)}: {playlist_id}")
            
            # 爬取歌单
            processed = self.crawl_playlist(playlist_id)
            total_processed += processed
            playlist_processed += 1
            
            # 显示当前进度
            self.show_progress()
            print()  # 换行
        
        # 保存数据
        print("\n" + "-"*60)
        if self.songs_data:
            data_file = self.save_daily_data()
            
            # 更新进度
            self.progress_manager.update_daily_progress(
                self.config.TODAY,
                len(self.songs_data),
                playlist_processed
            )
            
            print(f"\n🎉 每日爬取完成！")
            print(f"✅ 处理歌单: {playlist_processed} 个")
            print(f"✅ 获取歌曲: {len(self.songs_data)} 首")
            print(f"💾 数据文件: {data_file}")
            
            # 显示当日统计
            self.show_daily_statistics()
        else:
            print("❌ 今日未获取到任何歌曲数据")
        
        return len(self.songs_data)
    
    def show_daily_statistics(self):
        """显示当日统计"""
        if not self.songs_data:
            return
        
        import pandas as pd
        df = pd.DataFrame(self.songs_data)
        
        print("\n📊 当日数据统计")
        print("-"*40)
        
        # 流派分布
        if 'genre' in df.columns:
            genre_counts = df['genre'].value_counts()
            print("流派分布:")
            for genre, count in genre_counts.items():
                percentage = count / len(df) * 100
                print(f"  {genre:<10}: {count:>3} 首 ({percentage:>5.1f}%)")
        
        # 语言分布
        if 'language' in df.columns:
            lang_counts = df['language'].value_counts()
            print("\n语言分布:")
            for lang, count in lang_counts.items():
                percentage = count / len(df) * 100
                print(f"  {lang:<8}: {count:>3} 首 ({percentage:>5.1f}%)")
        
        # 歌手统计
        if 'artists' in df.columns:
            # 获取出现最多的歌手
            all_artists = []
            for artists_str in df['artists']:
                if isinstance(artists_str, str):
                    all_artists.extend([a.strip() for a in artists_str.split(',')])
            
            from collections import Counter
            top_artists = Counter(all_artists).most_common(5)
            
            print("\n热门歌手TOP5:")
            for i, (artist, count) in enumerate(top_artists, 1):
                print(f"  {i}. {artist:<15}: {count} 首")
        
        print("-"*40)