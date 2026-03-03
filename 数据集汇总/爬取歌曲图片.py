import os
import time
import random
import logging
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import sys

# ========== 配置 ==========
# 文件路径
RAW_SONGS_PATH = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\all_songs.csv"
PROCESSED_SONGS_PATH = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\separated_processed_data\all_song_features.csv"

# 图片保存目录（建议放在 Flask 静态目录下）
SAVE_DIR = r"C:\Users\小侯\Desktop\学校作业\毕业设计\music_recommendation_api\static\song_covers"
os.makedirs(SAVE_DIR, exist_ok=True)

# 并发设置
MAX_WORKERS = 10          # 同时下载的线程数
REQUEST_DELAY = (0.5, 1.5)  # 随机延时范围
MAX_RETRIES = 3           # 搜索重试次数

# 网易云 Cookie（必须替换为您的有效 Cookie）
# 获取方法：登录网易云音乐网页版，F12 -> 网络 -> 任意请求 -> 复制 Cookie 字符串
COOKIE_STR = "请替换为您的网易云Cookie"  # 例如 "MUSIC_U=xxx; __csrf=xxx"

# 解析 Cookie 字符串为字典
COOKIES = {}
if COOKIE_STR and COOKIE_STR != "请替换为您的网易云Cookie":
    for item in COOKIE_STR.split(';'):
        if '=' in item:
            key, value = item.strip().split('=', 1)
            COOKIES[key] = value

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://music.163.com/',
    # Cookie 通过 cookies 参数传递，不放这里
}

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== 全局停止标志 ==========
stop_flag = False

def signal_handler(sig, frame):
    global stop_flag
    logger.info("收到中断信号，正在停止...")
    stop_flag = True

signal.signal(signal.SIGINT, signal_handler)

# ========== 数据匹配 ==========
logger.info("开始读取并匹配数据...")
raw_df = pd.read_csv(RAW_SONGS_PATH)
processed_df = pd.read_csv(PROCESSED_SONGS_PATH)

raw_df['song_name'] = raw_df['song_name'].astype(str)
processed_df['song_name'] = processed_df['song_name'].astype(str)

# 去重（避免重复爬取）
raw_df = raw_df.drop_duplicates(subset=['song_name'])
processed_df = processed_df.drop_duplicates(subset=['song_name'])

merged = pd.merge(
    processed_df[['song_id', 'song_name']],
    raw_df[['song_name']],
    on='song_name',
    how='inner'
).drop_duplicates(subset=['song_id'])

song_list = merged.to_dict('records')
logger.info(f"共匹配到 {len(song_list)} 首需要爬取封面的歌曲")

# ========== 工具函数 ==========
def get_cover_url(song_name):
    """通过网易云搜索获取歌曲ID，再通过详情API获取封面URL"""
    if stop_flag:
        return None
    
    # 第一步：搜索获取歌曲ID
    search_url = "https://music.163.com/api/search/get"
    params = {'s': song_name, 'limit': 1, 'type': 1}
    
    for attempt in range(MAX_RETRIES):
        if stop_flag:
            return None
        try:
            resp = requests.get(search_url, headers=HEADERS, cookies=COOKIES, params=params, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"搜索HTTP错误 {resp.status_code}: {song_name}")
                continue
                
            data = resp.json()
            if data.get('code') != 200:
                logger.warning(f"搜索API错误码 {data.get('code')}: {data.get('msg', '')} - {song_name}")
                continue
                
            songs = data.get('result', {}).get('songs', [])
            if not songs:
                logger.warning(f"无搜索结果: {song_name}")
                return None
                
            # 获取第一个匹配的歌曲ID
            song_id = songs[0].get('id')
            if not song_id:
                logger.warning(f"搜索结果无歌曲ID: {song_name}")
                return None
                
            # 第二步：通过歌曲详情API获取封面URL
            detail_url = "https://music.163.com/api/song/detail"
            detail_params = {'id': song_id, 'ids': f'[{song_id}]'}
            detail_resp = requests.get(detail_url, headers=HEADERS, cookies=COOKIES, params=detail_params, timeout=10)
            
            if detail_resp.status_code != 200:
                logger.warning(f"详情HTTP错误 {detail_resp.status_code}: {song_name}")
                continue
                
            detail_data = detail_resp.json()
            if detail_data.get('code') != 200:
                logger.warning(f"详情API错误码 {detail_data.get('code')}: {song_name}")
                continue
                
            songs_detail = detail_data.get('songs', [])
            if not songs_detail:
                logger.warning(f"详情无歌曲: {song_name}")
                return None
                
            # 提取封面URL
            album = songs_detail[0].get('album', {})
            pic_url = album.get('picUrl')
            if pic_url:
                logger.info(f"成功获取封面: {song_name} -> {pic_url}")
                return pic_url
            else:
                logger.warning(f"详情无picUrl: {song_name}")
                return None
                
        except Exception as e:
            logger.error(f"获取封面异常: {song_name}, 尝试 {attempt+1}: {e}")
            time.sleep(1)
    
    return None

def download_cover(song_id, cover_url):
    """下载图片并保存"""
    if stop_flag:
        return None
    try:
        resp = requests.get(cover_url, headers=HEADERS, cookies=COOKIES, stream=True, timeout=15)
        if resp.status_code == 200:
            ext = cover_url.split('.')[-1].split('?')[0].lower()
            if ext not in ['jpg', 'jpeg', 'png', 'gif']:
                ext = 'jpg'
            file_path = os.path.join(SAVE_DIR, f"{song_id}.{ext}")
            with open(file_path, 'wb') as f:
                for chunk in resp.iter_content(1024):
                    if stop_flag:
                        return None
                    f.write(chunk)
            logger.info(f"下载成功: {song_id}")
            return file_path
        else:
            logger.warning(f"下载失败: {song_id}, 状态码: {resp.status_code}")
    except Exception as e:
        logger.error(f"下载异常: {song_id}: {e}")
    return None

def process_one(song):
    """处理单个歌曲"""
    if stop_flag:
        return song['song_id'], "stopped", None

    song_id = song['song_id']
    song_name = song['song_name']

    # 检查是否已存在
    for ext in ['.jpg', '.jpeg', '.png', '.gif']:
        if os.path.exists(os.path.join(SAVE_DIR, f"{song_id}{ext}")):
            logger.info(f"已存在: {song_id}，跳过")
            return song_id, "exists", None

    cover_url = get_cover_url(song_name)
    if not cover_url:
        logger.warning(f"无法获取封面 URL: {song_id} - {song_name}")
        return song_id, "failed", None

    file_path = download_cover(song_id, cover_url)
    if file_path:
        return song_id, "success", file_path
    else:
        return song_id, "failed", None

# ========== 主程序 ==========
def main():
    global stop_flag
    logger.info("开始爬取歌曲封面...")
    results = {"success": 0, "failed": 0, "exists": 0, "stopped": 0}
    success_records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_song = {executor.submit(process_one, song): song for song in song_list}
        try:
            for future in as_completed(future_to_song):
                if stop_flag:
                    logger.info("检测到停止信号，正在取消剩余任务...")
                    for f in future_to_song:
                        f.cancel()
                    break
                song_id, status, file_path = future.result()
                results[status] += 1
                if status == "success" and file_path:
                    success_records.append((song_id, file_path))
                time.sleep(random.uniform(*REQUEST_DELAY))
        except KeyboardInterrupt:
            stop_flag = True
            logger.info("用户中断，正在停止...")
            executor.shutdown(wait=False)
            raise
        finally:
            executor.shutdown(wait=True)

    logger.info(f"爬取完成: 成功 {results['success']}, 失败 {results['failed']}, 已存在 {results['exists']}, 中断 {results['stopped']}")

    # 生成成功列表，方便后续数据库更新
    if success_records:
        success_df = pd.DataFrame(success_records, columns=['song_id', 'file_path'])
        success_df.to_csv('song_cover_success.csv', index=False)
        logger.info("已保存成功记录到 song_cover_success.csv")

        # 生成 SQL 更新语句（示例）
        print("\n如需更新数据库，可执行以下 SQL（请替换路径为可访问的 URL）：")
        for song_id, file_path in success_records:
            filename = os.path.basename(file_path)
            # 假设静态路径为 /static/song_covers/
            url = f"/static/song_covers/{filename}"
            print(f"UPDATE enhanced_song_features SET cover_path = '{url}' WHERE song_id = '{song_id}';")
    else:
        logger.info("没有新下载的封面。")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)