import os
import time
import random
import logging
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import sys
import tempfile
import shutil

# 尝试导入 pydub
try:
    from pydub import AudioSegment
    HAS_PYDUB = True
except ImportError:
    HAS_PYDUB = False
    logging.warning("pydub 未安装，将下载完整音频（无法压缩）。建议安装：pip install pydub")

# ========== 配置 ==========
PROCESSED_SONGS_PATH = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集汇总\separated_processed_data\all_song_features.csv"
SAVE_DIR = r"C:\Users\小侯\Desktop\学校作业\毕业设计\music_recommendation_api\static\audio_files"
os.makedirs(SAVE_DIR, exist_ok=True)

MAX_WORKERS = 10
REQUEST_DELAY = (0.3, 0.8)
MAX_RETRIES = 2                     # 减少重试次数，避免浪费
CUT_DURATION = 15                    # 截取前 15 秒
TARGET_BITRATE = "32k"               # 目标比特率
FORCE_MONO = True                    # 强制单声道
DISK_MIN_FREE = 500 * 1024 * 1024    # 剩余空间低于 500MB 时停止

# 网易云 Cookie（必须替换为最新有效值）
COOKIE_STR = "_iuqxldmzr_=32; _ntes_nuid=1c90c6283d1103ca864de4c32379778d; WEVNSM=1.0.0; WM_TID=24RrnhZfrCtEQEVAQVLQGjL7VSAb0A46; ntes_utid=tid._.K0CeZBauMudFAhBUARaSXpWwCW3hsztm._.0; _ntes_nnid=1c90c6283d1103ca864de4c32379778d,1766937025980; __snaker__id=Ife6D7TYk44CzJxQ; NMTID=00O7NExLq8UckSWGknpgjQffduQxR0AAAGb1D9htQ; WNMCID=baedam.1768792482886.01.0; sDeviceId=YD-EYoma%2FIenFFEV0EAQAeGy3hzyWtsZ%2BHC; ntes_kaola_ad=1; Hm_lvt_d94b7d26fa25db7f6413fb58d7a438c7=1768792482,1768882661; JSESSIONID-WYYY=%2BiUt%2FajWBpG7UgR%5C1jwhDxHarx6S2s8zmQ8sOZPruJ%2F%2BqeCWOk8pQxaHtsYeuv0TR3mb5ViW%5CkbgREXxRf90xgftAGw9IrcMbNQIxN8OcVhP2WDkidjlkpkG8R3pmVTHHrFjBDZ9VwByHkKER6e0%2B2T1PwE5MdgQX6F08kKVsB%2BXypfE%3A1772530461050; Hm_lvt_1483fb4774c02a30ffa6f0e2945e9b70=1770549107,1772528661; HMACCOUNT=93D8DD5679A4071A; __csrf=9b5a5bf176048d8a4066006185ce6322; WM_NI=xa%2F%2BH7miBcq5xKb5S%2FCmMJMQiBCjn3TVQXx002e%2BKarwXnECNIRpQMbuOcLW%2Fchj274sx0iauGxKsn6%2B6C89Xdfcd5J25gBg6Phr9JotD5jCiVcYM%2FsIszy3cThZHQGFOXo%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed9fc54878b8598c14893968fa7d14f879a8a86c2798eb18296e659b3bcf7d6b32af0fea7c3b92abcbfe1cccb3dafb78e84f43fbc95aed6d22581b383abd05bab9e8d8bed39988a9fb9fb3cf4eeabccca6bb2a9988fc8498e979e91d447a2ab8184cf5dedbda88bb13df8adb8d1ec6bb799b8acf266f6f5faa4bb339cbcaa84e73f8aeefb88c16df690a893bc42adac8cd6e949ab8aadbad025b197bd9bfc39a18ca8bbcb5b859a81b9ee37e2a3; gdxidpyhxdE=uXyJPV62hnGcutyMd3jUOhtDYoBmHVS%5Cc5a79eR1pg%5CZM72mPBJnImDnL75p0NBpTsqCb0tq1geIwQOnOysyZx7NB%2Fuocb52xcWJs112i340sd%5CVIMVlUGjkQebzRzqrIzvDbmw2GVzty5Bu22VrZhhIJuIONlURrizji0HBg3HJuEHX%3A1772529579126; __csrf=9a0178f3c1d6fc889fb54f363fe8dac9; MUSIC_U=0049ECA139E550ADFD3397759223F6482866DCEA7CB0098B4AAAF2BEB7E5506BA9B0E084BC87164FE2A3D114FEB0088BA7D31ACBD1B5611196866A776BE21027C24F011660F657E85A40352416539E71DF03F8240E5E4B0253A5428931DFAB0A4CD421A6DD8F14746105745E71E5BA9C31C0D52CED7852516099F1D1213C5E361F7AB76776812EA9E98D20199C5A64C3D12E69FE1B376C2BAF65240109535C3A6CD99B1AD40371AB9DC77E9E862C649B6D5407DE22C81706A39A2899F436A9DD526750ED8FE8060A78532E21825EE1503CAA76FABE7B40EA0AECC756FCE82A012ECECC4EAB373E575FB3D259407153BE3788C873757F380A2E9841D0603343AC79B95E5AD12680BDAD9A79368070A0B2096CDC84E6561C794214E4E08CAA406B00BFCD88FD1F3D31E437E4CBF354851399DBB1DFCA89F9EED54731A290967C1C7939034CE819EB7711FEF7A9AF40EC7D62D1F2D6AAE0756F872CC6D3F69612F34DCD0F67A113ABF703019CD73ECA90EC1BA2C717264E7DB9FD9598E3EEC72C1AF1795B2EF99CB01FBD0D42E25F0B2166B3E63BF6A2E11EE4484E5721B309122885; Hm_lpvt_1483fb4774c02a30ffa6f0e2945e9b70=1772528814; playerid=78797783"
COOKIES = {}
if COOKIE_STR and COOKIE_STR != "_iuqxldmzr_=32; _ntes_nuid=1c90c6283d1103ca864de4c32379778d; WEVNSM=1.0.0; WM_TID=24RrnhZfrCtEQEVAQVLQGjL7VSAb0A46; ntes_utid=tid._.K0CeZBauMudFAhBUARaSXpWwCW3hsztm._.0; _ntes_nnid=1c90c6283d1103ca864de4c32379778d,1766937025980; __snaker__id=Ife6D7TYk44CzJxQ; NMTID=00O7NExLq8UckSWGknpgjQffduQxR0AAAGb1D9htQ; WNMCID=baedam.1768792482886.01.0; sDeviceId=YD-EYoma%2FIenFFEV0EAQAeGy3hzyWtsZ%2BHC; ntes_kaola_ad=1; Hm_lvt_d94b7d26fa25db7f6413fb58d7a438c7=1768792482,1768882661; JSESSIONID-WYYY=%2BiUt%2FajWBpG7UgR%5C1jwhDxHarx6S2s8zmQ8sOZPruJ%2F%2BqeCWOk8pQxaHtsYeuv0TR3mb5ViW%5CkbgREXxRf90xgftAGw9IrcMbNQIxN8OcVhP2WDkidjlkpkG8R3pmVTHHrFjBDZ9VwByHkKER6e0%2B2T1PwE5MdgQX6F08kKVsB%2BXypfE%3A1772530461050; Hm_lvt_1483fb4774c02a30ffa6f0e2945e9b70=1770549107,1772528661; HMACCOUNT=93D8DD5679A4071A; __csrf=9b5a5bf176048d8a4066006185ce6322; WM_NI=xa%2F%2BH7miBcq5xKb5S%2FCmMJMQiBCjn3TVQXx002e%2BKarwXnECNIRpQMbuOcLW%2Fchj274sx0iauGxKsn6%2B6C89Xdfcd5J25gBg6Phr9JotD5jCiVcYM%2FsIszy3cThZHQGFOXo%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed9fc54878b8598c14893968fa7d14f879a8a86c2798eb18296e659b3bcf7d6b32af0fea7c3b92abcbfe1cccb3dafb78e84f43fbc95aed6d22581b383abd05bab9e8d8bed39988a9fb9fb3cf4eeabccca6bb2a9988fc8498e979e91d447a2ab8184cf5dedbda88bb13df8adb8d1ec6bb799b8acf266f6f5faa4bb339cbcaa84e73f8aeefb88c16df690a893bc42adac8cd6e949ab8aadbad025b197bd9bfc39a18ca8bbcb5b859a81b9ee37e2a3; gdxidpyhxdE=uXyJPV62hnGcutyMd3jUOhtDYoBmHVS%5Cc5a79eR1pg%5CZM72mPBJnImDnL75p0NBpTsqCb0tq1geIwQOnOysyZx7NB%2Fuocb52xcWJs112i340sd%5CVIMVlUGjkQebzRzqrIzvDbmw2GVzty5Bu22VrZhhIJuIONlURrizji0HBg3HJuEHX%3A1772529579126; __csrf=9a0178f3c1d6fc889fb54f363fe8dac9; MUSIC_U=0049ECA139E550ADFD3397759223F6482866DCEA7CB0098B4AAAF2BEB7E5506BA9B0E084BC87164FE2A3D114FEB0088BA7D31ACBD1B5611196866A776BE21027C24F011660F657E85A40352416539E71DF03F8240E5E4B0253A5428931DFAB0A4CD421A6DD8F14746105745E71E5BA9C31C0D52CED7852516099F1D1213C5E361F7AB76776812EA9E98D20199C5A64C3D12E69FE1B376C2BAF65240109535C3A6CD99B1AD40371AB9DC77E9E862C649B6D5407DE22C81706A39A2899F436A9DD526750ED8FE8060A78532E21825EE1503CAA76FABE7B40EA0AECC756FCE82A012ECECC4EAB373E575FB3D259407153BE3788C873757F380A2E9841D0603343AC79B95E5AD12680BDAD9A79368070A0B2096CDC84E6561C794214E4E08CAA406B00BFCD88FD1F3D31E437E4CBF354851399DBB1DFCA89F9EED54731A290967C1C7939034CE819EB7711FEF7A9AF40EC7D62D1F2D6AAE0756F872CC6D3F69612F34DCD0F67A113ABF703019CD73ECA90EC1BA2C717264E7DB9FD9598E3EEC72C1AF1795B2EF99CB01FBD0D42E25F0B2166B3E63BF6A2E11EE4484E5721B309122885; Hm_lpvt_1483fb4774c02a30ffa6f0e2945e9b70=1772528814; playerid=78797783":
    for item in COOKIE_STR.split(';'):
        if '=' in item:
            key, value = item.strip().split('=', 1)
            COOKIES[key] = value

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://music.163.com/',
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

stop_flag = False

def signal_handler(sig, frame):
    global stop_flag
    logger.info("收到中断信号，正在停止...")
    stop_flag = True

signal.signal(signal.SIGINT, signal_handler)

# ========== 磁盘空间检查 ==========
def has_enough_disk_space():
    try:
        usage = shutil.disk_usage(SAVE_DIR)
        free_bytes = usage.free
        if free_bytes < DISK_MIN_FREE:
            logger.warning(f"磁盘剩余空间不足 {free_bytes//1024//1024}MB，停止下载")
            return False
        return True
    except Exception as e:
        logger.error(f"检查磁盘空间失败: {e}")
        return True

# ========== 数据准备 ==========
logger.info("开始读取歌曲数据...")
processed_df = pd.read_csv(PROCESSED_SONGS_PATH)
processed_df['song_id'] = processed_df['song_id'].astype(str)
all_song_list = processed_df[['song_id', 'song_name']].to_dict('records')
logger.info(f"共有 {len(all_song_list)} 首歌曲需要处理")

# 过滤已存在的文件
existing_files = {f.replace('.mp3', '') for f in os.listdir(SAVE_DIR) if f.endswith('.mp3')}
song_list = [s for s in all_song_list if s['song_id'] not in existing_files]
logger.info(f"过滤后剩余 {len(song_list)} 首需要处理（已存在 {len(existing_files)} 首）")

# ========== 工具函数 ==========
def get_netease_song_id(song_name):
    global stop_flag
    if stop_flag:
        return None, None
    search_url = "https://music.163.com/api/search/get"
    params = {'s': song_name, 'limit': 1, 'type': 1}
    try:
        resp = requests.get(search_url, headers=HEADERS, cookies=COOKIES, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('code') == 200:
                songs = data.get('result', {}).get('songs', [])
                if songs:
                    song_id = songs[0].get('id')
                    return song_id, songs[0].get('name', song_name)
    except Exception as e:
        logger.error(f"搜索异常: {song_name}: {e}")
    return None, None

def compress_audio(input_path, output_path):
    """使用 pydub 压缩音频：截取前 CUT_DURATION 秒，降低比特率，强制单声道"""
    try:
        audio = AudioSegment.from_file(input_path)
        # 截取前 CUT_DURATION 秒（如果音频过短则保留完整）
        if len(audio) / 1000 > CUT_DURATION:
            audio = audio[:CUT_DURATION * 1000]
        # 转换为单声道（如果启用）
        if FORCE_MONO and audio.channels > 1:
            audio = audio.set_channels(1)
        # 导出为低比特率 MP3
        audio.export(output_path, format="mp3", bitrate=TARGET_BITRATE)
        return True
    except Exception as e:
        logger.error(f"音频压缩失败: {e}")
        return False

def download_audio(song_id, netease_id, song_name):
    global stop_flag
    if stop_flag:
        return None
    if not has_enough_disk_space():
        logger.warning("磁盘空间不足，停止后续下载")
        stop_flag = True
        return None

    audio_url = f"http://music.163.com/song/media/outer/url?id={netease_id}.mp3"
    # 临时文件
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3", dir=SAVE_DIR) as tmp_file:
        temp_path = tmp_file.name
    final_path = os.path.join(SAVE_DIR, f"{song_id}.mp3")

    for attempt in range(MAX_RETRIES):
        if stop_flag:
            return None
        try:
            resp = requests.get(audio_url, headers=HEADERS, cookies=COOKIES, stream=True, timeout=30)
            if resp.status_code == 200:
                content_type = resp.headers.get('Content-Type', '')
                # 如果返回的是 HTML，直接判定失败
                if 'text/html' in content_type:
                    logger.warning(f"返回HTML，可能是Cookie失效或歌曲无版权: {song_id}")
                    return None
                if 'audio' in content_type or 'octet-stream' in content_type:
                    # 写入临时文件
                    with open(temp_path, 'wb') as f:
                        for chunk in resp.iter_content(1024 * 1024):
                            if stop_flag:
                                return None
                            f.write(chunk)
                    # 检查文件大小
                    if os.path.getsize(temp_path) < 1024:
                        os.unlink(temp_path)
                        logger.warning(f"下载文件过小，可能为错误页面: {song_id}")
                        return None

                    # 压缩处理
                    if HAS_PYDUB:
                        if compress_audio(temp_path, final_path):
                            logger.info(f"压缩成功: {song_id} - {song_name} ({CUT_DURATION}s, {TARGET_BITRATE})")
                            os.unlink(temp_path)
                            return final_path
                        else:
                            # 压缩失败，保留完整文件
                            shutil.move(temp_path, final_path)
                            logger.warning(f"压缩失败，保留完整音频: {song_id}")
                            return final_path
                    else:
                        shutil.move(temp_path, final_path)
                        logger.info(f"下载成功（完整）: {song_id} - {song_name}")
                        return final_path
                else:
                    logger.warning(f"非音频内容: {song_id}, Content-Type: {content_type}")
                    return None  # 直接失败，不重试
            else:
                logger.warning(f"下载失败: {song_id}, 状态码: {resp.status_code}")
        except Exception as e:
            logger.error(f"下载异常: {song_id}, 尝试 {attempt+1}: {e}")
            time.sleep(2)
    # 清理临时文件
    if os.path.exists(temp_path):
        os.unlink(temp_path)
    return None

def process_one(song):
    global stop_flag
    if stop_flag:
        return song['song_id'], "stopped", None
    song_id = song['song_id']
    song_name = song['song_name']

    netease_id, matched_name = get_netease_song_id(song_name)
    if not netease_id:
        logger.warning(f"无法获取歌曲ID: {song_id} - {song_name}")
        return song_id, "failed", None

    file_path = download_audio(song_id, netease_id, song_name)
    if file_path:
        return song_id, "success", file_path
    else:
        return song_id, "failed", None

# ========== 主程序 ==========
def main():
    global stop_flag
    logger.info("开始爬取歌曲音频...")
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

    if success_records:
        success_df = pd.DataFrame(success_records, columns=['song_id', 'file_path'])
        success_df.to_csv('audio_success.csv', index=False)
        logger.info("已保存成功记录到 audio_success.csv")
        print("\n如需更新数据库，可执行以下 SQL：")
        for song_id, file_path in success_records:
            filename = os.path.basename(file_path)
            audio_url = f"/static/audio_files/{filename}"
            print(f"UPDATE enhanced_song_features SET audio_path = '{audio_url}' WHERE song_id = '{song_id}';")
    else:
        logger.info("没有新下载的音频。")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)