"""
非功能需求性能测试脚本
- 测试推荐接口响应时间
- 测试100并发成功率
- 测试音频首包时间
"""

import requests
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://127.0.0.1:5000/api/v1"
RECOMMEND_URL = f"{BASE_URL}/recommend/1001?n=10"
AUDIO_URL = "http://127.0.0.1:5000/api/v1/songs/S000015/audio"  # 请替换为实际存在的歌曲ID

def test_response_time(n=100):
    """测试推荐接口平均响应时间"""
    times = []
    for i in range(n):
        start = time.perf_counter()
        try:
            resp = requests.get(RECOMMEND_URL, timeout=5)
            elapsed = (time.perf_counter() - start) * 1000  # ms
            if resp.status_code == 200:
                times.append(elapsed)
        except Exception as e:
            print(f"请求失败: {e}")
    if times:
        avg = statistics.mean(times)
        p95 = statistics.quantiles(times, n=100)[94] if len(times) >= 100 else max(times)
        print(f"推荐接口响应时间 (样本数 {len(times)}):")
        print(f"  平均: {avg:.2f} ms")
        print(f"  95分位: {p95:.2f} ms")
        return avg, p95
    else:
        print("无成功请求")
        return None, None

def test_concurrent_users(n=100):
    """测试100并发用户"""
    def worker():
        try:
            resp = requests.get(RECOMMEND_URL, timeout=10)
            return resp.status_code == 200
        except:
            return False
    
    with ThreadPoolExecutor(max_workers=n) as executor:
        futures = [executor.submit(worker) for _ in range(n)]
        results = [f.result() for f in as_completed(futures)]
    success = sum(results)
    print(f"并发测试: {n} 个并发请求, 成功 {success} 次, 成功率 {success/n*100:.1f}%")
    return success

def test_audio_first_byte(n=10):
    """测试音频首包时间"""
    times = []
    for i in range(n):
        start = time.perf_counter()
        try:
            with requests.get(AUDIO_URL, stream=True, timeout=10) as r:
                if r.status_code == 200 or r.status_code == 206:
                    # 读取第一个字节
                    for chunk in r.iter_content(chunk_size=1):
                        first_byte = (time.perf_counter() - start) * 1000
                        times.append(first_byte)
                        break
        except Exception as e:
            print(f"音频请求失败: {e}")
    if times:
        avg = statistics.mean(times)
        print(f"音频首包时间 (样本数 {len(times)}): 平均 {avg:.2f} ms")
        return avg
    else:
        print("无法获取音频首包时间")
        return None

if __name__ == "__main__":
    print("===== 非功能需求性能测试 =====")
    test_response_time(100)
    test_concurrent_users(100)
    test_audio_first_byte(10)