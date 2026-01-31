from functools import wraps
from time import time
from threading import Lock
from flask import request, current_app

class SimpleRateLimiter:
    """内存版简单限流器（生产环境建议用Redis）"""
    def __init__(self, max_requests: int = 100, window: int = 60):
        self.max_requests = max_requests
        self.window = window  # 秒
        self.clients = {}
        self.lock = Lock()
    
    def is_allowed(self, key: str) -> bool:
        now = time()
        with self.lock:
            if key not in self.clients:
                self.clients[key] = []
            
            # 清理过期记录
            self.clients[key] = [t for t in self.clients[key] if now - t < self.window]
            
            if len(self.clients[key]) < self.max_requests:
                self.clients[key].append(now)
                return True
            return False

# 全局限流器实例
limiter = SimpleRateLimiter()

def rate_limit(max_requests: int = None, window: int = None):
    """限流装饰器"""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            # 根据配置决定是否启用
            if not current_app.config.get('RATELIMIT_ENABLED', True):
                return f(*args, **kwargs)
            
            # 生成客户端标识（IP + 用户ID如果有）
            client_ip = request.remote_addr or 'unknown'
            user_id = request.args.get('user_id', '') or request.json.get('user_id', '') if request.json else ''
            key = f"{client_ip}:{user_id}"
            
            max_req = max_requests or current_app.config.get('DEFAULT_RATE_LIMIT', "100 per minute")
            # 解析 "100 per minute"
            try:
                limit_num = int(max_req.split()[0])
            except:
                limit_num = 100
            
            if not limiter.is_allowed(key):
                return {"code": 429, "message": "请求过于频繁，请稍后重试", "success": False}, 429
            
            return f(*args, **kwargs)
        return wrapped
    return decorator