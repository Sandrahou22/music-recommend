import os
import socket
from app import create_app

# 修复Windows中文主机名问题
# 方案1：强制使用IP地址，避免主机名解析
os.environ['FLASK_RUN_HOST'] = '127.0.0.1'

# 方案2：如果非要使用localhost，设置编码（备用）
# socket.getfqdn = lambda x: 'localhost'

app = create_app(os.getenv('FLASK_ENV', 'development'))

if __name__ == '__main__':
    # 使用IP地址而不是主机名，避免中文编码问题
    host = os.getenv('FLASK_HOST', '127.0.0.1')  # 不要用 'localhost'
    port = int(os.getenv('FLASK_PORT', 5000))
    
    print(f"Starting server on http://{host}:{port}")
    
    app.run(
        host=host,
        port=port,
        debug=app.config['DEBUG'],
        threaded=True,  # 启用多线程
        use_reloader=False  # Windows下有时文件监控会出错，如开发可设为True
    )