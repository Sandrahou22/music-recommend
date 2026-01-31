import sys
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler

# 解决Windows中文用户名问题
import socket
original_getfqdn = socket.getfqdn

def safe_getfqdn(name=''):
    try:
        return original_getfqdn(name)
    except UnicodeDecodeError:
        return 'localhost'

socket.getfqdn = safe_getfqdn

# 设置服务器
PORT = 8000

# 创建自定义处理器以支持UTF-8
class MyHTTPRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        # 添加UTF-8编码头
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        SimpleHTTPRequestHandler.end_headers(self)
    
    def guess_type(self, path):
        # 重写guess_type以正确处理中文文件名
        try:
            return SimpleHTTPRequestHandler.guess_type(self, path)
        except:
            return 'application/octet-stream'

# 启动服务器
print("正在启动服务器...")
print(f"访问地址: http://localhost:{PORT}")
print("按 Ctrl+C 停止服务器")

os.chdir(os.path.dirname(os.path.abspath(__file__)))

try:
    server = HTTPServer(('localhost', PORT), MyHTTPRequestHandler)
    server.serve_forever()
except KeyboardInterrupt:
    print("\n服务器已停止")
except Exception as e:
    print(f"启动失败: {e}")
    input("按回车键退出...")