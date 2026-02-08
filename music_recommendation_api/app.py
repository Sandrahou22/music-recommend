import os
import sys
import threading
import time
import logging
from logging.handlers import RotatingFileHandler

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

# 导入配置（包括所有配置类）
from config import (
    Config, 
    DevelopmentConfig,  # 明确导入
    ProductionConfig,   # 明确导入
    config_map
)

def setup_logging(app: Flask):
    """配置日志"""
    if not app.debug:
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, 'recommender.log'),
            maxBytes=10*1024*1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)
        
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)

def create_app(config_name: str = None):
    """应用工厂"""
    if config_name is None:
        config_name = os.getenv('FLASK_ENV', 'development')
    
    # 验证配置
    try:
        Config.validate()
    except ValueError as e:
        print(f"配置验证失败: {e}")
        raise
    
    app = Flask(__name__)
    
    # 关键修复：确保配置类存在
    config_class = config_map.get(config_name)
    if config_class is None:
        print(f"警告: 未知配置 '{config_name}'，使用 development")
        config_class = DevelopmentConfig
    
    app.config.from_object(config_class)
    setup_logging(app)
    
    # 在app.py的create_app函数中，修改CORS配置：
    # 【关键修复】添加详细的CORS配置
    CORS(app, resources={
        r"/api/*": {
            "origins": ["http://localhost:8000", "http://127.0.0.1:8000"],
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
            "allow_headers": ["Content-Type", "Authorization", "X-Requested-With", "Accept"],
            "expose_headers": ["Content-Range", "X-Content-Range"],
            "supports_credentials": True,
            "max_age": 86400
        }
    })

    # 添加全局的CORS处理
    #@app.after_request
    #def after_request(response):
    #    """添加CORS头到所有响应"""
    #    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8000')
    #    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    #    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    #    response.headers.add('Access-Control-Allow-Credentials', 'true')
    #    return response

    # 添加一个处理OPTIONS请求的路由
    @app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
    @app.route('/<path:path>', methods=['OPTIONS'])
    def options_handler(path):
        """处理所有OPTIONS预检请求"""
        return '', 200
    
    # 注册蓝图
    from routes import recommendation, user, song
    app.register_blueprint(recommendation.bp, url_prefix='/api/v1')
    app.register_blueprint(song.bp, url_prefix='/api/v1/songs')
    app.register_blueprint(user.bp, url_prefix='/api/v1/users')
    
    # 注册蓝图（管理员）
    from routes import admin
    app.register_blueprint(admin.bp, url_prefix='/api/v1/admin')

    
    # ========== 添加系统统计端点 ==========
    @app.route('/api/v1/system/info', methods=['GET'])
    def get_system_info():
        """获取系统信息"""
        from recommender_service import recommender_service
        
        return jsonify({
            "success": True,
            "data": {
                "name": "智能音乐推荐系统",
                "version": "1.0.0",
                "status": "running",
                "database": "connected" if recommender_service._engine else "disconnected",
                "model_loaded": recommender_service._model_loaded,
                "total_users": 43355,  # 可以从数据库获取
                "total_songs": 16588,  # 可以从数据库获取
                "uptime": time.time() - app.start_time if hasattr(app, 'start_time') else 0
            }
        })
    
    # 记录启动时间
    app.start_time = time.time()

    # 错误处理
    register_error_handlers(app)
    
    # API根目录路由 - 重命名为 api_index 避免冲突
    @app.route('/api/v1')
    def api_index():
        return jsonify({
            "service": "Music Recommendation API",
            "version": "1.0",
            "status": "running",
            "endpoints": {
                "health": "/health",
                "api_v1": "/api/v1",
                "recommend": "/api/v1/recommend/<user_id>",
                "hot_songs": "/api/v1/songs/hot",
                "user_profile": "/api/v1/users/<user_id>/profile",
                "song_detail": "/api/v1/songs/<song_id>"
            }
        })
    
    # 主页面路由 - 保持为 index
    @app.route('/')
    def index():
        return jsonify({
            "service": "Music Recommendation API",
            "status": "running",
            "version": "1.0",
            "api_root": "/api/v1",
            "health_check": "/health",
            "endpoints": {
                "api_v1": "/api/v1",
                "recommend": "/api/v1/recommend/<user_id>",
                "hot_songs": "/api/v1/songs/hot",
                "user_profile": "/api/v1/users/<user_id>/profile",
                "song_detail": "/api/v1/songs/<song_id>"
            }
        })
    
    # 健康检查路由 - 重命名为 app_health
    @app.route('/health')
    @app.route('/api/v1/health')
    def app_health():
        from recommender_service import recommender_service
        status = recommender_service.health_check()
        code = 200 if status['healthy'] else 503
        return jsonify(status), code
    
    # 后台初始化
    init_recommender_in_background(app)
    
    return app

def register_error_handlers(app: Flask):
    """注册错误处理器"""
    
    @app.errorhandler(400)
    def bad_request(e):
        return jsonify({"code": 400, "message": "请求参数错误", "success": False}), 400
    
    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"code": 404, "message": "资源不存在", "success": False}), 404
    
    @app.errorhandler(500)
    def internal_error(e):
        app.logger.exception("服务器内部错误")
        return jsonify({"code": 500, "message": "服务器内部错误", "success": False}), 500

def init_recommender_in_background(app: Flask):
    """后台初始化"""
    def auto_init():
        time.sleep(2)
        try:
            from recommender_service import recommender_service
            app.logger.info("后台初始化推荐系统...")
            if recommender_service.initialize():
                app.logger.info("推荐系统初始化完成")
            else:
                app.logger.error("推荐系统初始化失败")
        except Exception as e:
            app.logger.error(f"初始化异常: {e}", exc_info=True)
    
    thread = threading.Thread(target=auto_init, daemon=True)
    thread.start()

# 全局应用实例
try:
    app = create_app()
except Exception as e:
    print(f"创建应用失败: {e}")
    import traceback
    traceback.print_exc()
    raise

if __name__ == '__main__':
    host = os.getenv('FLASK_HOST', '127.0.0.1')
    port = int(os.getenv('FLASK_PORT', 5000))
    
    print(f"启动服务器 http://{host}:{port}")
    app.run(host=host, port=port, debug=app.config.get('DEBUG', True), threaded=True)