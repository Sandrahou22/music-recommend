# routes/__init__.py
from .recommendation import bp as recommendation_bp
from .song import bp as song_bp
from .user import bp as user_bp

# 导出所有蓝图
__all__ = ['recommendation_bp', 'song_bp', 'user_bp']