#!/usr/bin/env python3
# 快速修复脚本 - 修复版本
import os
import shutil
import tempfile
import sqlite3
from pathlib import Path

def setup_local_fonts():
    """设置本地字体文件"""
    print("设置本地字体文件...")
    
    # 创建目录
    static_dir = Path("static/css")
    static_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建简单的 Font Awesome 替代样式
    font_css = """/* 简化的 Font Awesome 替代 */
.fa, .fas, .far, .fal, .fad, .fab {
    font-family: 'Font Awesome 6 Free', 'Font Awesome 6 Brands', sans-serif;
    font-weight: 900;
}

.fa-music::before { content: "\\f001"; }
.fa-home::before { content: "\\f015"; }
.fa-list::before { content: "\\f03a"; }
.fa-compass::before { content: "\\f14e"; }
.fa-user::before { content: "\\f007"; }
.fa-moon::before { content: "\\f186"; }
.fa-sun::before { content: "\\f185"; }
.fa-search::before { content: "\\f002"; }
.fa-users::before { content: "\\f0c0"; }
.fa-chart-line::before { content: "\\f201"; }
.fa-fire::before { content: "\\f06d"; }
.fa-play::before { content: "\\f04b"; }
.fa-pause::before { content: "\\f04c"; }
.fa-step-backward::before { content: "\\f048"; }
.fa-step-forward::before { content: "\\f051"; }
.fa-volume-up::before { content: "\\f028"; }
.fa-sync-alt::before { content: "\\f2f1"; }
.fa-heart::before { content: "\\f004"; }
.fa-star::before { content: "\\f005"; }
.fa-info::before { content: "\\f129"; }
.fa-history::before { content: "\\f1da"; }
.fa-thumbs-up::before { content: "\\f164"; }
.fa-thumbs-down::before { content: "\\f165"; }
.fa-forward::before { content: "\\f04e"; }
.fa-save::before { content: "\\f0c7"; }
.fa-plus::before { content: "\\f067"; }
.fa-random::before { content: "\\f074"; }
.fa-times::before { content: "\\f00d"; }
.fa-step-backward::before { content: "\\f048"; }
.fa-step-forward::before { content: "\\f051"; }
.fa-volume-up::before { content: "\\f028"; }
.fa-bell::before { content: "\\f0f3"; }
.fa-headphones::before { content: "\\f025"; }
.fa-filter::before { content: "\\f0b0"; }
.fa-search::before { content: "\\f002"; }
.fa-exclamation-triangle::before { content: "\\f071"; }
.fa-check-circle::before { content: "\\f058"; }
.fa-times-circle::before { content: "\\f057"; }
.fa-info-circle::before { content: "\\f05a"; }
"""
    
    with open(static_dir / "font-awesome-local.css", "w", encoding="utf-8") as f:
        f.write(font_css)
    
    print("✓ 本地字体文件创建完成")
    return True

def update_html_for_local_fonts():
    """更新 HTML 使用本地字体"""
    print("更新 HTML 文件...")
    
    html_file = Path("index.html")
    if not html_file.exists():
        print("✗ index.html 文件不存在")
        return False
    
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 替换字体链接
        new_content = content.replace(
            'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css',
            'static/css/font-awesome-local.css'
        )
        
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(new_content)
        
        print("✓ HTML 文件更新完成")
        return True
    except Exception as e:
        print(f"✗ 更新 HTML 失败: {e}")
        return False

def create_mock_data_routes():
    """创建模拟数据路由"""
    print("创建模拟数据路由...")
    
    mock_routes = '''# mock_routes.py - 模拟数据路由
from flask import Blueprint, jsonify, request
import random
from datetime import datetime, timedelta

mock_bp = Blueprint('mock', __name__)

# 模拟流派数据
MOCK_GENRES = ['流行', '摇滚', '民谣', '电子', '嘻哈', '爵士', '古典', 'R&B', '金属', '放克', '灵魂', '乡村']

# 模拟艺术家
MOCK_ARTISTS = ['周杰伦', '林俊杰', '邓紫棋', '五月天', 'Taylor Swift', 'Ed Sheeran', 'Adele', 'Coldplay', 'Maroon 5', 'Bruno Mars']

@mock_bp.route('/api/v1/mock/songs/hot')
def mock_hot_songs():
    """模拟热门歌曲"""
    tier = request.args.get('tier', 'all')
    limit = int(request.args.get('limit', 20))
    
    songs = []
    for i in range(min(limit, 20)):
        genre = random.choice(MOCK_GENRES)
        songs.append({
            'song_id': f'mock_hot_{i+1:03d}',
            'song_name': f'{genre}歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': genre,
            'popularity': random.randint(60, 95),
            'audio_features': {
                'danceability': round(random.uniform(0.3, 0.9), 2),
                'energy': round(random.uniform(0.4, 0.95), 2),
                'valence': round(random.uniform(0.3, 0.8), 2),
                'tempo': random.randint(80, 160)
            }
        })
    
    return jsonify({"success": True, "data": {"songs": songs}})

@mock_bp.route('/api/v1/mock/songs/by-genre')
def mock_songs_by_genre():
    """模拟按流派筛选"""
    genre = request.args.get('genre', '流行')
    limit = int(request.args.get('limit', 12))
    
    songs = []
    for i in range(min(limit, 12)):
        songs.append({
            'song_id': f'mock_genre_{genre}_{i+1:03d}',
            'song_name': f'{genre}歌曲示例 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': genre,
            'popularity': random.randint(50, 90),
            'audio_features': {
                'danceability': round(random.uniform(0.3, 0.9), 2),
                'energy': round(random.uniform(0.4, 0.95), 2),
                'valence': round(random.uniform(0.3, 0.8), 2),
                'tempo': random.randint(80, 160)
            }
        })
    
    return jsonify({
        "success": True,
        "data": {
            "songs": songs,
            "pagination": {
                "page": 1,
                "limit": limit,
                "total": 50,
                "has_more": True
            }
        }
    })

@mock_bp.route('/api/v1/mock/users/<user_id>/history')
def mock_user_history(user_id):
    """模拟用户历史"""
    limit = int(request.args.get('limit', 10))
    
    history = []
    for i in range(min(limit, 10)):
        days_ago = random.randint(0, 30)
        history.append({
            'song_id': f'mock_hist_{i+1:03d}',
            'song_name': f'历史歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': random.choice(MOCK_GENRES),
            'popularity': random.randint(40, 85),
            'behavior': random.choice(['播放', '喜欢', '收藏']),
            'time_ago': f'{days_ago}天前'
        })
    
    return jsonify({
        "success": True,
        "data": {
            "user_id": user_id,
            "history": history,
            "total": len(history)
        }
    })

@mock_bp.route('/api/v1/mock/users/<user_id>/activity')
def mock_user_activity(user_id):
    """模拟用户活动"""
    limit = int(request.args.get('limit', 8))
    
    activities = []
    for i in range(min(limit, 8)):
        hours_ago = random.randint(1, 168)  # 1-168小时前
        activity_type = random.choice(['play', 'like', 'collect'])
        
        activities.append({
            'activity_id': i+1,
            'song_id': f'mock_act_{i+1:03d}',
            'song_name': f'活动歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'behavior_type': activity_type,
            'action_text': '播放了' if activity_type == 'play' else '喜欢了' if activity_type == 'like' else '收藏了',
            'icon': 'fas fa-play' if activity_type == 'play' else 'fas fa-heart' if activity_type == 'like' else 'fas fa-star',
            'time_ago': f'{hours_ago}小时前'
        })
    
    return jsonify({
        "success": True,
        "data": {
            "user_id": user_id,
            "activities": activities,
            "summary": {
                'total_activities': len(activities),
                'last_activity': '1小时前',
                'activity_by_type': {'play': 5, 'like': 2, 'collect': 1}
            },
            "period_days": 7
        }
    })

@mock_bp.route('/api/v1/mock/songs/genres')
def mock_genres():
    """模拟流派列表"""
    genres = []
    for i, genre in enumerate(MOCK_GENRES):
        genres.append({
            'genre': genre,
            'song_count': random.randint(100, 5000)
        })
    
    return jsonify({"success": True, "data": {"genres": genres}})

@mock_bp.route('/api/v1/mock/recommend/<user_id>')
def mock_recommendations(user_id):
    """模拟推荐"""
    algorithm = request.args.get('algorithm', 'hybrid')
    n = int(request.args.get('n', 10))
    
    recommendations = []
    for i in range(n):
        genre = random.choice(MOCK_GENRES)
        recommendations.append({
            'song_id': f'mock_rec_{user_id}_{i+1:03d}',
            'song_name': f'为您推荐的{genre}歌曲 {i+1}',
            'artists': random.choice(MOCK_ARTISTS),
            'genre': genre,
            'popularity': random.randint(60, 95),
            'score': round(random.uniform(0.7, 0.95), 3),
            'cold_start': random.choice([True, False])
        })
    
    return jsonify({
        "success": True,
        "data": {
            "user_id": user_id,
            "algorithm": algorithm,
            "recommendations": recommendations,
            "count": len(recommendations)
        }
    })

@mock_bp.route('/api/v1/mock/health')
def mock_health():
    """模拟健康检查"""
    return jsonify({
        "status": "healthy",
        "healthy": True,
        "ready": True,
        "timestamp": datetime.now().isoformat(),
        "service": "Mock Music Recommendation API"
    })
'''
    
    try:
        with open("mock_routes.py", "w", encoding="utf-8") as f:
            f.write(mock_routes)
        
        print("✓ 模拟数据路由创建完成")
        return True
    except Exception as e:
        print(f"✗ 创建模拟路由失败: {e}")
        return False

def update_app_for_mock_routes():
    """更新 app.py 注册模拟路由"""
    print("更新 app.py...")
    
    app_file = Path("app.py")
    if not app_file.exists():
        print("✗ app.py 文件不存在")
        return False
    
    try:
        with open(app_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 检查是否已经导入了mock_routes
        if 'from mock_routes import mock_bp' not in content:
            # 在导入语句后添加
            import_line = 'from routes import recommendation, user, song'
            if import_line in content:
                new_import = f'{import_line}\nfrom mock_routes import mock_bp'
                content = content.replace(import_line, new_import)
            else:
                # 如果找不到，尝试在其他位置添加
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if 'from routes import' in line:
                        lines.insert(i+1, 'from mock_routes import mock_bp')
                        break
                content = '\n'.join(lines)
        
        # 检查是否已经注册了mock_bp
        if 'app.register_blueprint(mock_bp' not in content:
            # 在用户蓝图注册后添加
            user_bp_line = "app.register_blueprint(user.bp, url_prefix='/api/v1/users')"
            if user_bp_line in content:
                new_user_bp = f"{user_bp_line}\n    app.register_blueprint(mock_bp, url_prefix='/api/v1/mock')"
                content = content.replace(user_bp_line, new_user_bp)
            else:
                # 如果找不到，尝试在蓝图注册区域添加
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if 'app.register_blueprint' in line and 'user' in line:
                        lines.insert(i+1, "    app.register_blueprint(mock_bp, url_prefix='/api/v1/mock')")
                        break
                content = '\n'.join(lines)
        
        with open(app_file, "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✓ app.py 更新完成")
        return True
    except Exception as e:
        print(f"✗ 更新 app.py 失败: {e}")
        return False

def update_js_for_mock_api():
    """更新前端JS使用模拟API"""
    print("更新 script.js...")
    
    js_file = Path("script.js")
    if not js_file.exists():
        print("✗ script.js 文件不存在")
        return False
    
    try:
        with open(js_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 添加API模式切换
        api_config = '''
// API模式切换
const USE_MOCK_API = true;  // 设置为 true 使用模拟API，false 使用真实API
const API_BASE_URL = USE_MOCK_API ? "http://127.0.0.1:5000/api/v1/mock" : "http://127.0.0.1:5000/api/v1";
'''
        
        # 替换API基础URL
        if 'const API_BASE_URL = "http://127.0.0.1:5000/api/v1"' in content:
            content = content.replace(
                'const API_BASE_URL = "http://127.0.0.1:5000/api/v1"',
                api_config.strip()
            )
        else:
            # 如果找不到，添加在顶部
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'const ENDPOINTS = {' in line:
                    lines.insert(i, api_config)
                    break
            content = '\n'.join(lines)
        
        # 更新端点配置
        if 'const ENDPOINTS = {' in content:
            new_endpoints = '''const ENDPOINTS = {
    recommend: (userId, algorithm, count) => 
        `${API_BASE_URL}/recommend/${userId}?algorithm=${algorithm}&n=${count}`,
    hotSongs: (tier) => 
        `${API_BASE_URL}/songs/hot?tier=${tier}`,
    songDetail: (songId) => 
        `${API_BASE_URL}/songs/${songId}`,
    userProfile: (userId) => 
        `${API_BASE_URL}/users/${userId}/profile`,
    userHistory: (userId) =>
        `${API_BASE_URL}/users/${userId}/history`,
    userActivity: (userId) =>
        `${API_BASE_URL}/users/${userId}/activity`,
    songsByGenre: (genre, limit) =>
        `${API_BASE_URL}/songs/by-genre?genre=${genre}&limit=${limit || 12}`,
    genres: () =>
        `${API_BASE_URL}/songs/genres`,
    feedback: `${API_BASE_URL}/feedback`,
    health: `${API_BASE_URL}/health`
};'''
            # 找到ENDPOINTS定义并替换
            lines = content.split('\n')
            start_idx = -1
            end_idx = -1
            brace_count = 0
            
            for i, line in enumerate(lines):
                if 'const ENDPOINTS = {' in line:
                    start_idx = i
                    brace_count = 1
                elif start_idx != -1:
                    if '{' in line:
                        brace_count += line.count('{')
                    if '}' in line:
                        brace_count -= line.count('}')
                        if brace_count == 0:
                            end_idx = i
                            break
            
            if start_idx != -1 and end_idx != -1:
                # 替换
                new_lines = lines[:start_idx] + [new_endpoints] + lines[end_idx+1:]
                content = '\n'.join(new_lines)
            else:
                # 如果找不到完整定义，简单替换
                content = content.replace('const ENDPOINTS = {', new_endpoints, 1)
        
        with open(js_file, "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✓ script.js 更新完成")
        return True
    except Exception as e:
        print(f"✗ 更新 script.js 失败: {e}")
        return False

def create_test_database_config():
    """创建测试数据库配置 - 修复版本"""
    print("创建测试数据库配置...")
    
    config_file = Path("config.py")
    if not config_file.exists():
        print("✗ config.py 文件不存在")
        return False
    
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 检查是否已经有 TestingConfig 类
        if 'class TestingConfig' not in content:
            # 在 Config 类后添加 TestingConfig 类
            config_class_end = content.find('\nclass DevelopmentConfig')
            if config_class_end == -1:
                config_class_end = content.find('\nclass ProductionConfig')
            
            if config_class_end != -1:
                # 创建测试配置类
                test_config = '''

class TestingConfig(Config):
    """测试环境配置"""
    TESTING = True
    DEBUG = True
    
    @classmethod
    def get_db_connection_string(cls):
        """使用SQLite作为测试数据库"""
        import tempfile
        import sqlite3
        
        # 创建临时SQLite数据库
        temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        temp_db.close()
        
        TEST_DB_PATH = temp_db.name
        return f"sqlite:///{TEST_DB_PATH}"
'''
                
                # 插入测试配置类
                new_content = content[:config_class_end] + test_config + content[config_class_end:]
                content = new_content
                
                # 更新 config_map 添加 testing 配置
                if "'testing': TestingConfig" not in content:
                    # 找到 config_map 定义
                    config_map_start = content.find("config_map = {")
                    if config_map_start != -1:
                        # 在 config_map 中添加 testing
                        insert_point = content.find("'default': DevelopmentConfig")
                        if insert_point != -1:
                            # 在 default 前添加 testing
                            before_default = content[:insert_point]
                            after_default = content[insert_point:]
                            new_content = before_default + "    'testing': TestingConfig,\n    " + after_default
                            content = new_content
        
        with open(config_file, "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✓ 测试数据库配置创建完成")
        return True
    except Exception as e:
        print(f"✗ 创建测试数据库配置失败: {e}")
        return False

def create_simple_fix():
    """创建简单的修复版本，不修改太多文件"""
    print("创建简单修复版本...")
    
    # 1. 创建本地字体文件
    setup_local_fonts()
    
    # 2. 创建模拟路由
    create_mock_data_routes()
    
    # 3. 创建一个简单的修复版 script.js
    create_simple_script_js()
    
    print("✓ 简单修复完成")
    return True

def create_simple_script_js():
    """创建简单修复的 script.js"""
    print("创建简单 script.js 修复...")
    
    simple_script = '''// 简单修复版 script.js
// API配置 - 使用模拟API
const USE_MOCK_API = true;
const API_BASE_URL = USE_MOCK_API ? "http://127.0.0.1:5000/api/v1/mock" : "http://127.0.0.1:5000/api/v1";

const ENDPOINTS = {
    recommend: (userId, algorithm, count) => 
        `${API_BASE_URL}/recommend/${userId}?algorithm=${algorithm}&n=${count}`,
    hotSongs: (tier) => 
        `${API_BASE_URL}/songs/hot?tier=${tier}`,
    songDetail: (songId) => 
        `${API_BASE_URL}/songs/${songId}`,
    userProfile: (userId) => 
        `${API_BASE_URL}/users/${userId}/profile`,
    userHistory: (userId) =>
        `${API_BASE_URL}/users/${userId}/history`,
    userActivity: (userId) =>
        `${API_BASE_URL}/users/${userId}/activity`,
    songsByGenre: (genre, limit) =>
        `${API_BASE_URL}/songs/by-genre?genre=${genre}&limit=${limit || 12}`,
    genres: () =>
        `${API_BASE_URL}/songs/genres`,
    feedback: `${API_BASE_URL}/feedback`,
    health: `${API_BASE_URL}/health`
};

// 全局变量
let currentUser = "1001";
let currentAlgorithm = "hybrid";
let currentRecommendations = [];
let currentHotSongs = [];
let allGenres = [];
let isPlaying = false;
let currentSongIndex = 0;
let playerInterval;

// DOM加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    initApp();
});

// 初始化应用
function initApp() {
    console.log('初始化应用...');
    
    // 设置示例用户
    const userIdInput = document.getElementById('user-id-input');
    if (userIdInput) userIdInput.value = currentUser;
    
    // 设置事件监听器
    setupEventListeners();
    
    // 加载热门歌曲
    loadHotSongs('all');
    
    // 更新统计信息
    updateStats();
    
    // 初始化主题
    initTheme();
    
    console.log('应用初始化完成');
}

// 设置事件监听器
function setupEventListeners() {
    // 导航栏切换
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('href').substring(1);
            switchSection(targetId);
            
            // 更新活跃状态
            document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
            this.classList.add('active');
        });
    });
    
    // 获取推荐按钮
    const searchBtn = document.getElementById('search-btn');
    if (searchBtn) {
        searchBtn.addEventListener('click', getRecommendations);
    }
    
    const refreshBtn = document.getElementById('refresh-recommendations');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', getRecommendations);
    }
    
    // 热门歌曲标签切换
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const tier = this.dataset.tier;
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            loadHotSongs(tier);
        });
    });
    
    // 播放器控制
    const playBtn = document.getElementById('play-btn');
    if (playBtn) {
        playBtn.addEventListener('click', togglePlayback);
    }
    
    // 简单的事件代理
    document.addEventListener('click', function(e) {
        // 点击流派标签
        if (e.target.classList.contains('genre-tag-btn')) {
            const genre = e.target.dataset.genre;
            filterSongsByGenre(genre);
        }
        
        // 点击歌曲卡片
        if (e.target.closest('.song-card')) {
            const songCard = e.target.closest('.song-card');
            const songId = songCard.dataset.songId;
            console.log('点击歌曲:', songId);
        }
    });
}

// 切换页面区域
function switchSection(sectionId) {
    // 隐藏所有区域
    document.querySelectorAll('.section').forEach(section => {
        section.classList.remove('active');
    });
    
    // 显示目标区域
    const targetSection = document.getElementById(sectionId);
    if (targetSection) {
        targetSection.classList.add('active');
    }
}

// 初始化主题
function initTheme() {
    const savedTheme = localStorage.getItem('theme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);
    
    const themeToggleBtn = document.getElementById('theme-toggle');
    if (themeToggleBtn) {
        if (savedTheme === 'dark') {
            themeToggleBtn.innerHTML = '<i class="fas fa-sun"></i>';
        } else {
            themeToggleBtn.innerHTML = '<i class="fas fa-moon"></i>';
        }
        
        themeToggleBtn.addEventListener('click', function() {
            const currentTheme = document.documentElement.getAttribute('data-theme');
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
            
            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('theme', newTheme);
            
            if (newTheme === 'dark') {
                this.innerHTML = '<i class="fas fa-sun"></i>';
            } else {
                this.innerHTML = '<i class="fas fa-moon"></i>';
            }
        });
    }
}

// 获取推荐
async function getRecommendations() {
    // 获取用户ID
    const userIdInput = document.getElementById('user-id-input');
    if (userIdInput) {
        currentUser = userIdInput.value.trim();
    }
    
    if (!currentUser) {
        showNotification('请输入用户ID', 'warning');
        return;
    }
    
    // 获取算法和数量
    const algorithm = document.getElementById('rec-algorithm-select')?.value || 'hybrid';
    const count = document.getElementById('rec-count-select')?.value || 10;
    
    showLoading(true);
    
    try {
        const response = await fetch(ENDPOINTS.recommend(currentUser, algorithm, count));
        const data = await response.json();
        
        if (data.success) {
            currentRecommendations = data.data.recommendations || [];
            
            // 显示推荐
            displayRecommendations(currentRecommendations);
            
            // 更新显示信息
            const currentUserIdEl = document.getElementById('current-user-id');
            const currentAlgorithmEl = document.getElementById('current-algorithm');
            const currentCountEl = document.getElementById('current-count');
            
            if (currentUserIdEl) currentUserIdEl.textContent = currentUser;
            if (currentAlgorithmEl) currentAlgorithmEl.textContent = getAlgorithmName(algorithm);
            if (currentCountEl) currentCountEl.textContent = count;
            
            // 获取用户历史记录
            loadUserHistory(currentUser);
            
            showNotification(`成功生成${currentRecommendations.length}条推荐`, 'success');
            
            // 切换到推荐区域
            switchSection('recommendations');
        } else {
            throw new Error(data.message || '获取推荐失败');
        }
    } catch (error) {
        console.error('获取推荐失败:', error);
        showNotification(`获取推荐失败: ${error.message}`, 'error');
        
        // 显示模拟数据作为备选
        displayMockRecommendations();
    } finally {
        showLoading(false);
    }
}

// 显示推荐结果
function displayRecommendations(recommendations) {
    const container = document.getElementById('recommendations-container');
    if (!container) return;
    
    if (!recommendations || recommendations.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-music"></i>
                <p>没有找到推荐结果</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = recommendations.map((song, index) => `
        <div class="song-card" data-song-id="${song.song_id}">
            <div class="song-card-header">
                <i class="fas fa-music"></i>
                <span>推荐 #${index + 1}</span>
                ${song.cold_start ? '<span class="cold-badge">冷启动</span>' : ''}
            </div>
            <div class="song-card-body">
                <h3 class="song-title">${song.song_name || '未知歌曲'}</h3>
                <p class="song-artist">${song.artists || '未知艺术家'}</p>
                <div class="song-meta">
                    <span class="genre-tag">${song.genre || '未知流派'}</span>
                    <span class="popularity-badge">${song.popularity || 50}</span>
                </div>
                <div class="song-score">
                    <div class="score-bar">
                        <div class="score-fill" style="width: ${(song.score || 0.5) * 100}%"></div>
                    </div>
                    <span class="score-text">推荐度: ${((song.score || 0.5) * 100).toFixed(1)}%</span>
                </div>
                <div class="song-actions">
                    <button class="action-btn play-song-btn">
                        <i class="fas fa-play"></i> 播放
                    </button>
                    <button class="action-btn detail-btn">
                        <i class="fas fa-info"></i> 详情
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

// 显示模拟推荐
function displayMockRecommendations() {
    const mockRecommendations = [
        {
            song_id: "mock_001",
            song_name: "夜空中最亮的星",
            artists: "逃跑计划",
            genre: "摇滚",
            popularity: 85,
            score: 0.92,
            cold_start: false
        },
        {
            song_id: "mock_002",
            song_name: "平凡之路",
            artists: "朴树",
            genre: "民谣",
            popularity: 90,
            score: 0.88,
            cold_start: false
        },
        {
            song_id: "mock_003",
            song_name: "起风了",
            artists: "买辣椒也用券",
            genre: "流行",
            popularity: 88,
            score: 0.85,
            cold_start: false
        }
    ];
    
    currentRecommendations = mockRecommendations;
    displayRecommendations(mockRecommendations);
    
    // 更新显示信息
    const currentUserIdEl = document.getElementById('current-user-id');
    const currentAlgorithmEl = document.getElementById('current-algorithm');
    const currentCountEl = document.getElementById('current-count');
    
    if (currentUserIdEl) currentUserIdEl.textContent = currentUser;
    if (currentAlgorithmEl) currentAlgorithmEl.textContent = getAlgorithmName(currentAlgorithm);
    if (currentCountEl) currentCountEl.textContent = "3";
    
    showNotification('使用模拟数据展示', 'info');
}

// 加载热门歌曲
async function loadHotSongs(tier = 'all') {
    try {
        const response = await fetch(ENDPOINTS.hotSongs(tier));
        const data = await response.json();
        
        if (data.success) {
            currentHotSongs = data.data.songs || [];
            displayHotSongs(currentHotSongs);
        } else {
            throw new Error(data.message || '获取热门歌曲失败');
        }
    } catch (error) {
        console.error('获取热门歌曲失败:', error);
        displayMockHotSongs();
    }
}

// 显示热门歌曲
function displayHotSongs(songs) {
    const container = document.getElementById('hot-songs-container');
    if (!container) return;
    
    if (!songs || songs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-music"></i>
                <p>暂无热门歌曲数据</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = songs.slice(0, 8).map(song => `
        <div class="song-card" data-song-id="${song.song_id}">
            <div class="song-card-header">
                <i class="fas fa-fire"></i>
                <span>热门歌曲</span>
            </div>
            <div class="song-card-body">
                <h3 class="song-title">${song.song_name || '未知歌曲'}</h3>
                <p class="song-artist">${song.artists || '未知艺术家'}</p>
                <div class="song-meta">
                    <span class="genre-tag">${song.genre || '未知流派'}</span>
                    <span class="popularity-badge">${song.popularity || 50}</span>
                </div>
                <div class="song-actions">
                    <button class="action-btn play-song-btn">
                        <i class="fas fa-play"></i> 播放
                    </button>
                    <button class="action-btn detail-btn">
                        <i class="fas fa-info"></i> 详情
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

// 显示模拟热门歌曲
function displayMockHotSongs() {
    const mockHotSongs = [
        {
            song_id: "hot_001",
            song_name: "孤勇者",
            artists: "陈奕迅",
            genre: "流行",
            popularity: 95
        },
        {
            song_id: "hot_002",
            song_name: "一路生花",
            artists: "温奕心",
            genre: "流行",
            popularity: 88
        },
        {
            song_id: "hot_003",
            song_name: "New Boy",
            artists: "朴树",
            genre: "民谣",
            popularity: 82
        }
    ];
    
    displayHotSongs(mockHotSongs);
}

// 按流派筛选歌曲
async function filterSongsByGenre(genre) {
    showLoading(true);
    
    try {
        if (genre === 'all') {
            // 显示所有热门歌曲
            displayHotSongs(currentHotSongs);
            
            // 更新活跃状态
            document.querySelectorAll('.genre-tag-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            const allBtn = document.querySelector('.genre-tag-btn[data-genre="all"]');
            if (allBtn) allBtn.classList.add('active');
            
            showLoading(false);
            return;
        }
        
        // 调用API获取该流派歌曲
        const response = await fetch(ENDPOINTS.songsByGenre(genre, 12));
        const data = await response.json();
        
        if (data.success && data.data.songs && data.data.songs.length > 0) {
            // 显示筛选结果
            displayFilteredSongs(data.data.songs, genre);
            
            // 更新活跃状态
            document.querySelectorAll('.genre-tag-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            const genreBtn = document.querySelector(`.genre-tag-btn[data-genre="${genre}"]`);
            if (genreBtn) genreBtn.classList.add('active');
            
            showNotification(`找到 ${data.data.songs.length} 首${genre}歌曲`, 'success');
        } else {
            showNotification(`没有找到${genre}流派的歌曲`, 'warning');
            displayHotSongs(currentHotSongs);
        }
    } catch (error) {
        console.error('筛选歌曲失败:', error);
        showNotification(`筛选失败: ${error.message}`, 'error');
        displayHotSongs(currentHotSongs);
    } finally {
        showLoading(false);
    }
}

// 显示筛选后的歌曲
function displayFilteredSongs(songs, genre) {
    const container = document.getElementById('hot-songs-container');
    if (!container) return;
    
    if (!songs || songs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-search"></i>
                <p>没有找到"${genre}"流派的歌曲</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = songs.slice(0, 8).map(song => `
        <div class="song-card" data-song-id="${song.song_id}">
            <div class="song-card-header" style="background: linear-gradient(135deg, #7209b7, #f72585);">
                <i class="fas fa-filter"></i>
                <span>${genre}</span>
            </div>
            <div class="song-card-body">
                <h3 class="song-title">${song.song_name || '未知歌曲'}</h3>
                <p class="song-artist">${song.artists || '未知艺术家'}</p>
                <div class="song-meta">
                    <span class="genre-tag">${song.genre || '未知流派'}</span>
                    <span class="popularity-badge">${song.popularity || 50}</span>
                </div>
                <div class="song-actions">
                    <button class="action-btn play-song-btn">
                        <i class="fas fa-play"></i> 播放
                    </button>
                    <button class="action-btn detail-btn">
                        <i class="fas fa-info"></i> 详情
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

// 加载用户历史记录
async function loadUserHistory(userId) {
    try {
        const response = await fetch(ENDPOINTS.userHistory(userId));
        const data = await response.json();
        
        if (data.success) {
            displayUserHistory(data.data.history);
        } else {
            throw new Error(data.message || '获取历史记录失败');
        }
    } catch (error) {
        console.error('获取用户历史记录失败:', error);
        displayMockHistory();
    }
}

// 显示用户历史记录
function displayUserHistory(history) {
    const container = document.getElementById('history-container');
    if (!container) return;
    
    if (!history || history.length === 0) {
        container.innerHTML = `
            <div class="empty-state compact">
                <i class="fas fa-history"></i>
                <p>暂无收听记录</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = history.slice(0, 5).map(item => `
        <div class="song-item" data-song-id="${item.song_id}">
            <div class="song-icon" style="background-color: #4361ee;">
                <i class="fas fa-headphones"></i>
            </div>
            <div class="song-info">
                <h4>${item.song_name || '未知歌曲'}</h4>
                <p>${item.artists || '未知艺术家'} • ${item.behavior || '播放'} • ${item.time_ago || '刚刚'}</p>
            </div>
            <div class="song-stats">
                <span class="popularity-badge small">${item.popularity || 50}</span>
                <button class="action-btn play-song-btn" title="播放">
                    <i class="fas fa-play"></i>
                </button>
            </div>
        </div>
    `).join('');
}

// 显示模拟历史记录
function displayMockHistory() {
    const mockHistory = [
        {song_id: 'mock_1', song_name: "夜曲", artists: "周杰伦", behavior: "播放", time_ago: "2小时前", popularity: 85},
        {song_id: 'mock_2', song_name: "江南", artists: "林俊杰", behavior: "喜欢", time_ago: "5小时前", popularity: 82},
        {song_id: 'mock_3', song_name: "七里香", artists: "周杰伦", behavior: "收藏", time_ago: "昨天", popularity: 90}
    ];
    
    displayUserHistory(mockHistory);
}

// 更新统计信息
function updateStats() {
    // 这里可以从API获取实时统计，暂时使用固定值
    animateCount('user-count', 43355);
    animateCount('song-count', 16588);
    animateCount('rec-count', 500);
}

// 数字动画
function animateCount(elementId, target) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    const current = parseInt(element.textContent.replace(/,/g, '')) || 0;
    const increment = Math.ceil((target - current) / 50);
    let count = current;
    
    const timer = setInterval(() => {
        count += increment;
        if (count >= target) {
            count = target;
            clearInterval(timer);
        }
        element.textContent = count.toLocaleString();
    }, 20);
}

// 获取算法名称
function getAlgorithmName(algorithm) {
    const algorithmNames = {
        'hybrid': '混合推荐',
        'usercf': '用户协同过滤',
        'cf': '物品协同过滤',
        'content': '内容推荐',
        'mf': '矩阵分解',
        'cold': '冷启动推荐',
        'auto': '自动选择'
    };
    
    return algorithmNames[algorithm] || algorithm;
}

// 显示加载状态
function showLoading(show) {
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        if (show) {
            overlay.classList.add('active');
        } else {
            overlay.classList.remove('active');
        }
    }
}

// 显示通知
function showNotification(message, type = 'info') {
    console.log(`[${type}] ${message}`);
    
    // 创建简单的通知（可以根据需要扩展）
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.innerHTML = `
        <div class="notification-content">
            <i class="fas fa-${getNotificationIcon(type)}"></i>
            <span>${message}</span>
        </div>
        <button class="notification-close">&times;</button>
    `;
    
    // 添加到页面
    document.body.appendChild(notification);
    
    // 自动移除
    setTimeout(() => {
        if (notification.parentNode) {
            notification.remove();
        }
    }, 3000);
}

// 获取通知图标
function getNotificationIcon(type) {
    const icons = {
        'info': 'info-circle',
        'success': 'check-circle',
        'warning': 'exclamation-triangle',
        'error': 'times-circle'
    };
    
    return icons[type] || 'info-circle';
}

// 播放控制
function togglePlayback() {
    isPlaying = !isPlaying;
    const playBtn = document.getElementById('play-btn');
    if (playBtn) {
        const icon = playBtn.querySelector('i');
        if (icon) {
            icon.className = isPlaying ? 'fas fa-pause' : 'fas fa-play';
        }
    }
    
    if (isPlaying) {
        showNotification('开始播放', 'info');
    } else {
        showNotification('暂停播放', 'info');
    }
}
'''
    
    try:
        # 备份原文件
        js_file = Path("script.js")
        if js_file.exists():
            backup_file = Path("script.js.backup")
            shutil.copy2(js_file, backup_file)
            print("✓ 已备份原 script.js 文件")
        
        with open(js_file, "w", encoding="utf-8") as f:
            f.write(simple_script)
        
        print("✓ 创建简单 script.js 完成")
        return True
    except Exception as e:
        print(f"✗ 创建简单 script.js 失败: {e}")
        return False

def create_app_py_fix():
    """创建修复的 app.py"""
    print("创建修复的 app.py...")
    
    # 读取原 app.py
    app_file = Path("app.py")
    if not app_file.exists():
        print("✗ app.py 文件不存在")
        return False
    
    try:
        with open(app_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 修复 CORS 配置
        if 'CORS(app, resources={' in content:
            # 替换 CORS 配置为更宽松的版本
            new_cors_config = '''    # CORS - 更宽松的配置
    CORS(app, resources={
        r"/*": {
            "origins": "*",
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["*"]
        }
    }, supports_credentials=True)
    
    # 处理OPTIONS请求的中间件
    @app.after_request
    def after_request(response):
        """添加CORS头到所有响应"""
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        return response'''
            
            # 找到旧的 CORS 配置并替换
            lines = content.split('\n')
            new_lines = []
            in_cors_section = False
            cors_replaced = False
            
            for line in lines:
                if 'CORS(app, resources={' in line and not cors_replaced:
                    # 开始替换
                    new_lines.append(new_cors_config)
                    in_cors_section = True
                    cors_replaced = True
                elif in_cors_section and '})' in line:
                    # 跳过旧 CORS 配置的其余部分
                    in_cors_section = False
                    continue
                elif not in_cors_section:
                    new_lines.append(line)
            
            content = '\n'.join(new_lines)
        
        # 添加 mock_routes 导入
        if 'from routes import recommendation, user, song' in content:
            if 'from mock_routes import mock_bp' not in content:
                content = content.replace(
                    'from routes import recommendation, user, song',
                    'from routes import recommendation, user, song\nfrom mock_routes import mock_bp'
                )
        
        # 添加 mock_bp 注册
        if 'app.register_blueprint(user.bp, url_prefix=\'/api/v1/users\')' in content:
            if 'app.register_blueprint(mock_bp' not in content:
                content = content.replace(
                    'app.register_blueprint(user.bp, url_prefix=\'/api/v1/users\')',
                    'app.register_blueprint(user.bp, url_prefix=\'/api/v1/users\')\n    app.register_blueprint(mock_bp, url_prefix=\'/api/v1/mock\')'
                )
        
        # 备份原文件
        backup_file = Path("app.py.backup")
        shutil.copy2(app_file, backup_file)
        print("✓ 已备份原 app.py 文件")
        
        # 写入修复后的内容
        with open(app_file, "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✓ app.py 修复完成")
        return True
    except Exception as e:
        print(f"✗ 修复 app.py 失败: {e}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("音乐推荐系统快速修复工具 - 修复版本")
    print("=" * 60)
    
    success_count = 0
    total_steps = 6
    
    try:
        # 1. 设置本地字体
        print(f"\n[1/{total_steps}] 设置本地字体文件...")
        if setup_local_fonts():
            success_count += 1
        
        # 2. 更新HTML使用本地字体
        print(f"\n[2/{total_steps}] 更新HTML使用本地字体...")
        if update_html_for_local_fonts():
            success_count += 1
        
        # 3. 创建模拟数据路由
        print(f"\n[3/{total_steps}] 创建模拟数据路由...")
        if create_mock_data_routes():
            success_count += 1
        
        # 4. 修复 app.py
        print(f"\n[4/{total_steps}] 修复 app.py...")
        if create_app_py_fix():
            success_count += 1
        
        # 5. 创建简单修复的 script.js
        print(f"\n[5/{total_steps}] 创建简单修复的 script.js...")
        if create_simple_script_js():
            success_count += 1
        
        # 6. 创建测试数据库配置
        print(f"\n[6/{total_steps}] 创建测试数据库配置...")
        if create_test_database_config():
            success_count += 1
        
        print("\n" + "=" * 60)
        print(f"修复完成！成功步骤: {success_count}/{total_steps}")
        print("=" * 60)
        
        if success_count >= 4:
            print("\n✅ 修复成功！下一步操作：")
            print("1. 重启Flask服务器: python app.py")
            print("2. 刷新浏览器页面: http://localhost:8000/index.html")
            print("3. 系统现在应该可以正常工作了")
        else:
            print("\n⚠️  部分修复失败，但系统可能仍能工作")
            print("请检查错误信息并手动修复")
        
        print("\n📝 注意：")
        print("- 目前使用的是模拟数据")
        print("- 要使用真实数据，请修改 script.js 中的 USE_MOCK_API = false")
        print("- 并确保SQL Server数据库可以正常连接")
        print("\n📁 已备份的文件：")
        print("- script.js.backup (原script.js备份)")
        print("- app.py.backup (原app.py备份)")
        
    except Exception as e:
        print(f"\n❌ 修复过程中出现错误: {e}")
        print("请检查错误信息并手动修复")

if __name__ == "__main__":
    main()