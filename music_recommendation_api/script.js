// 简单修复版 script.js
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
