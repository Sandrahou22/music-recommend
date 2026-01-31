// 全局变量
let currentUser = "1001";
let currentAlgorithm = "hybrid";
let currentRecommendations = [];
let currentHotSongs = [];
let allGenres = [];
let isPlaying = false;
let currentSongIndex = 0;
let playerInterval;

// API配置
const API_BASE_URL = "http://127.0.0.1:5000/api/v1";
const ENDPOINTS = {
    recommend: (userId, algorithm, count) => 
        `${API_BASE_URL}/recommend/${userId}?algorithm=${algorithm}&n=${count}`,
    hotSongs: (tier) => 
        `${API_BASE_URL}/songs/hot?tier=${tier}`,
    songDetail: (songId) => 
        `${API_BASE_URL}/songs/${songId}`,
    userProfile: (userId) => 
        `${API_BASE_URL}/users/${userId}/profile`,
    feedback: `${API_BASE_URL}/feedback`,
    health: `${API_BASE_URL}/health`
};

// DOM加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    initApp();
});

// 初始化应用
function initApp() {
    // 检查API连接
    checkApiHealth();
    
    // 设置事件监听器
    setupEventListeners();
    
    // 加载热门歌曲
    loadHotSongs('all');
    
    // 加载流派列表
    loadGenres();
    
    // 设置示例用户
    document.getElementById('user-id-input').value = currentUser;
    
    // 更新统计信息
    updateStats();
    
    // 初始化主题
    initTheme();
}

// 检查API连接
async function checkApiHealth() {
    try {
        const response = await fetch(ENDPOINTS.health);
        const data = await response.json();
        
        if (data.healthy) {
            console.log('API连接正常');
            document.getElementById('api-url').textContent = API_BASE_URL;
        } else {
            showNotification('API服务不可用，部分功能可能受限', 'warning');
        }
    } catch (error) {
        console.error('API连接失败:', error);
        showNotification('无法连接到推荐服务，请检查后端是否运行', 'error');
    }
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
    
    // 主题切换
    document.getElementById('theme-toggle').addEventListener('click', toggleTheme);
    
    // 获取推荐按钮
    document.getElementById('search-btn').addEventListener('click', getRecommendations);
    document.getElementById('refresh-recommendations').addEventListener('click', getRecommendations);
    
    // 热门歌曲标签切换
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const tier = this.dataset.tier;
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            loadHotSongs(tier);
        });
    });
    
    // 算法选择
    document.getElementById('algorithm-select').addEventListener('change', function() {
        currentAlgorithm = this.value;
    });
    
    document.getElementById('rec-algorithm-select').addEventListener('change', function() {
        currentAlgorithm = this.value;
    });
    
    // 推荐数量选择
    document.getElementById('rec-count-select').addEventListener('change', function() {
        // 将在获取推荐时使用
    });
    
    // 流派筛选
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('genre-tag-btn')) {
            const genre = e.target.dataset.genre;
            filterSongsByGenre(genre);
            
            // 更新活跃状态
            document.querySelectorAll('.genre-tag-btn').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
        }
    });
    
    // 播放器控制
    document.getElementById('play-btn').addEventListener('click', togglePlayback);
    document.getElementById('prev-btn').addEventListener('click', playPrevious);
    document.getElementById('next-btn').addEventListener('click', playNext);
    document.getElementById('volume-slider').addEventListener('input', updateVolume);
    
    // 歌曲卡片点击
    document.addEventListener('click', function(e) {
        // 点击歌曲卡片
        if (e.target.closest('.song-card')) {
            const songCard = e.target.closest('.song-card');
            const songId = songCard.dataset.songId;
            showSongDetail(songId);
        }
        
        // 点击播放按钮
        if (e.target.closest('.play-song-btn')) {
            e.stopPropagation();
            const songCard = e.target.closest('.song-card');
            const songId = songCard.dataset.songId;
            playSong(songId);
        }
        
        // 点击反馈按钮
        if (e.target.closest('.feedback-btn')) {
            e.stopPropagation();
            const songCard = e.target.closest('.song-card');
            const songId = songCard.dataset.songId;
            showFeedbackModal(songId);
        }
    });
    
    // 模态框关闭
    document.querySelectorAll('.close-modal').forEach(btn => {
        btn.addEventListener('click', function() {
            this.closest('.modal').classList.remove('active');
        });
    });
    
    // 点击模态框背景关闭
    document.querySelectorAll('.modal').forEach(modal => {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                this.classList.remove('active');
            }
        });
    });
    
    // 保存偏好设置
    document.getElementById('save-preferences').addEventListener('click', savePreferences);
    
    // 多样性滑块
    document.getElementById('diversity-slider').addEventListener('input', function() {
        const value = parseInt(this.value);
        const labels = ['低', '较低', '中等', '较高', '高'];
        const labelIndex = Math.floor(value / 2);
        document.getElementById('diversity-value').textContent = labels[labelIndex] || '中等';
    });
    
    // 键盘快捷键
    document.addEventListener('keydown', function(e) {
        // 空格键控制播放/暂停
        if (e.code === 'Space' && !e.target.matches('input, textarea')) {
            e.preventDefault();
            togglePlayback();
        }
        
        // ESC键关闭模态框
        if (e.code === 'Escape') {
            document.querySelectorAll('.modal').forEach(modal => {
                modal.classList.remove('active');
            });
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
    
    // 如果是推荐区域且已有用户，加载推荐
    if (sectionId === 'recommendations' && currentUser) {
        if (currentRecommendations.length === 0) {
            getRecommendations();
        } else {
            displayRecommendations(currentRecommendations);
        }
    }
    
    // 如果是发现区域，加载探索内容
    if (sectionId === 'explore') {
        loadExploreContent();
    }
}

// 初始化主题
function initTheme() {
    const savedTheme = localStorage.getItem('theme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);
    
    const themeToggleBtn = document.getElementById('theme-toggle');
    if (savedTheme === 'dark') {
        themeToggleBtn.innerHTML = '<i class="fas fa-sun"></i>';
    } else {
        themeToggleBtn.innerHTML = '<i class="fas fa-moon"></i>';
    }
}

// 切换主题
function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    
    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    
    const themeToggleBtn = document.getElementById('theme-toggle');
    if (newTheme === 'dark') {
        themeToggleBtn.innerHTML = '<i class="fas fa-sun"></i>';
    } else {
        themeToggleBtn.innerHTML = '<i class="fas fa-moon"></i>';
    }
}

// 获取推荐
async function getRecommendations() {
    // 获取用户ID
    const userIdInput = document.getElementById('user-id-input');
    currentUser = userIdInput.value.trim();
    
    if (!currentUser) {
        showNotification('请输入用户ID', 'warning');
        return;
    }
    
    // 获取算法和数量
    const algorithm = document.getElementById('rec-algorithm-select').value;
    const count = document.getElementById('rec-count-select').value;
    
    // 显示加载状态
    showLoading(true);
    
    try {
        // 获取推荐
        const response = await fetch(ENDPOINTS.recommend(currentUser, algorithm, count));
        const data = await response.json();
        
        if (data.success) {
            currentRecommendations = data.data.recommendations;
            
            // 显示推荐
            displayRecommendations(currentRecommendations);
            
            // 更新显示信息
            document.getElementById('current-user-id').textContent = currentUser;
            document.getElementById('current-algorithm').textContent = getAlgorithmName(algorithm);
            document.getElementById('current-count').textContent = count;
            
            // 获取用户画像
            loadUserProfile(currentUser);
            
            // 获取历史记录
            loadUserHistory(currentUser);
            
            // 显示成功消息
            showNotification(`成功生成${currentRecommendations.length}条推荐`, 'success');
            
            // 切换到推荐区域
            switchSection('recommendations');
            
            // 更新导航状态
            document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
            document.querySelector('a[href="#recommendations"]').classList.add('active');
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
                <h3 class="song-title">${song.song_name}</h3>
                <p class="song-artist">${song.artists}</p>
                <div class="song-meta">
                    <span class="genre-tag">${song.genre || '未知流派'}</span>
                    <span class="popularity-badge">${song.popularity || 50}</span>
                </div>
                <div class="song-score">
                    <div class="score-bar">
                        <div class="score-fill" style="width: ${song.score * 100}%"></div>
                    </div>
                    <span class="score-text">推荐度: ${(song.score * 100).toFixed(1)}%</span>
                </div>
                <div class="song-actions">
                    <button class="action-btn play-song-btn">
                        <i class="fas fa-play"></i> 播放
                    </button>
                    <button class="action-btn feedback-btn">
                        <i class="fas fa-thumbs-up"></i> 反馈
                    </button>
                    <button class="action-btn detail-btn">
                        <i class="fas fa-info"></i> 详情
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

// 显示模拟推荐（当API不可用时）
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
        },
        {
            song_id: "mock_004",
            song_name: "光年之外",
            artists: "G.E.M.邓紫棋",
            genre: "流行",
            popularity: 92,
            score: 0.82,
            cold_start: true
        },
        {
            song_id: "mock_005",
            song_name: "晴天",
            artists: "周杰伦",
            genre: "流行",
            popularity: 95,
            score: 0.80,
            cold_start: false
        }
    ];
    
    currentRecommendations = mockRecommendations;
    displayRecommendations(mockRecommendations);
    
    // 更新显示信息
    document.getElementById('current-user-id').textContent = currentUser;
    document.getElementById('current-algorithm').textContent = getAlgorithmName(currentAlgorithm);
    document.getElementById('current-count').textContent = "5";
    
    // 显示模拟用户画像
    displayMockUserProfile();
    
    showNotification('使用模拟数据展示（API连接失败）', 'warning');
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
        
        // 显示模拟热门歌曲
        displayMockHotSongs();
    }
}

// 显示热门歌曲
function displayHotSongs(songs) {
    const container = document.getElementById('hot-songs-container');
    
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
        },
        {
            song_id: "hot_004",
            song_name: "漠河舞厅",
            artists: "柳爽",
            genre: "民谣",
            popularity: 85
        },
        {
            song_id: "hot_005",
            song_name: "这世界那么多人",
            artists: "莫文蔚",
            genre: "流行",
            popularity: 90
        },
        {
            song_id: "hot_006",
            song_name: "星辰大海",
            artists: "黄霄雲",
            genre: "流行",
            popularity: 87
        },
        {
            song_id: "hot_007",
            song_name: "少年",
            artists: "梦然",
            genre: "流行",
            popularity: 84
        },
        {
            song_id: "hot_008",
            song_name: "错位时空",
            artists: "艾辰",
            genre: "流行",
            popularity: 83
        }
    ];
    
    displayHotSongs(mockHotSongs);
}

// 加载用户画像
async function loadUserProfile(userId) {
    try {
        const response = await fetch(ENDPOINTS.userProfile(userId));
        const data = await response.json();
        
        if (data.success) {
            displayUserProfile(data.data);
        } else {
            throw new Error(data.message || '获取用户画像失败');
        }
    } catch (error) {
        console.error('获取用户画像失败:', error);
        displayMockUserProfile();
    }
}

// 显示用户画像
function displayUserProfile(profile) {
    document.getElementById('profile-user-id').textContent = profile.user_id || currentUser;
    document.getElementById('profile-song-count').textContent = profile.n_songs || '0';
    
    // 流行度偏好
    const popPref = profile.avg_popularity || 50;
    let popPrefText = '中等';
    if (popPref < 40) popPrefText = '偏爱冷门';
    else if (popPref > 60) popPrefText = '偏爱热门';
    document.getElementById('profile-pop-pref').textContent = popPrefText;
    
    // 偏好流派
    const genresContainer = document.getElementById('profile-genres');
    const genres = profile.top_genres || [];
    
    if (genres.length > 0) {
        genresContainer.innerHTML = genres.map(genre => `
            <span class="genre-tag">${genre}</span>
        `).join('');
    } else {
        genresContainer.innerHTML = '<span class="no-data">暂无数据</span>';
    }
    
    // 更新用户区域
    document.getElementById('profile-user-id-large').textContent = profile.user_id || currentUser;
    document.getElementById('total-listens').textContent = profile.n_songs || '0';
    document.getElementById('fav-genre').textContent = genres[0] || '未知';
    document.getElementById('avg-popularity').textContent = Math.round(popPref) || '50';
    
    // 活跃等级
    const songCount = profile.n_songs || 0;
    let activityLevel = '低活跃';
    if (songCount > 50) activityLevel = '高活跃';
    else if (songCount > 20) activityLevel = '中活跃';
    document.getElementById('activity-level').textContent = activityLevel;
}

// 显示模拟用户画像
function displayMockUserProfile() {
    const mockProfile = {
        user_id: currentUser,
        n_songs: Math.floor(Math.random() * 100) + 10,
        top_genres: ['流行', '摇滚', '民谣'].sort(() => Math.random() - 0.5).slice(0, 3),
        avg_popularity: Math.floor(Math.random() * 40) + 30
    };
    
    displayUserProfile(mockProfile);
}

// 加载用户历史记录
async function loadUserHistory(userId) {
    // 这里可以调用历史记录API，暂时使用模拟数据
    displayMockHistory();
}

// 显示模拟历史记录
function displayMockHistory() {
    const mockHistory = [
        {song_name: "夜曲", artists: "周杰伦", time: "2小时前"},
        {song_name: "江南", artists: "林俊杰", time: "5小时前"},
        {song_name: "七里香", artists: "周杰伦", time: "昨天"},
        {song_name: "倔强", artists: "五月天", time: "昨天"},
        {song_name: "小幸运", artists: "田馥甄", time: "2天前"}
    ];
    
    const container = document.getElementById('history-container');
    container.innerHTML = mockHistory.map(item => `
        <div class="song-item">
            <div class="song-icon">
                <i class="fas fa-history"></i>
            </div>
            <div class="song-info">
                <h4>${item.song_name}</h4>
                <p>${item.artists} • ${item.time}</p>
            </div>
            <button class="action-btn">
                <i class="fas fa-play"></i>
            </button>
        </div>
    `).join('');
}

// 加载流派列表
async function loadGenres() {
    // 这里可以从API获取，暂时使用固定列表
    allGenres = ['流行', '摇滚', '民谣', '电子', '嘻哈', '爵士', '古典', '乡村', 'R&B', '金属', '放克', '灵魂'];
    
    const container = document.getElementById('genre-tags');
    container.innerHTML = allGenres.map(genre => `
        <button class="genre-tag-btn" data-genre="${genre}">${genre}</button>
    `).join('');
}

// 按流派筛选歌曲
function filterSongsByGenre(genre) {
    // 这里可以调用API按流派筛选，暂时只显示提示
    showNotification(`正在筛选流派: ${genre}`, 'info');
    
    // 模拟筛选
    if (genre === 'all') {
        displayHotSongs(currentHotSongs);
    } else {
        const filtered = currentHotSongs.filter(song => 
            song.genre && song.genre.includes(genre)
        );
        
        if (filtered.length > 0) {
            displayHotSongs(filtered);
        } else {
            showNotification(`没有找到${genre}流派的歌曲`, 'warning');
        }
    }
}

// 加载探索内容
async function loadExploreContent() {
    // 这里可以加载更多探索内容，暂时显示热门歌曲
    const container = document.getElementById('explore-container');
    
    if (currentHotSongs.length > 0) {
        displayHotSongs(currentHotSongs);
    } else {
        // 加载热门歌曲
        loadHotSongs('all');
    }
}

// 显示歌曲详情
async function showSongDetail(songId) {
    try {
        const response = await fetch(ENDPOINTS.songDetail(songId));
        const data = await response.json();
        
        if (data.success) {
            displaySongDetail(data.data);
        } else {
            throw new Error(data.message || '获取歌曲详情失败');
        }
    } catch (error) {
        console.error('获取歌曲详情失败:', error);
        // 显示模拟详情
        displayMockSongDetail(songId);
    }
}

// 显示歌曲详情
function displaySongDetail(song) {
    // 更新模态框内容
    document.getElementById('modal-song-title').textContent = '歌曲详情';
    document.getElementById('detail-song-name').textContent = song.song_name || '未知歌曲';
    document.getElementById('detail-artists').textContent = song.artists || '未知艺术家';
    document.getElementById('detail-genre').textContent = song.genre || '未知流派';
    document.getElementById('detail-popularity').textContent = `流行度: ${song.popularity || 50}`;
    
    // 音频特征
    const features = song.audio_features || {};
    document.getElementById('danceability-value').textContent = (features.danceability || 0.5).toFixed(2);
    document.getElementById('danceability-bar').style.width = `${(features.danceability || 0.5) * 100}%`;
    
    document.getElementById('energy-value').textContent = (features.energy || 0.5).toFixed(2);
    document.getElementById('energy-bar').style.width = `${(features.energy || 0.5) * 100}%`;
    
    document.getElementById('valence-value').textContent = (features.valence || 0.5).toFixed(2);
    document.getElementById('valence-bar').style.width = `${(features.valence || 0.5) * 100}%`;
    
    document.getElementById('tempo-value').textContent = `${features.tempo || 120} BPM`;
    
    // 推荐理由
    const reasonText = generateRecommendationReason(song);
    document.getElementById('recommendation-reason-text').textContent = reasonText;
    
    // 设置播放按钮
    document.getElementById('play-now-btn').dataset.songId = song.song_id;
    
    // 显示模态框
    document.getElementById('song-modal').classList.add('active');
}

// 显示模拟歌曲详情
function displayMockSongDetail(songId) {
    const mockSong = {
        song_id: songId,
        song_name: "示例歌曲",
        artists: "示例艺术家",
        genre: "流行",
        popularity: 75,
        audio_features: {
            danceability: 0.7,
            energy: 0.8,
            valence: 0.6,
            tempo: 120
        }
    };
    
    displaySongDetail(mockSong);
}

// 生成推荐理由
function generateRecommendationReason(song) {
    const reasons = [
        "根据您的听歌历史和偏好推荐",
        "与您最近收听歌曲风格相似",
        "与您偏好流派高度匹配",
        "当前热门歌曲，符合大众口味",
        "小众精品，发现独特音乐",
        "基于协同过滤算法推荐"
    ];
    
    // 随机选择一个理由
    const randomIndex = Math.floor(Math.random() * reasons.length);
    return reasons[randomIndex];
}

// 播放歌曲
function playSong(songId) {
    // 查找歌曲信息
    let song;
    
    // 在推荐列表中查找
    song = currentRecommendations.find(s => s.song_id === songId);
    
    // 在热门歌曲中查找
    if (!song) {
        song = currentHotSongs.find(s => s.song_id === songId);
    }
    
    if (song) {
        // 更新播放器显示
        document.getElementById('now-playing-title').textContent = song.song_name;
        document.getElementById('now-playing-artist').textContent = song.artists;
        
        // 开始播放
        isPlaying = true;
        updatePlayButton();
        startPlaybackProgress();
        
        // 显示通知
        showNotification(`正在播放: ${song.song_name}`, 'info');
    }
}

// 切换播放/暂停
function togglePlayback() {
    isPlaying = !isPlaying;
    updatePlayButton();
    
    if (isPlaying) {
        startPlaybackProgress();
        showNotification('继续播放', 'info');
    } else {
        stopPlaybackProgress();
        showNotification('暂停播放', 'info');
    }
}

// 播放上一首
function playPrevious() {
    // 这里可以实现播放列表逻辑
    showNotification('播放上一首', 'info');
}

// 播放下一首
function playNext() {
    // 这里可以实现播放列表逻辑
    showNotification('播放下一首', 'info');
}

// 更新播放按钮
function updatePlayButton() {
    const playBtn = document.getElementById('play-btn');
    const icon = playBtn.querySelector('i');
    
    if (isPlaying) {
        icon.className = 'fas fa-pause';
        playBtn.classList.add('playing');
    } else {
        icon.className = 'fas fa-play';
        playBtn.classList.remove('playing');
    }
}

// 开始播放进度
function startPlaybackProgress() {
    let progress = 0;
    const progressFill = document.querySelector('.progress-fill');
    const currentTimeEl = document.getElementById('current-time');
    const totalTimeEl = document.getElementById('total-time');
    
    // 设置总时间（模拟）
    const totalSeconds = 180; // 3分钟
    totalTimeEl.textContent = formatTime(totalSeconds);
    
    // 清除之前的定时器
    if (playerInterval) clearInterval(playerInterval);
    
    // 开始更新进度
    playerInterval = setInterval(() => {
        if (!isPlaying) return;
        
        progress += 1;
        if (progress > totalSeconds) {
            progress = 0;
            playNext();
        }
        
        // 更新进度条
        const percentage = (progress / totalSeconds) * 100;
        progressFill.style.width = `${percentage}%`;
        
        // 更新时间显示
        currentTimeEl.textContent = formatTime(progress);
    }, 1000);
}

// 停止播放进度
function stopPlaybackProgress() {
    if (playerInterval) {
        clearInterval(playerInterval);
        playerInterval = null;
    }
}

// 格式化时间（秒 -> MM:SS）
function formatTime(seconds) {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs < 10 ? '0' : ''}${secs}`;
}

// 更新音量
function updateVolume() {
    const volume = document.getElementById('volume-slider').value;
    // 这里可以实际控制音频音量
    console.log(`音量设置为: ${volume}%`);
}

// 显示反馈模态框
function showFeedbackModal(songId) {
    document.getElementById('feedback-modal').classList.add('active');
    
    // 设置反馈按钮的事件
    document.querySelectorAll('.feedback-btn').forEach(btn => {
        btn.onclick = function() {
            const action = this.dataset.action;
            submitFeedback(songId, action);
        };
    });
}

// 提交反馈
async function submitFeedback(songId, action) {
    const comment = document.getElementById('feedback-comment').value;
    
    try {
        const response = await fetch(ENDPOINTS.feedback, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                user_id: currentUser,
                song_id: songId,
                action: action,
                context: {
                    comment: comment,
                    timestamp: new Date().toISOString()
                }
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('反馈已提交，感谢您的意见！', 'success');
            document.getElementById('feedback-modal').classList.remove('active');
            document.getElementById('feedback-comment').value = '';
        } else {
            throw new Error(data.message || '提交反馈失败');
        }
    } catch (error) {
        console.error('提交反馈失败:', error);
        showNotification('反馈提交失败，请稍后重试', 'error');
    }
}

// 保存偏好设置
function savePreferences() {
    const defaultAlgorithm = document.getElementById('default-algorithm').value;
    const defaultCount = document.getElementById('default-count').value;
    const diversityValue = document.getElementById('diversity-slider').value;
    
    // 保存到localStorage
    localStorage.setItem('musicRec_preferences', JSON.stringify({
        defaultAlgorithm,
        defaultCount,
        diversityValue
    }));
    
    // 更新当前设置
    document.getElementById('algorithm-select').value = defaultAlgorithm;
    document.getElementById('rec-algorithm-select').value = defaultAlgorithm;
    document.getElementById('rec-count-select').value = defaultCount;
    currentAlgorithm = defaultAlgorithm;
    
    showNotification('偏好设置已保存', 'success');
}

// 更新统计信息
function updateStats() {
    // 这里可以从API获取实时统计，暂时使用固定值
    // 可以添加动画效果
    animateCount('user-count', 43355);
    animateCount('song-count', 16588);
    animateCount('rec-count', 500);
}

// 数字动画
function animateCount(elementId, target) {
    const element = document.getElementById(elementId);
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
    if (show) {
        overlay.classList.add('active');
    } else {
        overlay.classList.remove('active');
    }
}

// 显示通知
function showNotification(message, type = 'info') {
    // 创建通知元素
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
    
    // 添加样式
    const style = document.createElement('style');
    style.textContent = `
        .notification {
            position: fixed;
            top: 20px;
            right: 20px;
            background-color: white;
            color: #333;
            padding: 1rem 1.5rem;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            display: flex;
            align-items: center;
            justify-content: space-between;
            min-width: 300px;
            max-width: 400px;
            z-index: 9999;
            transform: translateX(150%);
            transition: transform 0.3s ease;
        }
        
        .notification.active {
            transform: translateX(0);
        }
        
        .notification-info {
            border-left: 4px solid #2196F3;
        }
        
        .notification-success {
            border-left: 4px solid #4CAF50;
        }
        
        .notification-warning {
            border-left: 4px solid #FF9800;
        }
        
        .notification-error {
            border-left: 4px solid #F44336;
        }
        
        .notification-content {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        
        .notification-close {
            background: none;
            border: none;
            font-size: 1.5rem;
            color: #999;
            cursor: pointer;
            line-height: 1;
            padding: 0;
            margin-left: 1rem;
        }
        
        .notification-close:hover {
            color: #333;
        }
    `;
    
    if (!document.querySelector('#notification-styles')) {
        style.id = 'notification-styles';
        document.head.appendChild(style);
    }
    
    // 显示通知
    setTimeout(() => {
        notification.classList.add('active');
    }, 10);
    
    // 关闭按钮事件
    notification.querySelector('.notification-close').addEventListener('click', () => {
        notification.classList.remove('active');
        setTimeout(() => {
            notification.remove();
        }, 300);
    });
    
    // 自动关闭
    setTimeout(() => {
        if (notification.parentNode) {
            notification.classList.remove('active');
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.remove();
                }
            }, 300);
        }
    }, 4000);
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