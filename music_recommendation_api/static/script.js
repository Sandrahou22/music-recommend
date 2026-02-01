// ========== 全局变量 ==========
let currentUser = "1001";
let currentAlgorithm = "hybrid";
let currentRecommendations = [];
let currentHotSongs = [];
let allGenres = [];
let isPlaying = false;
let currentSongIndex = 0;
let playerInterval;

// 播放追踪相关变量
let playStartTime = null;
let hasReportedListen = false;
let currentPlayingSongId = null;

// 音频播放器全局变量
let audioPlayer = null;
let currentAudioUrl = null;

let currentExploreOffset = 0;
const EXPLORE_PAGE_SIZE = 12;

// 播放列表管理
let playerPlaylist = [];      // 当前播放队列
let playerCurrentIndex = -1;  // 当前播放索引
let isDraggingProgress = false; // 是否正在拖动进度条

// 实时活动流（内存存储，页面刷新后清空）
let recentActivities = [];
const MAX_ACTIVITIES = 20; // 最多保留20条

// 播放列表UI状态
let isPlaylistVisible = false;
let abTestData = null;

let audioContext = null;
let analyser = null;
let visualizerInterval = null;

// 如果喜欢这个方案，使用7分类：
const DISPLAY_GENRES = ['流行', '摇滚', '民谣', '电子', '说唱', '金属', '其他'];

const REVERSE_MAP = {
    '流行': ['华语流行', '欧美流行', '日本流行', 'Pop', 'K-Pop'],
    '摇滚': ['Rock', 'Punk', '摇滚'],
    '民谣': ['Folk', '民谣', 'Country'], // 独立出来
    '电子': ['Electronic', '电子'],
    '说唱': ['Rap', '说唱'],
    '金属': ['Metal'],
    '其他': ['Jazz', 'Blues', 'Latin', 'New Age', 'World', 'Reggae', 'RnB', '翻唱', '现场', '影视原声']
};

// API配置
const API_BASE_URL = "http://127.0.0.1:5000/api/v1";
const ENDPOINTS = {
    recommend: (userId, algorithm, count) => 
        `${API_BASE_URL}/recommend/${userId}?algorithm=${algorithm}&n=${count}`,
    diverse: (userId, count) => 
        `${API_BASE_URL}/recommend/${userId}/diverse?n=${count}`,
    hotSongs: (tier) => 
        `${API_BASE_URL}/songs/hot?tier=${tier}`,
    songDetail: (songId) => 
        `${API_BASE_URL}/songs/${songId}`,
    userProfile: (userId) => 
        `${API_BASE_URL}/users/${userId}/profile`,
    userHistory: (userId) => 
        `${API_BASE_URL}/users/${userId}/history?n=20`,
    feedback: `${API_BASE_URL}/feedback`,
    behavior: `${API_BASE_URL}/behavior`,
    saveRecommendations: `${API_BASE_URL}/recommendations/save`,
    recommendationStatus: `${API_BASE_URL}/recommendations/status`,
    register: `${API_BASE_URL}/users/register`,
    health: `${API_BASE_URL}/health`
};

// DOM加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    initAudioPlayer();
    initApp();
    
    // 绑定音量滑块事件
    const volumeSlider = document.getElementById('volume-slider');
    if (volumeSlider) {
        volumeSlider.addEventListener('input', updateVolume);
        // 初始化音量
        updateVolume();
    }
    
    // 注册模态框事件
    const registerForm = document.getElementById('register-form');
    if (registerForm) {
        registerForm.addEventListener('submit', handleRegister);
    }
});

// ========== 初始化应用 ==========
function initApp() {
    checkApiHealth();
    setupEventListeners();
    
    // 关键：先加载热门歌曲，完成后立即加载流派标签
    loadHotSongs('all').then(() => {
        console.log('热门歌曲加载完成，准备加载流派...');
        loadGenres();  // 必须在这里调用，确保容器已存在
    });
    
    updateStats();
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

// ========== 音频播放器初始化 ==========
function initAudioPlayer() {
    if (!audioPlayer) {
        audioPlayer = new Audio();
        audioPlayer.crossOrigin = "anonymous";
        initVisualizer(); // 初始化可视化
        // 播放结束自动下一首
        audioPlayer.addEventListener('ended', () => {
            playNext();
        });
        
        // 音频加载错误处理 - 优化版本
        audioPlayer.addEventListener('error', (e) => {
            console.error('音频加载错误:', e);
            isPlaying = false;
            updatePlayButton();
            
            const error = audioPlayer.error;
            let msg = '音频加载失败';
            
            if (error) {
                switch(error.code) {
                    case 1: // MEDIA_ERR_ABORTED
                        msg = '音频加载被中断';
                        break;
                    case 2: // MEDIA_ERR_NETWORK
                        msg = '网络错误，无法加载音频';
                        break;
                    case 3: // MEDIA_ERR_DECODE
                        msg = '音频解码错误，文件可能损坏';
                        break;
                    case 4: // MEDIA_ERR_SRC_NOT_SUPPORTED
                        msg = '音频文件不存在或格式不支持(404)';
                        break;
                }
            }
            
            showNotification(msg, 'error');
            
            // 自动跳过到下一首（如果有）
            setTimeout(() => {
                if (playerPlaylist.length > 0) {
                    playNext();
                }
            }, 2000);
        });
        
    }
}

function initVisualizer() {
    try {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        analyser = audioContext.createAnalyser();
        analyser.fftSize = 64; // 较小值以获得平滑效果
        analyser.smoothingTimeConstant = 0.8;
        
        const source = audioContext.createMediaElementSource(audioPlayer);
        source.connect(analyser);
        analyser.connect(audioContext.destination);
        
        // 开始可视化循环
        updateVisualizers();
        
    } catch (e) {
        console.warn('音频可视化初始化失败:', e);
    }
}

function updateVisualizers() {
    const visualizer = document.getElementById('realtime-visualizer');
    if (!analyser || !visualizer) {
        requestAnimationFrame(updateVisualizers);
        return;
    }
    
    const bufferLength = analyser.frequencyBinCount;
    const dataArray = new Uint8Array(bufferLength);
    analyser.getByteFrequencyData(dataArray);
    
    const bars = visualizer.querySelectorAll('.bar');
    const barCount = bars.length;
    
    // 将频谱数据映射到8个柱状图
    for (let i = 0; i < barCount; i++) {
        // 采样频谱数据（取平均值使效果更平滑）
        const dataIndex = Math.floor((i / barCount) * (bufferLength / 2));
        const value = dataArray[dataIndex] || 0;
        
        // 映射到高度 (20px - 180px)
        const height = Math.max(20, (value / 255) * 180);
        bars[i].style.height = `${height}px`;
        
        // 根据音量调整透明度（无阴影，用透明度做层次感）
        const opacity = 0.3 + (value / 255) * 0.7;
        bars[i].style.opacity = opacity;
    }
    
    // 检查是否在播放
    if (audioPlayer && !audioPlayer.paused) {
        visualizer.classList.add('playing');
    } else {
        visualizer.classList.remove('playing');
    }
    
    requestAnimationFrame(updateVisualizers);
}

// 初始化进度条拖动
function initProgressBarDrag() {
    const progressBar = document.querySelector('.progress-bar');
    if (!progressBar) return;
    
    progressBar.addEventListener('mousedown', function(e) {
        isDraggingProgress = true;
        updateProgressFromMouse(e, progressBar);
    });
    
    document.addEventListener('mousemove', function(e) {
        if (isDraggingProgress && progressBar) {
            updateProgressFromMouse(e, progressBar);
        }
    });
    
    document.addEventListener('mouseup', function() {
        if (isDraggingProgress) {
            isDraggingProgress = false;
        }
    });
}

function updateProgressFromMouse(e, progressBar) {
    const rect = progressBar.getBoundingClientRect();
    const x = Math.max(0, Math.min(e.clientX - rect.left, rect.width));
    const percentage = x / rect.width;
    
    document.querySelector('.progress-fill').style.width = `${percentage * 100}%`;
    
    // 实时跳转音频位置
    if (audioPlayer && audioPlayer.duration) {
        audioPlayer.currentTime = percentage * audioPlayer.duration;
    }
}

// ========== 事件监听器设置 ==========
function setupEventListeners() {
    // 导航栏切换
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('href').substring(1);
            switchSection(targetId);
            
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
    // script.js 中 setupEventListeners 函数修改
    document.addEventListener('click', function(e) {
        // 改为：只有点击详情按钮才显示详情页
        if (e.target.closest('.detail-btn')) {
            e.stopPropagation();
            const songCard = e.target.closest('.song-card');
            if (songCard) {
                const songId = songCard.dataset.songId;
                showSongDetail(songId);
                
                // 记录点击查看
                if (currentRecommendations.some(r => r.song_id === songId)) {
                    updateRecommendationStatus(songId, 'click');
                }
            }
        }
        
        // 点击播放按钮
        if (e.target.closest('.play-song-btn')) {
            e.stopPropagation();
            const songCard = e.target.closest('.song-card');
            if (songCard) {
                const songId = songCard.dataset.songId;
                playSong(songId);
            }
        }
        
        // 点击反馈按钮（上方的喜欢按钮）
        if (e.target.closest('.feedback-btn')) {
            e.stopPropagation();
            const songCard = e.target.closest('.song-card');
            if (songCard) {
                const songId = songCard.dataset.songId;
                showFeedbackModal(songId);
            }
        }
    });
    
    // 播放器控制
    document.getElementById('play-btn').addEventListener('click', togglePlayback);
    document.getElementById('prev-btn').addEventListener('click', playPrevious);
    document.getElementById('next-btn').addEventListener('click', playNext);
    
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
    
    // 模态框按钮事件
    document.getElementById('play-now-btn')?.addEventListener('click', function() {
        const songId = this.dataset.songId || currentPlayingSongId;
        if (songId) {
            playSong(songId);
            document.getElementById('song-modal').classList.remove('active');
            showNotification('开始播放', 'success');
        }
    });

    document.getElementById('add-to-playlist-btn')?.addEventListener('click', function() {
        const songId = document.getElementById('play-now-btn').dataset.songId;
        if (songId) {
            addToPlaylist(songId);
        }
    });

    document.getElementById('similar-songs-btn')?.addEventListener('click', function() {
        const songId = document.getElementById('play-now-btn').dataset.songId;
        if (songId) {
            showNotification('正在查找相似歌曲...', 'info');
            document.getElementById('song-modal').classList.remove('active');
            loadSimilarSongsGraph(songId);
        }
    });
    
    // 键盘快捷键
    document.addEventListener('keydown', (e) => {
        // 输入框内不触发
        if (e.target.matches('input, textarea, select')) return;
        
        switch(e.code) {
            case 'Space':
                e.preventDefault();
                togglePlayback();
                break;
            case 'ArrowRight':
                if (e.shiftKey) playNext();
                else if (audioPlayer) audioPlayer.currentTime += 5;
                break;
            case 'ArrowLeft':
                if (e.shiftKey) playPrevious();
                else if (audioPlayer) audioPlayer.currentTime -= 5;
                break;
            case 'ArrowUp':
                e.preventDefault();
                const volUp = document.getElementById('volume-slider');
                if (volUp) {
                    volUp.value = Math.min(100, parseInt(volUp.value) + 5);
                    updateVolume();
                }
                break;
            case 'ArrowDown':
                e.preventDefault();
                const volDown = document.getElementById('volume-slider');
                if (volDown) {
                    volDown.value = Math.max(0, parseInt(volDown.value) - 5);
                    updateVolume();
                }
                break;
            case 'KeyM':
                if (audioPlayer) {
                    audioPlayer.muted = !audioPlayer.muted;
                    showNotification(audioPlayer.muted ? '已静音' : '已取消静音', 'info');
                }
                break;
            case 'KeyF':
                if (currentPlayingSongId) {
                    submitFeedback(currentPlayingSongId, 'like', currentAlgorithm);
                }
                break;
            case 'Escape':
                document.querySelectorAll('.modal').forEach(modal => {
                    modal.classList.remove('active');
                });
                break;
        }
    });
}

// ========== 播放列表功能 ==========
function togglePlaylist() {
    const panel = document.getElementById('playlist-panel');
    if (!panel) return;
    
    isPlaylistVisible = !isPlaylistVisible;
    if (isPlaylistVisible) {
        panel.classList.add('active');
        renderPlaylist();
    } else {
        panel.classList.remove('active');
    }
}

function renderPlaylist() {
    // 更新标题
    const headerCount = document.getElementById('playlist-header-count');
    if (headerCount) headerCount.textContent = `(${playerPlaylist.length}首)`;
    
    const body = document.getElementById('playlist-body');
    const countEl = document.getElementById('playlist-count');
    
    if (!body) return;
    
    // 更新数量角标
    if (countEl) countEl.textContent = playerPlaylist.length;
    
    if (playerPlaylist.length === 0) {
        body.innerHTML = `
            <div class="playlist-empty">
                <i class="fas fa-music"></i>
                <p>播放列表为空</p>
                <span>点击推荐歌曲添加到列表</span>
            </div>
        `;
        return;
    }
    
    // 获取歌曲详情
    const songs = [];
    for (const songId of playerPlaylist) {
        let song = currentRecommendations.find(r => r.song_id === songId) || 
                   currentHotSongs.find(s => s.song_id === songId);
        
        if (!song) {
            if (window.tempSongStore && window.tempSongStore[songId]) {
                song = window.tempSongStore[songId];
            } else {
                song = { song_id: songId, song_name: '加载中...', artists: '未知' };
            }
        }
        songs.push(song);
    }
    
    body.innerHTML = songs.map((song, index) => `
        <div class="playlist-item ${index === playerCurrentIndex ? 'playing' : ''}" 
             onclick="playSongAtIndex(${index})">
            <div class="playlist-number">${index + 1}</div>
            <div class="playlist-info">
                <div class="playlist-title">${song.song_name}</div>
                <div class="playlist-artist">${song.artists}</div>
            </div>
            <div class="playlist-actions">
                ${index === playerCurrentIndex ? '<i class="fas fa-volume-up"></i>' : ''}
                <button onclick="event.stopPropagation(); removeFromPlaylist(${index})" 
                        class="btn-icon-small" title="移除">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        </div>
    `).join('');
}

function playSongAtIndex(index) {
    if (index < 0 || index >= playerPlaylist.length) return;
    playerCurrentIndex = index;
    playSong(playerPlaylist[index]);
    renderPlaylist();
}

function removeFromPlaylist(index) {
    playerPlaylist.splice(index, 1);
    
    if (index === playerCurrentIndex) {
        if (audioPlayer) audioPlayer.pause();
        isPlaying = false;
        updatePlayButton();
        playerCurrentIndex = -1;
    } else if (index < playerCurrentIndex) {
        playerCurrentIndex--;
    }
    
    renderPlaylist();
    showNotification('已从播放列表移除', 'info');
}

function addToPlaylist(songId) {
    if (playerPlaylist.includes(songId)) {
        showNotification('该歌曲已在播放列表中', 'info');
        return;
    }
    
    playerPlaylist.push(songId);
    showNotification('已添加到播放列表', 'success');
    
    // 更新角标
    const countEl = document.getElementById('playlist-count');
    if (countEl) countEl.textContent = playerPlaylist.length;
    
    if (!isPlaying && playerPlaylist.length === 1) {
        playSong(songId);
    }
}

// ========== 活动记录功能 ==========
function addActivity(type, text, icon = 'fa-music', color = '#4361ee') {
    const activity = {
        type: type,
        text: text,
        icon: icon,
        color: color,
        time: new Date().toISOString(),
        id: Date.now() + Math.random()
    };
    
    recentActivities.unshift(activity);
    
    if (recentActivities.length > MAX_ACTIVITIES) {
        recentActivities.pop();
    }
    
    const container = document.getElementById('activity-feed');
    if (container && document.getElementById('profile').classList.contains('active')) {
        renderActivities();
    }
}

function renderActivities() {
    const container = document.getElementById('activity-feed');
    if (!container) return;
    
    const formatTime = (timestamp) => {
        if (!timestamp) return '刚刚';
        const now = new Date();
        const date = new Date(timestamp);
        const diff = (now - date) / 1000;
        
        if (diff < 60) return '刚刚';
        if (diff < 3600) return `${Math.floor(diff / 60)}分钟前`;
        if (diff < 86400) return `${Math.floor(diff / 3600)}小时前`;
        return date.toLocaleDateString();
    };
    
    if (recentActivities.length === 0) {
        container.innerHTML = '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">暂无活动记录</p>';
        return;
    }
    
    container.innerHTML = recentActivities.map(act => `
        <div class="activity-item" data-id="${act.id || ''}">
            <div class="activity-icon" style="background-color: ${act.color}20; color: ${act.color};">
                <i class="fas ${act.icon}"></i>
            </div>
            <div class="activity-content">
                <p>${act.text}</p>
                <span class="activity-time">${formatTime(act.time)}</span>
            </div>
        </div>
    `).join('');
}

// ========== 页面切换 ==========
function switchSection(sectionId) {
    document.querySelectorAll('.section').forEach(section => {
        section.classList.remove('active');
    });
    
    const targetSection = document.getElementById(sectionId);
    if (targetSection) {
        targetSection.classList.add('active');
    }
    
    if (sectionId === 'recommendations' && currentUser) {
        if (currentRecommendations.length === 0) {
            getRecommendations();
        } else {
            displayRecommendations(currentRecommendations);
        }
    }
    
    if (sectionId === 'explore') {
        loadExploreContent();
    }
    
    if (sectionId === 'profile' && currentUser) {
        loadUserProfile(currentUser);
        renderActivities();
        loadRecentActivity(currentUser);
    }
}

// ========== 主题切换 ==========
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

// ========== 获取推荐主函数 ==========
async function getRecommendations() {
    const userIdInput = document.getElementById('user-id-input');
    currentUser = userIdInput.value.trim();
    
    if (!currentUser) {
        showNotification('请输入用户ID', 'warning');
        return;
    }
    
    const algorithm = document.getElementById('rec-algorithm-select').value;
    const count = document.getElementById('rec-count-select').value;
    
    showLoading(true);
    
    try {
        let response;
        if (algorithm === 'diverse') {
            response = await fetch(ENDPOINTS.diverse(currentUser, count));
        } else {
            response = await fetch(ENDPOINTS.recommend(currentUser, algorithm, count));
        }
        
        const data = await response.json();
        
        if (data.success) {
            currentRecommendations = data.data.recommendations;
            
            // 记录生成推荐活动
            if (currentRecommendations.length > 0) {
                addActivity(
                    'recommend',
                    `生成了${currentRecommendations.length}个个性化推荐`,
                    'fa-magic',
                    '#4CAF50'
                );
            }
            
            displayRecommendations(currentRecommendations);
            
            await saveRecommendations(currentUser, currentRecommendations, algorithm);
            
            document.getElementById('current-user-id').textContent = currentUser;
            document.getElementById('current-algorithm').textContent = getAlgorithmName(algorithm);
            document.getElementById('current-count').textContent = count;
            
            await loadUserProfile(currentUser);
            await loadUserHistory(currentUser);
            
            for (let i = 0; i < Math.min(3, currentRecommendations.length); i++) {
                await updateRecommendationStatus(currentRecommendations[i].song_id, 'view');
            }
            
            showNotification(`成功生成${currentRecommendations.length}条推荐`, 'success');
            switchSection('recommendations');
            
            document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
            document.querySelector('a[href="#recommendations"]').classList.add('active');
        } else {
            throw new Error(data.message || '获取推荐失败');
        }
    } catch (error) {
        console.error('获取推荐失败:', error);
        showNotification(`获取推荐失败: ${error.message}`, 'error');
        displayMockRecommendations();
    } finally {
        showLoading(false);
    }
}

// 保存推荐结果
async function saveRecommendations(userId, recommendations, algorithm) {
    try {
        const payload = {
            user_id: userId,
            recommendations: recommendations.map((rec, idx) => ({
                song_id: rec.song_id,
                score: rec.score,
                algorithm: algorithm === 'diverse' ? 'mmr' : algorithm,
                rank_position: idx + 1
            }))
        };
        
        const response = await fetch(ENDPOINTS.saveRecommendations, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload)
        });
        
        if (!response.ok) {
            console.warn('保存推荐结果失败:', await response.text());
        }
    } catch (error) {
        console.error('保存推荐结果失败:', error);
    }
}

// 更新推荐状态
async function updateRecommendationStatus(songId, action) {
    if (!currentUser) return;
    
    try {
        const response = await fetch(ENDPOINTS.recommendationStatus, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                user_id: currentUser,
                song_id: songId,
                action: action,
                timestamp: new Date().toISOString()
            })
        });
        
        if (!response.ok) {
            console.warn('更新推荐状态失败:', await response.text());
        }
    } catch (error) {
        console.error('更新推荐状态失败:', error);
    }
}

// script.js 中 displayRecommendations 函数修改
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
    
    const withAudioCount = recommendations.filter(r => r.has_audio).length;
    
    container.innerHTML = recommendations.map((song, index) => {
        const playButton = song.has_audio ? `
            <button class="action-btn play-song-btn">
                <i class="fas fa-play"></i> 播放
            </button>
        ` : `
            <button class="action-btn disabled" title="暂无音频，仅可查看详情" 
                    onclick="event.stopPropagation(); showNotification('该歌曲暂无音频文件，仅可查看详情', 'info')">
                <i class="fas fa-eye"></i> 预览
            </button>
        `;
        
        const audioBadge = song.has_audio ? 
            '<span class="audio-badge" title="可播放"><i class="fas fa-volume-up"></i></span>' : 
            '<span class="no-audio-badge">预览</span>';
        
        /* 删除这部分：start
        const feedbackButtons = `
            <div class="explicit-feedback">
                <button class="feedback-btn-thumb" 
                        onclick="event.stopPropagation(); submitFeedback('${song.song_id}', 'like', '${song.cold_start ? 'cold' : currentAlgorithm}')"
                        title="推荐准确">
                    <i class="fas fa-thumbs-up"></i>
                </button>
                <button class="feedback-btn-thumb" 
                        onclick="event.stopPropagation(); submitFeedback('${song.song_id}', 'dislike', '${song.cold_start ? 'cold' : currentAlgorithm}')"
                        title="不感兴趣">
                    <i class="fas fa-thumbs-down"></i>
                </button>
            </div>
        `;
        删除这部分：end */
        
        return `
        <div class="song-card ${!song.has_audio ? 'no-audio' : ''}" data-song-id="${song.song_id}">
            <div class="song-card-header">
                <i class="fas fa-music"></i>
                <span>推荐 #${index + 1}</span>
                ${song.cold_start ? '<span class="cold-badge">冷启动</span>' : ''}
                ${audioBadge}
            </div>
            <div class="song-card-body">
                <h3 class="song-title" title="${song.song_name}">${song.song_name}</h3>
                <p class="song-artist">${song.artists}</p>
                <div class="song-meta">
                    <span class="genre-tag">${song.genre || '未知流派'}</span>
                    <span class="popularity-badge">${song.popularity || 50}</span>
                </div>
                <div class="song-score">
                    <div class="score-bar">
                        <div class="score-fill" style="width: ${Math.min(song.score * 100, 100)}%"></div>
                    </div>
                    <span class="score-text">匹配度: ${(song.score * 100).toFixed(1)}%</span>
                </div>
                <div class="song-actions">
                    ${playButton}
                    <button class="action-btn feedback-btn">
                        <i class="fas fa-thumbs-up"></i> 喜欢
                    </button>
                    <button class="action-btn detail-btn">
                        <i class="fas fa-info-circle"></i> 详情
                    </button>
                </div>
                <!-- 删除了 explicit-feedback 部分的插入 -->
            </div>
        </div>
    `}).join('');
    
    // 将推荐设为播放列表
    playerPlaylist = recommendations.map(r => r.song_id);
    playerCurrentIndex = -1;
    
    const statsEl = document.getElementById('current-count');
    if (statsEl) {
        statsEl.innerHTML = `${recommendations.length} <small style="color: #4CAF50;">(${withAudioCount}首可播)</small>`;
    }
}

async function submitFeedback(songId, feedback, algorithm) {
    if (!currentUser) {
        showNotification('请先登录', 'warning');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/recommendations/feedback`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                user_id: currentUser,
                song_id: songId,
                feedback: feedback,
                algorithm: algorithm
            })
        });
        
        const data = await response.json();
        if (data.success) {
            showNotification(feedback === 'like' ? '感谢您的认可！' : '我们会改进推荐', 'success');
            const card = document.querySelector(`[data-song-id="${songId}"]`);
            if (card) {
                const thumbs = card.querySelectorAll('.feedback-btn-thumb');
                thumbs.forEach(btn => btn.classList.remove('active'));
                if (feedback === 'like') thumbs[0].classList.add('active');
                if (feedback === 'dislike') thumbs[1].classList.add('active');
            }
        }
    } catch (e) {
        console.error('反馈提交失败:', e);
    }
}

// 模拟推荐数据
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
        }
    ];
    
    currentRecommendations = mockRecommendations;
    displayRecommendations(mockRecommendations);
    
    document.getElementById('current-user-id').textContent = currentUser;
    document.getElementById('current-algorithm').textContent = getAlgorithmName(currentAlgorithm);
    document.getElementById('current-count').textContent = "5";
    
    showNotification('使用模拟数据展示（API连接失败）', 'warning');
}

// ========== 热门歌曲功能 ==========
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

function displayMockHotSongs() {
    const mockHotSongs = [
        {
            song_id: "hot_001",
            song_name: "孤勇者",
            artists: "陈奕迅",
            genre: "流行",
            popularity: 95
        }
    ];
    
    displayHotSongs(mockHotSongs);
}

// ========== 用户历史记录 ==========
async function loadUserHistory(userId) {
    try {
        const response = await fetch(ENDPOINTS.userHistory(userId));
        const data = await response.json();
        
        if (data.success && data.data && data.data.length > 0) {
            displayHistory(data.data);
        } else {
            const container = document.getElementById('history-container');
            container.innerHTML = `
                <div class="empty-state-small">
                    <i class="fas fa-music"></i>
                    <p>暂无历史记录</p>
                </div>
            `;
        }
    } catch (error) {
        console.error('加载历史记录失败:', error);
        const container = document.getElementById('history-container');
        container.innerHTML = `
            <div class="empty-state-small">
                <i class="fas fa-exclamation-circle"></i>
                <p>加载历史记录失败</p>
            </div>
        `;
    }
}

function displayHistory(history) {
    const container = document.getElementById('history-container');
    
    if (!history || history.length === 0) {
        container.innerHTML = `
            <div class="empty-state-small">
                <i class="fas fa-music"></i>
                <p>暂无历史记录</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = history.map((item, index) => {
        const timeStr = item.timestamp ? 
            new Date(item.timestamp).toLocaleDateString() : '未知时间';
        
        const types = item.interaction_types ? item.interaction_types.split(',') : [];
        const hasLike = types.includes('like');
        const hasCollect = types.includes('collect');
        
        return `
        <div class="song-item" data-song-id="${item.song_id}">
            <div class="song-icon">
                <i class="fas fa-music"></i>
            </div>
            <div class="song-info">
                <h4>${item.song_name}</h4>
                <p>${item.artists} • ${item.genre || '未知流派'}</p>
            </div>
            <div class="song-badges">
                ${hasLike ? '<span class="badge-like" title="已喜欢"><i class="fas fa-heart"></i></span>' : ''}
                ${hasCollect ? '<span class="badge-collect" title="已收藏"><i class="fas fa-star"></i></span>' : ''}
            </div>
            <span class="song-time">${timeStr}</span>
            <button class="play-btn" data-song-id="${item.song_id}" data-song-name="${item.song_name}" data-artists="${item.artists}">
                <i class="fas fa-play"></i>
            </button>
        </div>
    `}).join('');
    
    container.querySelectorAll('.play-btn').forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.stopPropagation();
            const songId = this.getAttribute('data-song-id');
            const songName = this.getAttribute('data-song-name');
            const artists = this.getAttribute('data-artists');
            
            if (!window.tempSongStore) window.tempSongStore = {};
            window.tempSongStore[songId] = { song_id: songId, song_name: songName, artists: artists };
            
            playSong(songId);
        });
    });
    
    container.querySelectorAll('.song-item').forEach(item => {
        item.addEventListener('click', function(e) {
            if (e.target.closest('.play-btn')) return;
            const songId = this.getAttribute('data-song-id');
            showSongDetail(songId);
        });
    });
}

// ========== 最近活动功能 ==========
async function loadRecentActivity(userId) {
    try {
        const historyResponse = await fetch(ENDPOINTS.userHistory(userId));
        const historyData = await historyResponse.json();
        
        if (historyData.success && historyData.data) {
            const historyActivities = historyData.data.slice(0, 5).map(item => {
                const types = item.interaction_types ? item.interaction_types.split(',') : ['play'];
                let icon = 'fa-play';
                let color = '#4361ee';
                let text = `播放了歌曲《${item.song_name}》`;
                
                if (types.includes('collect')) {
                    icon = 'fa-star';
                    color = '#f72585';
                    text = `收藏了歌曲《${item.song_name}》`;
                } else if (types.includes('like')) {
                    icon = 'fa-heart';
                    color = '#f72585';
                    text = `喜欢了歌曲《${item.song_name}》`;
                }
                
                return {
                    type: 'history',
                    text: text,
                    icon: icon,
                    color: color,
                    time: item.timestamp,
                    id: 'hist_' + item.song_id
                };
            });
            
            const existingIds = new Set(recentActivities.map(a => a.text));
            historyActivities.forEach(act => {
                if (!existingIds.has(act.text)) {
                    recentActivities.push(act);
                }
            });
        }
    } catch (error) {
        console.error('加载历史活动失败:', error);
    }
    
    renderActivities();
}

// ========== 用户画像加载与显示 ==========
async function loadUserProfile(userId) {
    try {
        console.log(`开始加载用户画像: ${userId}`);
        const response = await fetch(ENDPOINTS.userProfile(userId));
        const data = await response.json();
        
        if (data.success) {
            console.log('获取到用户数据:', data.data);
            displayUserProfile(data.data);
        } else {
            throw new Error(data.message || '获取用户画像失败');
        }
    } catch (error) {
        console.error('获取用户画像失败:', error);
        showNotification('获取用户画像失败', 'error');
    }
}

function displayUserProfile(profile) {
    document.getElementById('profile-user-id').textContent = profile.user_id || '-';
    document.getElementById('profile-user-id-large').textContent = profile.user_id || '-';
    
    const nSongs = parseInt(profile.n_songs) || 0;
    document.getElementById('total-listens').textContent = nSongs;
    
    let totalInteractions = parseInt(profile.total_interactions);
    if (isNaN(totalInteractions) || totalInteractions === 0) {
        totalInteractions = nSongs;
    }
    
    const interactionsEl = document.getElementById('total-interactions');
    if (interactionsEl) {
        interactionsEl.textContent = totalInteractions;
    }
    
    document.getElementById('profile-song-count').textContent = nSongs > 0 ? nSongs : '-';
    
    const popPref = parseFloat(profile.avg_popularity) || 50;
    let popPrefText = '中等偏好';
    if (popPref < 30) popPrefText = '探索型用户（偏爱冷门）';
    else if (popPref < 40) popPrefText = '偏爱冷门';
    else if (popPref > 80) popPrefText = '极度偏好热门流行';
    else if (popPref > 70) popPrefText = '偏好热门流行';
    else if (popPref > 60) popPrefText = '偏好流行';
    else if (popPref < 50) popPrefText = '偏好小众';
    
    document.getElementById('profile-pop-pref').textContent = popPrefText;
    document.getElementById('avg-popularity').textContent = Math.round(popPref);
    
    const activityEl = document.getElementById('activity-level');
    if (activityEl) {
        const level = profile.activity_level || '普通用户';
        activityEl.textContent = level;
        activityEl.style.color = '';
        if (level.includes('高')) activityEl.style.color = '#4CAF50';
        else if (level.includes('中')) activityEl.style.color = '#FF9800';
        else if (level.includes('低')) activityEl.style.color = '#999';
        else if (level.includes('新')) activityEl.style.color = '#2196F3';
    }
    
    const diversityEl = document.getElementById('diversity-ratio');
    if (diversityEl) {
        const diversity = parseFloat(profile.diversity_ratio);
        if (!isNaN(diversity) && diversity > 0) {
            diversityEl.textContent = (diversity * 100).toFixed(1) + '%';
        } else {
            const genres = profile.top_genres || [];
            const estimatedDiversity = genres.length > 0 ? Math.min(genres.length * 20, 100) : 0;
            diversityEl.textContent = estimatedDiversity + '%';
        }
    }
    
    const genresContainer = document.getElementById('profile-genres');
    let genres = profile.top_genres || [];
    
    if (typeof genres === 'string') {
        genres = genres.split(/[,，、]/).map(g => g.trim()).filter(g => g);
    }
    
    if (genres.length > 0) {
        if (document.getElementById('fav-genre')) {
            document.getElementById('fav-genre').textContent = genres[0];
        }
        genresContainer.innerHTML = genres.map(genre => 
            `<span class="genre-tag">${genre}</span>`
        ).join('');
    } else {
        if (document.getElementById('fav-genre')) {
            document.getElementById('fav-genre').textContent = '-';
        }
        genresContainer.innerHTML = '<span class="no-data">暂无数据</span>';
    }
    
    const popPrefEl = document.getElementById('profile-pop-pref');
    if (popPrefEl && (profile.is_cold_start || nSongs < 5)) {
        if (nSongs < 50 && !popPrefEl.innerHTML.includes('新用户')) {
            popPrefEl.innerHTML += ' <span class="cold-badge">新用户</span>';
        }
    }
}

// ========== 播放控制 ==========

async function playSong(songId) {
    // 如果正在播放同一首歌，则暂停
    if (currentPlayingSongId === songId && audioPlayer && !audioPlayer.paused) {
        audioPlayer.pause();
        isPlaying = false;
        updatePlayButton();
        return;
    }

    // 清理旧播放器（如果存在）
    if (audioPlayer) {
        audioPlayer.pause();
        audioPlayer.src = '';
        // 清除所有旧事件（防止内存泄漏和状态混乱）
        audioPlayer.oncanplay = null;
        audioPlayer.onended = null;
        audioPlayer.onerror = null;
        audioPlayer.ontimeupdate = null;
        audioPlayer.onloadedmetadata = null;
    }

    // 【关键】创建全新的音频实例
    audioPlayer = new Audio();
    audioPlayer.crossOrigin = "anonymous";
    
    const audioUrl = `${API_BASE_URL}/songs/${songId}/audio`;
    audioPlayer.src = audioUrl;
    currentPlayingSongId = songId;
    
    // 获取歌曲信息
    let song = currentRecommendations.find(s => s.song_id === songId) || 
               currentHotSongs.find(s => s.song_id === songId) ||
               (window.tempSongStore && window.tempSongStore[songId]);
    
    if (!song) {
        try {
            const response = await fetch(ENDPOINTS.songDetail(songId));
            const data = await response.json();
            if (data.success) song = data.data;
        } catch(e) {
            console.warn('获取歌曲详情失败', e);
        }
    }

    // 更新UI显示
    document.getElementById('now-playing-title').textContent = song?.song_name || '未知歌曲';
    document.getElementById('now-playing-artist').textContent = song?.artists || '未知艺术家';
    
    // 【关键】绑定进度条更新事件
    audioPlayer.addEventListener('timeupdate', () => {
        if (!isDraggingProgress && audioPlayer.duration) {
            const progress = (audioPlayer.currentTime / audioPlayer.duration) * 100;
            document.querySelector('.progress-fill').style.width = `${progress}%`;
            document.getElementById('current-time').textContent = formatTime(Math.floor(audioPlayer.currentTime));
            if (audioPlayer.duration && audioPlayer.duration !== Infinity && !isNaN(audioPlayer.duration)) {
                document.getElementById('total-time').textContent = formatTime(Math.floor(audioPlayer.duration));
            }
        }
    });
    
    // 绑定元数据加载（获取总时长）
    audioPlayer.addEventListener('loadedmetadata', () => {
        if (audioPlayer.duration && audioPlayer.duration !== Infinity) {
            document.getElementById('total-time').textContent = formatTime(Math.floor(audioPlayer.duration));
        }
    });
    
    // 绑定播放结束（自动下一首）
    audioPlayer.addEventListener('ended', () => {
        isPlaying = false;
        updatePlayButton();
        playNext();
    });
    
    // 绑定错误处理
    audioPlayer.addEventListener('error', (e) => {
        console.error('音频播放错误:', e);
        showNotification('音频播放失败，文件可能不存在', 'error');
        isPlaying = false;
        updatePlayButton();
    });

    // 开始加载音频
    audioPlayer.load();
    
    // 等待可以播放
    try {
        await new Promise((resolve, reject) => {
            audioPlayer.oncanplay = () => resolve();
            audioPlayer.onerror = () => reject(new Error('音频加载失败'));
            // 设置10秒超时（避免卡住）
            setTimeout(() => reject(new Error('加载超时')), 10000);
        });
        
        // 开始播放
        await audioPlayer.play();
        isPlaying = true;
        updatePlayButton();
        
        // 管理播放列表
        if (!playerPlaylist.includes(songId)) {
            playerPlaylist.push(songId);
            playerCurrentIndex = playerPlaylist.length - 1;
        } else {
            playerCurrentIndex = playerPlaylist.indexOf(songId);
        }
        
        // 记录行为
        recordBehavior(songId, 'play', 0.5);
        
    } catch (error) {
        console.error('播放失败:', error);
        isPlaying = false;
        updatePlayButton();
        // 不显示通知（避免频繁报错），只控制台记录
    }
}

async function recordBehavior(songId, behaviorType, weight) {
    if (!currentUser) return;
    
    try {
        const response = await fetch(ENDPOINTS.behavior, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                user_id: currentUser,
                song_id: songId,
                behavior_type: behaviorType,
                weight: weight,
                timestamp: new Date().toISOString()
            })
        });
        
        if (!response.ok) {
            console.warn('行为记录失败:', await response.text());
        }
    } catch (error) {
        console.error('记录行为失败:', error);
    }
}

function togglePlayback() {
    if (!audioPlayer) {
        showNotification('请先选择一首歌曲', 'warning');
        return;
    }

    if (isPlaying) {
        audioPlayer.pause();
        isPlaying = false;
    } else {
        if ((!audioPlayer.src || audioPlayer.src === window.location.href) && playerPlaylist.length > 0) {
            playSongAtIndex(0);
            return;
        }
        
        audioPlayer.play().catch(e => {
            console.error('播放失败:', e);
            showNotification('播放失败，请检查音频文件', 'error');
            isPlaying = false;
            updatePlayButton();
            return;
        });
        isPlaying = true;
    }
    updatePlayButton();
}

function playNext() {
    if (playerPlaylist.length === 0) {
        showNotification('播放列表为空', 'info');
        return;
    }
    
    let nextIndex = playerCurrentIndex + 1;
    if (nextIndex >= playerPlaylist.length) {
        nextIndex = 0;
    }
    
    playSong(playerPlaylist[nextIndex]);
}

function playPrevious() {
    if (playerPlaylist.length === 0) {
        showNotification('播放列表为空', 'info');
        return;
    }
    
    let prevIndex = playerCurrentIndex - 1;
    if (prevIndex < 0) {
        prevIndex = playerPlaylist.length - 1;
    }
    
    playSong(playerPlaylist[prevIndex]);
}

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

function formatTime(seconds) {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs < 10 ? '0' : ''}${secs}`;
}

function updateVolume() {
    const volumeSlider = document.getElementById('volume-slider');
    if (!volumeSlider) return;
    
    const volume = parseInt(volumeSlider.value);
    if (audioPlayer) {
        audioPlayer.volume = volume / 100;
        audioPlayer.muted = (volume === 0);
    }
    
    const volumeIcon = document.querySelector('.player-volume i');
    if (volumeIcon) {
        if (volume === 0) {
            volumeIcon.className = 'fas fa-volume-mute';
        } else if (volume < 50) {
            volumeIcon.className = 'fas fa-volume-down';
        } else {
            volumeIcon.className = 'fas fa-volume-up';
        }
    }
}

// ========== 歌曲详情与相似歌曲 ==========
async function showSongDetail(songId) {
    try {
        const response = await fetch(ENDPOINTS.songDetail(songId));
        const data = await response.json();
        
        if (data.success) {
            const song = data.data;
            
            let explanation = null;
            if (currentUser) {
                try {
                    const recResponse = await fetch(
                        `${API_BASE_URL}/explain/${currentUser}/${songId}`
                    );
                    if (recResponse.ok) {
                        const recData = await recResponse.json();
                        if (recData.success && recData.data) {
                            explanation = recData.data.explanation || recData.data
                        }
                    }
                } catch (e) {
                    console.log('获取个性化解释失败:', e);
                }
            }
            
            displaySongDetail(song, explanation);
        } else {
            throw new Error(data.message || '获取歌曲详情失败');
        }
    } catch (error) {
        console.error('获取歌曲详情失败:', error);
        showNotification('获取歌曲详情失败', 'error');
    }
}

async function loadSimilarSongsGraph(songId) {
    try {
        const response = await fetch(`${API_BASE_URL}/songs/${songId}/similar`);
        const data = await response.json();
        
        if (data.success && data.data.similar_songs.length > 0) {
            const container = document.getElementById('similar-songs-container') || createSimilarContainer();
            renderSimilarityGraph(data.data.source_song, data.data.similar_songs);
        }
    } catch (e) {
        console.error('加载相似歌曲失败:', e);
    }
}

function createSimilarContainer() {
    const modalBody = document.querySelector('#song-modal .modal-body');
    const section = document.createElement('div');
    section.className = 'similar-songs-section';
    section.innerHTML = `
        <h4>相似歌曲探索</h4>
        <div id="similar-songs-graph" class="similar-graph"></div>
    `;
    modalBody.appendChild(section);
    return document.getElementById('similar-songs-graph');
}

function renderSimilarityGraph(centerId, similarSongs) {
    const container = document.getElementById('similar-songs-graph');
    if (!container) return;
    
    container.innerHTML = `
        <div class="center-node">
            <i class="fas fa-compact-disc"></i>
            <span>当前歌曲</span>
        </div>
        <div class="similar-edges">
            ${similarSongs.map((song, idx) => `
                <div class="similar-node" style="--delay: ${idx * 0.1}s" 
                     onclick="playSong('${song.song_id}')">
                    <div class="node-line" style="opacity: ${song.similarity_score}"></div>
                    <div class="node-content">
                        <div class="node-title">${song.song_name}</div>
                        <div class="node-score">相似度: ${(song.similarity_score * 100).toFixed(0)}%</div>
                    </div>
                </div>
            `).join('')}
        </div>
    `;
    
    const modal = document.getElementById('song-modal');
    if (modal) modal.classList.add('active');
}

function displaySongDetail(song, explanation = null) {
    document.getElementById('modal-song-title').textContent = '歌曲详情';
    document.getElementById('detail-song-name').textContent = song.song_name || '未知歌曲';
    document.getElementById('detail-artists').textContent = song.artists || '未知艺术家';
    document.getElementById('detail-genre').textContent = song.genre || '未知流派';
    document.getElementById('detail-popularity').textContent = `流行度: ${song.popularity || 50}`;
    
    const features = song.audio_features || {};
    document.getElementById('danceability-value').textContent = (features.danceability || 0.5).toFixed(2);
    document.getElementById('danceability-bar').style.width = `${(features.danceability || 0.5) * 100}%`;
    
    document.getElementById('energy-value').textContent = (features.energy || 0.5).toFixed(2);
    document.getElementById('energy-bar').style.width = `${(features.energy || 0.5) * 100}%`;
    
    document.getElementById('valence-value').textContent = (features.valence || 0.5).toFixed(2);
    document.getElementById('valence-bar').style.width = `${(features.valence || 0.5) * 100}%`;
    
    document.getElementById('tempo-value').textContent = `${Math.round(features.tempo || 120)} BPM`;
    
    let reasonText = '基于您的音乐偏好推荐';
    
    if (explanation) {
        if (typeof explanation === 'string') {
            reasonText = explanation;
        } else if (typeof explanation === 'object') {
            if (explanation.main_reason && typeof explanation.main_reason === 'string') {
                reasonText = explanation.main_reason;
            } else if (explanation.explanation && typeof explanation.explanation === 'string') {
                reasonText = explanation.explanation;
            }
            if (explanation.details && Array.isArray(explanation.details)) {
                reasonText += '<br><small style="color: #666; line-height: 1.4; display: block; margin-top: 8px;">' + explanation.details.join('，') + '</small>';
            }
        }
    } else {
        const reasons = [
            "根据您的听歌历史和偏好推荐",
            "与您最近收听歌曲风格相似",
            "与您偏好流派高度匹配",
            "当前热门歌曲，符合大众口味",
            "小众精品，发现独特音乐"
        ];
        reasonText = reasons[Math.floor(Math.random() * reasons.length)];
    }
    
    document.getElementById('recommendation-reason-text').innerHTML = String(reasonText);
    
    const playBtn = document.getElementById('play-now-btn');
    playBtn.dataset.songId = song.song_id;
    playBtn.onclick = function() {
        playSong(song.song_id);
        document.getElementById('song-modal').classList.remove('active');
    };
    
    document.getElementById('song-modal').classList.add('active');
}

// ========== 反馈模态框 ==========
async function showFeedbackModal(songId) {
    document.getElementById('feedback-modal').classList.add('active');
    
    document.querySelectorAll('.feedback-btn').forEach(btn => {
        btn.onclick = function() {
            const action = this.dataset.action;
            submitFeedback(songId, action);
        };
    });
}

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

// ========== 其他功能 ==========
function savePreferences() {
    const defaultAlgorithm = document.getElementById('default-algorithm').value;
    const defaultCount = document.getElementById('default-count').value;
    const diversityValue = document.getElementById('diversity-slider').value;
    
    localStorage.setItem('musicRec_preferences', JSON.stringify({
        defaultAlgorithm,
        defaultCount,
        diversityValue
    }));
    
    document.getElementById('algorithm-select').value = defaultAlgorithm;
    document.getElementById('rec-algorithm-select').value = defaultAlgorithm;
    document.getElementById('rec-count-select').value = defaultCount;
    currentAlgorithm = defaultAlgorithm;
    
    showNotification('偏好设置已保存', 'success');
}

const GENRE_STATS = {
    '流行': 3302,    // 1519+1108+193+482
    '摇滚': 9410,    // 9260+150
    '电子': 1255,
    '金属': 794,
    '说唱': 331,
    '其他': 1506     // 208+182+204+912
};

// 渲染时带数量
function loadGenres() {
    const container = document.getElementById('genre-tags');
    if (!container) {
        console.error('[错误] 找不到容器 #genre-tags');
        return;
    }
    
    // 生成 HTML（确保有 onclick 内联事件作为兜底）
    let html = `<button class="genre-tag-btn active" onclick="handleGenreClick('all', this)" data-genre="all">全部</button>`;
    
    DISPLAY_GENRES.forEach(genre => {
        html += `<button class="genre-tag-btn" onclick="handleGenreClick('${genre}', this)" data-genre="${genre}">${genre}</button>`;
    });
    
    container.innerHTML = html;
    console.log('[成功] 流派标签已渲染，共', DISPLAY_GENRES.length + 1, '个');
}

function handleGenreClick(genre, btnElement) {
    console.log('[点击] 流派:', genre);
    
    // 更新样式
    document.querySelectorAll('.genre-tag-btn').forEach(b => b.classList.remove('active'));
    btnElement.classList.add('active');
    
    // 执行筛选
    filterSongsByGenre(genre);
}

// 【新增】绑定流派标签点击事件
function bindGenreEvents() {
    const container = document.getElementById('genre-tags');
    if (!container) return;
    
    container.addEventListener('click', function(e) {
        // 使用事件委托，点击按钮时触发
        if (e.target.classList.contains('genre-tag-btn')) {
            e.preventDefault();
            e.stopPropagation();
            
            const genre = e.target.dataset.genre;
            console.log('点击流派:', genre); // 调试用
            
            // UI状态更新：移除其他active，添加当前active
            document.querySelectorAll('.genre-tag-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            e.target.classList.add('active');
            
            // 调用筛选函数
            if (typeof filterSongsByGenre === 'function') {
                filterSongsByGenre(genre);
            } else {
                console.error('filterSongsByGenre 函数未定义');
            }
        }
    });
    
    console.log('流派标签事件绑定完成');
}

// ================== 修复版流派筛选（支持自动从后端加载） ==================

async function filterSongsByGenre(genre) {
    const container = document.getElementById('explore-container');
    
    if (!container) return;
    
    // 显示加载中
    container.innerHTML = '<div class="empty-state"><i class="fas fa-spinner fa-spin"></i><p>正在筛选...</p></div>';
    
    // 全部 -> 显示当前已加载的热门歌曲
    if (genre === 'all') {
        if (currentHotSongs && currentHotSongs.length > 0) {
            displayExploreSongs(currentHotSongs);
        } else {
            await loadHotSongs('all');
        }
        return;
    }
    
    // 获取该分类对应的原始流派列表
    const sourceGenres = REVERSE_MAP[genre];
    if (!sourceGenres) {
        console.error('[错误] 未知流派:', genre);
        container.innerHTML = '<div class="empty-state"><p>未知分类</p></div>';
        return;
    }
    
    console.log(`[筛选] ${genre} -> 查找:`, sourceGenres);
    
    // 【第一步】先在前端已加载的数据中过滤
    const filtered = currentHotSongs.filter(song => {
        if (!song || !song.genre) return false;
        return sourceGenres.includes(song.genre);
    });
    
    console.log(`[前端数据] 找到: ${filtered.length} 首`);
    
    // 【第二步】如果前端没有，自动从后端数据库加载
    if (filtered.length === 0) {
        console.log('[提示] 前端数据中没有，正在从数据库加载...');
        await loadSongsByGenreFromBackend(sourceGenres, genre);
        return;
    }
    
    // 前端有数据，直接显示
    displayExploreSongs(filtered);
    showNotification(`${genre}: 找到 ${filtered.length} 首歌曲`, 'success');
}

// 【新增】从后端数据库加载指定流派的歌曲
async function loadSongsByGenreFromBackend(sourceGenres, displayGenre) {
    const container = document.getElementById('explore-container');
    container.innerHTML = '<div class="empty-state"><i class="fas fa-spinner fa-spin"></i><p>正在从数据库加载...</p></div>';
    
    try {
        // 【方案A】如果有改造后的 /songs/by-genre 接口（推荐）
        // 将多个原始流派用逗号分隔传给后端
        const genreParam = encodeURIComponent(sourceGenres.join(','));
        const response = await fetch(`${API_BASE_URL}/songs/by-genre?genre=${genreParam}&limit=50&offset=0`);
        const data = await response.json();
        
        if (data.success && data.data.songs && data.data.songs.length > 0) {
            displayExploreSongs(data.data.songs);
            showNotification(`${displayGenre}: 从数据库加载 ${data.data.songs.length} 首歌曲`, 'success');
        } else {
            throw new Error('API返回空数据');
        }
        
    } catch (error) {
        console.warn('API加载失败，尝试备用方案:', error);
        
        // 【方案B】备用：加载大量热门歌曲再过滤（如果后端API还没改）
        try {
            container.innerHTML = '<div class="empty-state"><i class="fas fa-spinner fa-spin"></i><p>尝试加载更多数据...</p></div>';
            
            // 加载100首热门歌曲
            const response = await fetch(`${API_BASE_URL}/songs/hot?tier=all&n=100`);
            const data = await response.json();
            
            if (data.success && data.data.songs) {
                // 更新当前数据
                currentHotSongs = data.data.songs;
                
                // 再次过滤
                const filtered = currentHotSongs.filter(song => {
                    if (!song || !song.genre) return false;
                    return sourceGenres.includes(song.genre);
                });
                
                if (filtered.length > 0) {
                    displayExploreSongs(filtered);
                    showNotification(`${displayGenre}: 找到 ${filtered.length} 首`, 'success');
                } else {
                    // 100首中还没有，说明这些歌确实冷门，直接显示全部100首并提示
                    displayExploreSongs(currentHotSongs);
                    showNotification(`提示：${displayGenre}歌曲较冷门，当前展示全部热门歌曲`, 'info');
                }
            }
        } catch (err) {
            container.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-exclamation-circle"></i>
                    <p>加载失败，请检查网络连接</p>
                    <button class="btn btn-primary" onclick="filterSongsByGenre('${displayGenre}')" style="margin-top:1rem">
                        重试
                    </button>
                </div>
            `;
        }
    }
}

async function loadAllSongsByGenre(genre, offset = 0, limit = 20) {
    const container = document.getElementById('explore-container');
    
    if (offset === 0) {
        container.innerHTML = `
            <div class="empty-state" style="grid-column: 1 / -1; padding: 3rem;">
                <i class="fas fa-spinner fa-spin" style="font-size: 2rem; color: var(--primary-color);"></i>
                <p style="margin-top: 1rem;">正在从数据库加载「${genre}」歌曲...</p>
            </div>
        `;
    } else {
        const existingLoadMore = container.querySelector('.dynamic-load-more');
        if (existingLoadMore) existingLoadMore.remove();
    }
    
    try {
        const response = await fetch(`${API_BASE_URL}/songs/by-genre?genre=${encodeURIComponent(genre)}&limit=${limit}&offset=${offset}&fuzzy=1`);
        const data = await response.json();
        
        if (data.success && data.data.songs.length > 0) {
            if (offset === 0) {
                displayExploreSongs(data.data.songs);
                currentHotSongs = [...data.data.songs];
            } else {
                const newCards = data.data.songs.map(song => createSongCard(song, genre)).join('');
                container.insertAdjacentHTML('beforeend', newCards);
                currentHotSongs = [...currentHotSongs, ...data.data.songs];
            }
            
            const loaded = offset + data.data.songs.length;
            const total = data.data.total;
            
            showNotification(`已加载 ${loaded}/${total} 首${genre}歌曲`, 'success');
            
            if (data.data.has_more) {
                const loadMoreDiv = document.createElement('div');
                loadMoreDiv.className = 'load-more-container dynamic-load-more';
                loadMoreDiv.style.gridColumn = '1 / -1';
                loadMoreDiv.innerHTML = `
                    <button class="btn btn-secondary" onclick="loadAllSongsByGenre('${genre}', ${offset + limit}, ${limit})">
                        <i class="fas fa-plus"></i> 加载更多
                    </button>
                `;
                container.appendChild(loadMoreDiv);
            }
        } else {
            if (offset === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <i class="fas fa-music"></i>
                        <p>数据库中暂无「${genre}」流派的歌曲</p>
                    </div>
                `;
            } else {
                showNotification('已加载全部歌曲', 'info');
            }
        }
    } catch (error) {
        console.error('加载失败:', error);
        if (offset === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-exclamation-circle"></i>
                    <p>加载失败: ${error.message}</p>
                </div>
            `;
        }
    }
}

function createSongCard(song, genreLabel) {
    return `
        <div class="song-card explore-card" data-song-id="${song.song_id}">
            <div class="song-card-header" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                <i class="fas fa-compact-disc"></i>
                <span>${genreLabel || song.genre || '音乐'}</span>
            </div>
            <div class="song-card-body">
                <h3 class="song-title" title="${song.song_name || '未知歌曲'}">${song.song_name || '未知歌曲'}</h3>
                <p class="song-artist" title="${song.artists || '未知艺术家'}">${song.artists || '未知艺术家'}</p>
                <div class="song-meta">
                    <span class="genre-tag">${song.genre || '未知流派'}</span>
                    <span class="popularity-badge">${song.popularity || 50}</span>
                </div>
                <div class="song-actions">
                    <button class="action-btn play-song-btn" onclick="event.stopPropagation(); playSong('${song.song_id}')">
                        <i class="fas fa-play"></i> 播放
                    </button>
                    <button class="action-btn" onclick="event.stopPropagation(); showSongDetail('${song.song_id}')">
                        <i class="fas fa-info"></i> 详情
                    </button>
                </div>
            </div>
        </div>
    `;
}

async function loadExploreContent() {
    const container = document.getElementById('explore-container');
    const genericLoadMore = document.getElementById('explore-load-more');
    
    if (genericLoadMore) genericLoadMore.style.display = 'flex';
    
    container.innerHTML = `
        <div class="empty-state" style="grid-column: 1 / -1;">
            <i class="fas fa-spinner fa-spin"></i>
            <p>正在加载音乐库...</p>
        </div>
    `;
    
    try {
        if (currentHotSongs.length === 0) {
            await loadHotSongs('all');
        }
        
        let exploreSongs = [...currentHotSongs];
        
        currentRecommendations.forEach(song => {
            if (!exploreSongs.find(s => s.song_id === song.song_id)) {
                exploreSongs.push(song);
            }
        });
        
        if (exploreSongs.length > 0) {
            displayExploreSongs(exploreSongs);
            loadGenres();
        } else {
            throw new Error('无歌曲数据');
        }
    } catch (error) {
        console.error('加载探索内容失败:', error);
        container.innerHTML = `
            <div class="empty-state" style="grid-column: 1 / -1;">
                <i class="fas fa-exclamation-circle"></i>
                <p>加载失败，请先获取推荐或刷新页面</p>
                <button class="btn btn-primary" onclick="getRecommendations()" style="margin-top: 1rem;">
                    获取推荐
                </button>
            </div>
        `;
    }
}

function displayExploreSongs(songs) {
    const container = document.getElementById('explore-container');
    
    if (!songs || songs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-music"></i>
                <p>暂无歌曲</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = songs.map(song => `
        <div class="song-card explore-card" data-song-id="${song.song_id}">
            <div class="song-card-header" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                <i class="fas fa-compact-disc"></i>
                <span>发现</span>
            </div>
            <div class="song-card-body">
                <h3 class="song-title">${song.song_name || '未知歌曲'}</h3>
                <p class="song-artist">${song.artists || '未知艺术家'}</p>
                <div class="song-meta">
                    <span class="genre-tag">${song.genre || '未知流派'}</span>
                    <span class="popularity-badge">${song.popularity || 50}</span>
                </div>
                <div class="song-actions">
                    <button class="action-btn play-song-btn" onclick="event.stopPropagation(); playSong('${song.song_id}')">
                        <i class="fas fa-play"></i> 播放
                    </button>
                    <button class="action-btn" onclick="event.stopPropagation(); showSongDetail('${song.song_id}')">
                        <i class="fas fa-info"></i> 详情
                    </button>
                </div>
            </div>
        </div>
    `).join('');
}

function updateStats() {
    animateCount('user-count', 43355);
    animateCount('song-count', 16588);
    animateCount('rec-count', 500);
}

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

function getAlgorithmName(algorithm) {
    const algorithmNames = {
        'hybrid': '混合推荐',
        'usercf': '用户协同过滤',
        'cf': '物品协同过滤',
        'content': '内容推荐',
        'mf': '矩阵分解',
        'cold': '冷启动推荐',
        'diverse': '多样性推荐',
        'auto': '自动选择'
    };
    return algorithmNames[algorithm] || algorithm;
}

function showLoading(show) {
    const overlay = document.getElementById('loading-overlay');
    if (show) {
        overlay.classList.add('active');
    } else {
        overlay.classList.remove('active');
    }
}

// ========== 用户注册功能 ==========
function showRegisterModal() {
    generateUserId();
    document.getElementById('register-modal').classList.add('active');
}

function generateUserId() {
    const timestamp = Date.now().toString().slice(-6);
    const random = Math.floor(Math.random() * 1000).toString().padStart(3, '0');
    const userIdInput = document.getElementById('reg-user-id');
    if (userIdInput) {
        userIdInput.value = `new_${timestamp}${random}`;
    }
}

async function handleRegister(e) {
    e.preventDefault();
    
    const userData = {
        user_id: document.getElementById('reg-user-id').value,
        nickname: document.getElementById('reg-nickname').value,
        gender: document.getElementById('reg-gender').value || null,
        age: document.getElementById('reg-age').value || null,
        province: document.getElementById('reg-province').value || '',
        city: document.getElementById('reg-city').value || '',
        source: 'internal'
    };
    
    if (!userData.nickname) {
        showNotification('请输入昵称', 'warning');
        return;
    }
    
    console.log('开始注册:', userData);
    
    try {
        const response = await fetch(ENDPOINTS.register, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(userData)
        });
        
        const data = await response.json();
        
        if (data.success) {
            showNotification('注册成功！', 'success');
            document.getElementById('register-modal').classList.remove('active');
            
            const userIdInput = document.getElementById('user-id-input');
            if (userIdInput) {
                userIdInput.value = userData.user_id;
            }
            currentUser = userData.user_id;
            
            document.getElementById('algorithm-select').value = 'cold';
            document.getElementById('rec-algorithm-select').value = 'cold';
            currentAlgorithm = 'cold';
            
            getRecommendations();
        } else {
            throw new Error(data.message || '注册失败');
        }
    } catch (error) {
        console.error('注册失败:', error);
        showNotification('注册失败: ' + error.message, 'error');
    }
}

// ========== 通知系统 ==========
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.innerHTML = `
        <div class="notification-content">
            <i class="fas fa-${getNotificationIcon(type)}"></i>
            <span>${message}</span>
        </div>
        <button class="notification-close">&times;</button>
    `;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.classList.add('active');
    }, 10);
    
    notification.querySelector('.notification-close').addEventListener('click', () => {
        notification.classList.remove('active');
        setTimeout(() => notification.remove(), 300);
    });
    
    setTimeout(() => {
        if (notification.parentNode) {
            notification.classList.remove('active');
            setTimeout(() => {
                if (notification.parentNode) notification.remove();
            }, 300);
        }
    }, 4000);
}

function getNotificationIcon(type) {
    const icons = {
        'info': 'info-circle',
        'success': 'check-circle',
        'warning': 'exclamation-triangle',
        'error': 'times-circle'
    };
    return icons[type] || 'info-circle';
}

// ========== 加载更多功能 ==========
function initLoadMore() {
    const loadMoreBtn = document.getElementById('load-more-btn');
    if (loadMoreBtn) {
        loadMoreBtn.addEventListener('click', () => {
            const activeGenreBtn = document.querySelector('.genre-tag-btn.active');
            const currentGenre = activeGenreBtn ? activeGenreBtn.dataset.genre : 'all';
            
            if (currentGenre === 'all') {
                loadMoreSongs();
            } else {
                loadAllSongsByGenre(currentGenre, currentHotSongs.length, 20);
            }
        });
    }
}

async function loadMoreSongs() {
    const btn = document.getElementById('load-more-btn');
    const originalText = btn.innerHTML;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加载中...';
    btn.disabled = true;
    
    try {
        showNotification('已加载更多歌曲', 'success');
    } catch (error) {
        showNotification('加载失败', 'error');
    } finally {
        btn.innerHTML = originalText;
        btn.disabled = false;
    }
}

// ========== A/B测试功能 ==========
async function toggleABTest() {
    const container = document.getElementById('ab-test-container');
    const normalContainer = document.getElementById('recommendations-container');
    const recTitle = document.getElementById('rec-title');
    const btnText = document.getElementById('ab-test-btn-text');
    
    if (container.style.display === 'none' || !container.style.display) {
        showLoading(true);
        try {
            const response = await fetch(`${API_BASE_URL}/recommend/${currentUser}/compare?n=6`);
            const data = await response.json();
            
            if (data.success) {
                abTestData = data.data;
                renderABTestResults(data.data);
                
                container.style.display = 'block';
                normalContainer.style.display = 'none';
                recTitle.innerHTML = '<i class="fas fa-flask"></i> A/B测试对比';
                btnText.textContent = '退出对比';
                showNotification('已进入A/B测试模式，请选择更好的推荐列表', 'info');
            }
        } catch (e) {
            showNotification('加载A/B测试失败', 'error');
        } finally {
            showLoading(false);
        }
    } else {
        // 退出AB测试
        container.style.display = 'none';
        normalContainer.style.display = 'grid';
        recTitle.textContent = '个性化推荐';
        btnText.textContent = 'A/B测试对比';
        displayRecommendations(currentRecommendations);
    }
}


function renderABTestResults(data) {
    const colA = document.getElementById('ab-column-a');
    const colB = document.getElementById('ab-column-b');
    
    // 更新算法描述
    document.querySelector('.algorithm-a .algo-desc').textContent = data.group_a.description;
    document.querySelector('.algorithm-b .algo-desc').textContent = data.group_b.description;
    
    if (colA) colA.innerHTML = data.group_a.recommendations.map(song => createABSongCard(song)).join('');
    if (colB) colB.innerHTML = data.group_b.recommendations.map(song => createABSongCard(song)).join('');
}

function createABSongCard(song) {
    return `
        <div class="song-card" data-song-id="${song.song_id}">
            <div class="song-card-body">
                <div style="flex: 1;">
                    <h3 class="song-title" title="${song.song_name}">${song.song_name}</h3>
                    <p class="song-artist">${song.artists}</p>
                </div>
                <div class="song-score" style="width: 60px; text-align: right;">
                    <span style="font-weight: 600; color: var(--primary-color);">${(song.score * 100).toFixed(0)}%</span>
                </div>
                <div class="song-actions" style="margin-left: 10px;">
                    <button class="action-btn play-song-btn" onclick="event.stopPropagation(); playSong('${song.song_id}')" style="padding: 0.4rem 0.8rem;">
                        <i class="fas fa-play"></i>
                    </button>
                </div>
            </div>
        </div>
    `;
}

async function voteABTest(choice) {
    if (!abTestData || !currentUser) return;
    
    const algoNames = {
        'a': abTestData.group_a.algorithm,
        'b': abTestData.group_b.algorithm,
        'tie': 'tie'
    };
    
    try {
        await fetch(ENDPOINTS.behavior, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                user_id: currentUser,
                song_id: 'ab_test_vote',
                behavior_type: 'vote',
                weight: 1.0,
                context: {
                    winner: choice,
                    algorithm_a: abTestData.group_a.algorithm,
                    algorithm_b: abTestData.group_b.algorithm,
                    timestamp: new Date().toISOString()
                }
            })
        });
        
        showNotification(`已记录您的选择！感谢参与评测`, 'success');
        
        // 延迟后退出
        setTimeout(() => {
            toggleABTest();
        }, 1500);
        
    } catch (e) {
        console.error('投票失败:', e);
    }
}