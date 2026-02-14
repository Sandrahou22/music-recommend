// ========== 全局变量 ==========
let currentUser = "1001";
let currentAlgorithm = "hybrid";
let currentRecommendations = [];
let currentHotSongs = [];
let allGenres = [];
let isPlaying = false;
let currentSongIndex = 0;
let playerInterval;

// ========== 评论功能全局变量 ==========
let currentSongIdForComments = null;
let currentCommentsData = null;
let commentsSortBy = 'time';
let commentsCurrentPage = 1;
let commentsTotalPages = 1;

let isInitialized = false;
let genreEventsBound = false;
let loadMoreInitialized = false;

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

const REVERSE_MAP = {
    '流行': ['华语流行', '欧美流行', '日本流行', 'Pop', 'K-Pop'],
    '摇滚': ['Rock', 'Punk', '摇滚'],
    '电子': ['Electronic', '电子'],
    '金属': ['Metal'],
    '说唱': ['Rap', '说唱'], 
    '民谣': ['Folk', '民谣', 'Country'],
    '其他': ['RnB', 'Jazz', 'Blues', 'Latin', 'New Age', 'World', 'Reggae', '翻唱', '现场', '影视原声']
};

const DISPLAY_GENRES = ['流行', '摇滚', '电子', '金属', '说唱', '民谣', '其他'];

// 情感图标映射
const SENTIMENT_ICONS = {
    'sentiment-positive': 'fa-smile',
    'sentiment-neutral': 'fa-meh',
    'sentiment-negative': 'fa-frown'
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

async function checkSongAudioStatus(songId) {
    try {
        // 优先从当前数据中查找
        const song = currentRecommendations.find(r => r.song_id === songId) || 
                    currentHotSongs.find(s => s.song_id === songId);
        
        if (song && song.has_audio !== undefined) {
            return song.has_audio;
        }
        
        // 调用后端API检查
        const response = await fetch(`${API_BASE_URL}/songs/${songId}/audio/status`);
        if (!response.ok) {
            return false;
        }
        
        const data = await response.json();
        if (data.success) {
            return data.data.has_audio || false;
        }
        
        return false;
    } catch (error) {
        console.error('检查音频状态失败:', error);
        return true; // 出错时默认返回true，避免阻塞播放
    }
}

// ========== 初始化应用 ==========
function initApp() {
    if (isInitialized) {
        console.log('[初始化] 已初始化，跳过');
        return;
    }
    
    console.log('[初始化] 开始');
    
    checkApiHealth();
    setupEventListeners();
    
    // 先加载热门歌曲
    loadHotSongs('all').then(() => {
        console.log('[初始化] 热门歌曲加载完成');
        
        // 加载流派标签
        loadGenres();
        
        // 初始化搜索
        initSearch();
        
        // 初始化加载更多
        initLoadMore();
        
        // 更新统计
        updateStats();
    }).catch(error => {
        console.error('[初始化] 错误:', error);
        showNotification('部分功能初始化失败', 'error');
    });
    
    initTheme();
    initProgressBarDrag();
    
    isInitialized = true;
    console.log('[初始化] 完成');

    // 初始化评论字符计数
    initCommentCharCount();
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
            clearTimeout(timeoutId);
            console.error('[播放调试] 音频播放错误:', e);
            console.error('[播放调试] 错误代码:', audioPlayer.error?.code);
            console.error('[播放调试] 错误信息:', audioPlayer.error?.message);
            
            let msg = '音频加载失败';
            const error = audioPlayer.error;
            
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
            
            isPlaying = false;
            updatePlayButton();
            window.currentPlayingRequest = null;
            
            audioPlayer.addEventListener('error', (e) => {
                clearTimeout(timeoutId);
                console.error('[播放调试] 音频播放错误:', e);
                
                let msg = '音频加载失败';
                if (audioPlayer.error) {
                    switch(audioPlayer.error.code) {
                        case 1: msg = '音频加载被中止'; break;
                        case 2: msg = '网络错误，无法加载音频'; break;
                        case 3: msg = '音频解码错误'; break;
                        case 4: msg = '音频文件不存在'; break;
                    }
                }
                
                showNotification(msg, 'error');
                isPlaying = false;
                updatePlayButton();
                window.currentPlayingRequest = null;
                
                // 【关键修复】只显示通知，不修改歌曲状态
                // 这样UI就不会错误变化
            }, { once: true });
            
            // 【新增】更新播放列表中的状态
            updateSongCardAudioStatus(songId, false);
            
        }, { once: true });
        
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
    console.log('[事件监听] 开始设置');
    
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
    const algorithmSelect = document.getElementById('algorithm-select');
    if (algorithmSelect) {
        algorithmSelect.addEventListener('change', function() {
            currentAlgorithm = this.value;
        });
    }
    
    const recAlgorithmSelect = document.getElementById('rec-algorithm-select');
    if (recAlgorithmSelect) {
        recAlgorithmSelect.addEventListener('change', function() {
            currentAlgorithm = this.value;
        });
    }
    
    // 推荐数量选择
    const recCountSelect = document.getElementById('rec-count-select');
    if (recCountSelect) {
        recCountSelect.addEventListener('change', function() {
            console.log('推荐数量改为:', this.value);
        });
    }
    
    // 播放器控制
    const playBtn = document.getElementById('play-btn');
    if (playBtn) {
        playBtn.addEventListener('click', togglePlayback);
    }
    
    const prevBtn = document.getElementById('prev-btn');
    if (prevBtn) {
        prevBtn.addEventListener('click', playPrevious);
    }
    
    const nextBtn = document.getElementById('next-btn');
    if (nextBtn) {
        nextBtn.addEventListener('click', playNext);
    }
    
    // 音量控制
    const volumeSlider = document.getElementById('volume-slider');
    if (volumeSlider) {
        volumeSlider.addEventListener('input', updateVolume);
    }
    
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
    
    // 【关键修复】保存偏好设置按钮
    const savePrefBtn = document.getElementById('save-preferences');
    if (savePrefBtn) {
        console.log('[事件监听] 找到保存偏好按钮');
        savePrefBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            console.log('[保存偏好] 按钮被点击');
            savePreferences();
        });
    } else {
        console.error('[事件监听] 找不到保存偏好按钮');
    }
    
    // 多样性滑块
    const diversitySlider = document.getElementById('diversity-slider');
    if (diversitySlider) {
        diversitySlider.addEventListener('input', function() {
            const value = parseInt(this.value);
            const labels = ['低', '较低', '中等', '较高', '高'];
            const labelIndex = Math.floor(value / 2);
            const valueEl = document.getElementById('diversity-value');
            if (valueEl) {
                valueEl.textContent = labels[labelIndex] || '中等';
            }
        });
    }
    
    // 默认算法选择
    const defaultAlgorithmSelect = document.getElementById('default-algorithm');
    if (defaultAlgorithmSelect) {
        defaultAlgorithmSelect.addEventListener('change', function() {
            console.log('默认算法改为:', this.value);
        });
    }
    
    // 默认数量选择
    const defaultCountSelect = document.getElementById('default-count');
    if (defaultCountSelect) {
        defaultCountSelect.addEventListener('change', function() {
            console.log('默认数量改为:', this.value);
        });
    }
    
    // 模态框按钮事件
    const playNowBtn = document.getElementById('play-now-btn');
    if (playNowBtn) {
        playNowBtn.addEventListener('click', function() {
            const songId = this.dataset.songId || currentPlayingSongId;
            if (songId) {
                playSong(songId);
                const modal = document.getElementById('song-modal');
                if (modal) modal.classList.remove('active');
                showNotification('开始播放', 'success');
            }
        });
    }
    
    const addToPlaylistBtn = document.getElementById('add-to-playlist-btn');
    if (addToPlaylistBtn) {
        addToPlaylistBtn.addEventListener('click', function() {
            const songId = document.getElementById('play-now-btn')?.dataset.songId;
            if (songId) {
                addToPlaylist(songId);
            }
        });
    }
    
    const similarSongsBtn = document.getElementById('similar-songs-btn');
    if (similarSongsBtn) {
        similarSongsBtn.addEventListener('click', function() {
            const songId = document.getElementById('play-now-btn')?.dataset.songId;
            if (songId) {
                showNotification('正在查找相似歌曲...', 'info');
                const modal = document.getElementById('song-modal');
                if (modal) modal.classList.remove('active');
                loadSimilarSongsGraph(songId);
            }
        });
    }
    
    // A/B测试按钮
    const abTestBtn = document.querySelector('button[onclick="toggleABTest()"]');
    if (abTestBtn) {
        abTestBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            toggleABTest();
        });
    }
    
    // 注册表单提交
    const registerForm = document.getElementById('register-form');
    if (registerForm) {
        registerForm.addEventListener('submit', function(e) {
            e.preventDefault();
            handleRegister(e);
        });
    }
    
    // 用户ID生成按钮
    const generateUserIdBtn = document.querySelector('button[onclick="generateUserId()"]');
    if (generateUserIdBtn) {
        generateUserIdBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            generateUserId();
        });
    }
    
    // 注册模态框显示
    const registerModalBtn = document.querySelector('button[onclick="showRegisterModal()"]');
    if (registerModalBtn) {
        registerModalBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            showRegisterModal();
        });
    }
    
    // 事件委托：歌曲卡片点击事件
    document.addEventListener('click', function(e) {
        // 点击详情按钮
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
                const hasAudio = songCard.dataset.hasAudio === "true";
                
                if (!hasAudio) {
                    showNotification('该歌曲暂无音频文件，仅可查看详情', 'info');
                    return;
                }
                
                playSong(songId);
                
                // 播放按钮临时禁用，防止重复点击
                const playBtn = songCard.querySelector('.play-song-btn');
                if (playBtn && !playBtn.disabled) {
                    playBtn.disabled = true;
                    setTimeout(() => {
                        playBtn.disabled = false;
                    }, 2000);
                }
            }
        }
        
        // 点击反馈按钮（喜欢按钮）
        if (e.target.closest('.feedback-btn')) {
            e.stopPropagation();
            const songCard = e.target.closest('.song-card');
            if (songCard) {
                const songId = songCard.dataset.songId;
                showFeedbackModal(songId);
            }
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
    
    // 【新增】探索页面搜索功能
    const searchExecuteBtn = document.getElementById('search-btn-execute');
    if (searchExecuteBtn) {
        searchExecuteBtn.addEventListener('click', function() {
            const searchInput = document.getElementById('explore-search-input');
            if (searchInput && searchInput.value.trim()) {
                performSearch(searchInput.value.trim());
            } else {
                showNotification('请输入搜索内容', 'warning');
            }
        });
    }
    
    const searchInput = document.getElementById('explore-search-input');
    if (searchInput) {
        searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                const query = searchInput.value.trim();
                if (query) {
                    performSearch(query);
                }
            }
        });
    }
    
    const clearSearchBtn = document.getElementById('clear-search-btn');
    if (clearSearchBtn) {
        clearSearchBtn.addEventListener('click', clearSearch);
    }
    
    // 【新增】加载更多按钮
    const loadMoreBtn = document.getElementById('load-more-btn');
    if (loadMoreBtn) {
        loadMoreBtn.addEventListener('click', async () => {
            console.log('[点击] 加载更多');
            
            // 获取当前激活的流派
            const activeBtn = document.querySelector('.genre-tag-btn.active');
            const currentGenre = activeBtn ? activeBtn.dataset.genre : 'all';
            
            loadMoreBtn.disabled = true;
            loadMoreBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加载中...';
            
            try {
                if (currentGenre === 'all') {
                    await loadMoreHotSongs();
                } else {
                    await loadMoreByGenre(currentGenre);
                }
            } finally {
                loadMoreBtn.disabled = false;
                loadMoreBtn.innerHTML = '<i class="fas fa-plus"></i> 加载更多';
            }
        });
    }
    
    console.log('[事件监听] 设置完成');

    // 绑定流派标签点击事件
    setTimeout(() => {
        bindGenreEvents();
        initLoadMore();
    }, 1000); // 延迟1秒确保DOM加载完成

    // 评论相关事件
    initCommentCharCount();
    
    // 提交评论快捷键（Ctrl+Enter）
    const commentTextarea = document.getElementById('comment-textarea');
    if (commentTextarea) {
        commentTextarea.addEventListener('keydown', (e) => {
            if (e.ctrlKey && e.key === 'Enter') {
                e.preventDefault();
                submitComment();
            }
        });
    }
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
            
            // 新增：记录推荐生成行为
            recordBehavior('recommend_generate', 'generate_recommend', currentRecommendations.length);
            
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
    
    container.innerHTML = recommendations.map((song, index) => {
        // 【关键修复】使用原始has_audio状态，但提供一个后备值
        const hasAudio = song.has_audio !== false; // 默认true，除非明确为false
        
        const playButton = hasAudio ? `
            <button class="action-btn play-song-btn">
                <i class="fas fa-play"></i> 播放
            </button>
        ` : `
            <button class="action-btn disabled" title="暂无音频，仅可查看详情" 
                    onclick="event.stopPropagation(); showNotification('该歌曲暂无音频文件，仅可查看详情', 'info')">
                <i class="fas fa-eye"></i> 预览
            </button>
        `;
        
        const audioBadge = hasAudio ? 
            '<span class="audio-badge" title="可播放"><i class="fas fa-volume-up"></i></span>' : 
            '<span class="no-audio-badge">预览</span>';
        
        // 【重要】在data属性中保存原始状态
        return `
        <div class="song-card ${!hasAudio ? 'no-audio' : ''}" 
             data-song-id="${song.song_id}" 
             data-has-audio="${hasAudio}">
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
            </div>
        </div>
    `}).join('');
    
    // 【修复】只添加有音频的歌曲到播放列表
    playerPlaylist = recommendations
        .filter(song => song.has_audio !== false) // 只包含有音频的
        .map(r => r.song_id);
    playerCurrentIndex = -1;
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
    console.log(`[热门歌曲] 加载: ${tier}`);
    
    try {
        // 直接调用后端API
        const response = await fetch(`${API_BASE_URL}/songs/hot?tier=${tier}`);
        
        if (!response.ok) {
            console.error(`[热门歌曲] HTTP错误: ${response.status}`);
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            const songs = data.data.songs || [];
            
            // 确保每首歌都有has_audio字段
            songs.forEach(song => {
                if (song.has_audio === undefined) {
                    song.has_audio = true;
                }
            });
            
            console.log(`[热门歌曲] ${tier}: 加载 ${songs.length} 首歌曲`);
            
            // 根据tier更新不同的显示
            if (tier === 'all') {
                currentHotSongs = songs;
                displayHotSongs(songs);
            } else {
                // 对于其他分类，更新对应的显示区域
                displayHotSongs(songs);
            }
            
            return songs;
        } else {
            throw new Error(data.message || '获取热门歌曲失败');
        }
    } catch (error) {
        console.error('获取热门歌曲失败:', error);
        
        // 使用备用数据
        const mockSongs = [
            {
                song_id: `mock_${tier}_1`,
                song_name: `${tier}歌曲示例1`,
                artists: "示例艺术家",
                genre: tier === 'all' ? '流行' : tier,
                popularity: 80 + Math.floor(Math.random() * 20),
                has_audio: true
            },
            {
                song_id: `mock_${tier}_2`,
                song_name: `${tier}歌曲示例2`,
                artists: "示例艺术家",
                genre: tier === 'all' ? '摇滚' : tier,
                popularity: 70 + Math.floor(Math.random() * 20),
                has_audio: true
            }
        ];
        
        if (tier === 'all') {
            currentHotSongs = mockSongs;
        }
        
        displayHotSongs(mockSongs);
        showNotification(`热门${tier}歌曲加载失败，使用示例数据`, 'warning');
        
        return mockSongs;
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
        <div class="song-card" data-song-id="${song.song_id}" data-has-audio="${song.has_audio !== false}">
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
        const response = await fetch(`${API_BASE_URL}/users/${userId}/recent-activities?limit=20`);
        const data = await response.json();
        
        if (data.success && data.data.length > 0) {
            // 将后端活动转换为前端活动格式
            const backendActivities = data.data.map(act => {
                    let text = '';
                    let icon = 'fa-music';
                    let color = '#4361ee';
                    
                    switch(act.type) {
                        case 'play':
                            text = `播放了歌曲《${act.song_name || '未知歌曲'}》`;
                            icon = 'fa-play';
                            color = '#4361ee';
                            break;
                        case 'like_song':
                            text = `喜欢了歌曲《${act.song_name || '未知歌曲'}》`;
                            icon = 'fa-heart';
                            color = '#f72585';
                            break;
                        case 'comment':
                            text = `评论了歌曲《${act.song_name || '未知歌曲'}》：${act.content?.substring(0, 30)}${act.content?.length > 30 ? '...' : ''}`;
                            icon = 'fa-comment';
                            color = '#06d6a0';
                            break;
                        case 'like_comment':
                            text = `点赞了评论：${act.content?.substring(0, 30)}${act.content?.length > 30 ? '...' : ''}`;
                            icon = 'fa-thumbs-up';
                            color = '#f72585';
                            break;
                        case 'generate_recommend':
                            const count = act.extra_data || 1;
                            text = `生成了 ${count} 首推荐`;
                            icon = 'fa-magic';
                            color = '#7209b7';
                            break;
                        default:
                            text = `进行了操作`;
                    }
                    
                    return {
                        type: act.type,
                        text: text,
                        icon: icon,
                        color: color,
                        time: act.timestamp,
                        id: `backend_${act.type}_${act.timestamp}`
                    };
                });
            
            // 合并现有活动（前端手动添加的，如推荐生成、偏好保存等）
            const allActivities = [...backendActivities, ...recentActivities];
            
            // 按时间排序（最新的在前）
            allActivities.sort((a, b) => new Date(b.time) - new Date(a.time));
            
            // 去重（基于 type + time，但前端手动添加的活动有唯一 id）
            const seen = new Set();
            recentActivities = allActivities.filter(act => {
                const key = act.id || `${act.type}_${act.time}`;
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
            }).slice(0, MAX_ACTIVITIES);  // 限制最大数量
            
            renderActivities();
        }
    } catch (error) {
        console.error('加载活动失败:', error);
    }
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
    console.log('显示用户画像:', profile);
    
    // 1. 用户ID
    document.getElementById('profile-user-id').textContent = profile.user_id || '-';
    document.getElementById('profile-user-id-large').textContent = profile.user_id || '-';
    
    // 2. 听歌数量
    const nSongs = parseInt(profile.n_songs) || 0;
    document.getElementById('total-listens').textContent = nSongs;
    document.getElementById('profile-song-count').textContent = nSongs > 0 ? nSongs : '-';
    
    // 3. 交互次数
    let totalInteractions = parseInt(profile.total_interactions);
    if (isNaN(totalInteractions) || totalInteractions === 0) {
        totalInteractions = nSongs;
    }
    const interactionsEl = document.getElementById('total-interactions');
    if (interactionsEl) {
        interactionsEl.textContent = totalInteractions;
    }
    
    // 4. 流行度偏好
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
    
    // 5. 活跃等级
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
    
    // 6. 多样性
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
    
    // 7. 流派偏好
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
    
    // 8. 注册时间（使用created_at字段）
    const joinDateEl = document.getElementById('profile-join-date');
    if (joinDateEl) {
        if (profile.created_at) {
            try {
                const regDate = new Date(profile.created_at);
                joinDateEl.textContent = `注册时间: ${regDate.toLocaleDateString('zh-CN')} ${regDate.toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'})}`;
            } catch (e) {
                joinDateEl.textContent = `注册时间: ${profile.created_at}`;
            }
        } else if (profile.registered_at) {
            try {
                const regDate = new Date(profile.registered_at);
                joinDateEl.textContent = `注册时间: ${regDate.toLocaleDateString('zh-CN')} ${regDate.toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'})}`;
            } catch (e) {
                joinDateEl.textContent = `注册时间: ${profile.registered_at}`;
            }
        } else {
            // 根据用户ID估算
            if (profile.user_id && profile.user_id.startsWith('new_')) {
                joinDateEl.textContent = '注册时间: 今日';
            } else {
                joinDateEl.textContent = '注册时间: 2023-01-01';
            }
        }
    }
    
    // 9. 新用户标识
    const popPrefEl = document.getElementById('profile-pop-pref');
    if (popPrefEl && (profile.is_cold_start || nSongs < 5)) {
        if (nSongs < 50 && !popPrefEl.innerHTML.includes('新用户')) {
            popPrefEl.innerHTML += ' <span class="cold-badge">新用户</span>';
        }
    }
    
    console.log('用户画像显示完成');
}

// ========== 播放控制 ==========

// ========== 播放控制 ==========

async function playSong(songId) {
    console.log(`[播放调试] 开始播放歌曲: ${songId}`);
    
    // 【关键】先检查歌曲卡片状态
    const songCard = document.querySelector(`[data-song-id="${songId}"]`);
    if (!songCard) {
        console.log('[播放调试] 未找到歌曲卡片');
        return;
    }
    
    // 【关键】检查是否有音频（从data属性读取，不可修改）
    const hasAudio = songCard.dataset.hasAudio === "true";
    if (!hasAudio) {
        showNotification('该歌曲暂无音频文件，仅可预览', 'warning');
        return;
    }
    
    // 防抖：如果正在请求同一首歌曲，避免重复
    if (window.currentPlayingRequest && window.currentPlayingRequest === songId) {
        console.log(`[播放调试] 正在处理同一首歌曲的请求，跳过`);
        return;
    }
    
    window.currentPlayingRequest = songId;
    
    // 清理旧播放器（如果存在）
    if (audioPlayer) {
        try {
            audioPlayer.pause();
            audioPlayer.src = '';
            // 清除所有旧事件
            audioPlayer.oncanplay = null;
            audioPlayer.onended = null;
            audioPlayer.onerror = null;
            audioPlayer.ontimeupdate = null;
            audioPlayer.onloadedmetadata = null;
        } catch (e) {
            console.warn('[播放调试] 清理旧播放器失败:', e);
        }
    }
    
    // 创建全新的音频实例
    audioPlayer = new Audio();
    audioPlayer.preload = 'auto';
    audioPlayer.crossOrigin = "anonymous";
    
    const audioUrl = `${API_BASE_URL}/songs/${songId}/audio`;
    console.log(`[播放调试] 音频URL: ${audioUrl}`);
    audioPlayer.src = audioUrl;
    currentPlayingSongId = songId;
    
    // 获取歌曲信息
    let song = currentRecommendations.find(s => s.song_id === songId) || 
               currentHotSongs.find(s => s.song_id === songId) ||
               (window.tempSongStore && window.tempSongStore[songId]);
    
    if (!song) {
        try {
            const response = await fetch(`${API_BASE_URL}/songs/${songId}`);
            const data = await response.json();
            if (data.success) song = data.data;
        } catch(e) {
            console.warn('[播放调试] 获取歌曲详情失败', e);
        }
    }
    
    // 更新UI显示
    if (song) {
        document.getElementById('now-playing-title').textContent = song.song_name || '未知歌曲';
        document.getElementById('now-playing-artist').textContent = song.artists || '未知艺术家';
    }
    
    // 设置超时计时器
    const timeoutId = setTimeout(() => {
        console.warn('[播放调试] 音频加载超时');
        if (audioPlayer.readyState === 0) { // HAVE_NOTHING
            showNotification('音频加载超时，可能文件不存在或网络问题', 'error');
            // 尝试下一首
            setTimeout(() => playNext(), 2000);
        }
    }, 10000); // 10秒超时
    
    // 【优化】简化事件监听器
    audioPlayer.addEventListener('canplay', () => {
        clearTimeout(timeoutId);
        console.log(`[播放调试] 音频可以播放: ${songId}`);
    }, { once: true });
    
    audioPlayer.addEventListener('canplaythrough', () => {
        console.log(`[播放调试] 音频已完全加载: ${songId}`);
    }, { once: true });
    
    audioPlayer.addEventListener('loadedmetadata', () => {
        console.log(`[播放调试] 音频元数据加载: 时长 ${audioPlayer.duration}秒`);
        if (audioPlayer.duration && audioPlayer.duration !== Infinity) {
            document.getElementById('total-time').textContent = formatTime(Math.floor(audioPlayer.duration));
        }
    });
    
    audioPlayer.addEventListener('timeupdate', () => {
        if (!isDraggingProgress && audioPlayer.duration) {
            const progress = (audioPlayer.currentTime / audioPlayer.duration) * 100;
            document.querySelector('.progress-fill').style.width = `${progress}%`;
            document.getElementById('current-time').textContent = formatTime(Math.floor(audioPlayer.currentTime));
        }
    });
    
    audioPlayer.addEventListener('ended', () => {
        console.log(`[播放调试] 音频播放结束: ${songId}`);
        isPlaying = false;
        updatePlayButton();
        playNext();
        window.currentPlayingRequest = null;
    }, { once: true });
    
    // 【关键修复】优化错误处理 - 完全不修改UI状态
    audioPlayer.addEventListener('error', (e) => {
        clearTimeout(timeoutId);
        console.error('[播放调试] 音频播放错误:', e);
        
        let msg = '音频加载失败';
        if (audioPlayer.error) {
            switch(audioPlayer.error.code) {
                case 1: // MEDIA_ERR_ABORTED
                    msg = '音频加载被中止';
                    break;
                case 2: // MEDIA_ERR_NETWORK
                    msg = '网络错误，无法加载音频';
                    break;
                case 3: // MEDIA_ERR_DECODE
                    msg = '音频解码错误';
                    break;
                case 4: // MEDIA_ERR_SRC_NOT_SUPPORTED
                    msg = '音频文件不存在或格式不支持';
                    break;
            }
        }
        
        showNotification(msg, 'error');
        
        isPlaying = false;
        updatePlayButton();
        window.currentPlayingRequest = null;
        
        // 【核心修复】不修改任何UI状态，只显示通知
        // 原来的代码会修改 song.has_audio 和卡片样式，现在完全删除
        
    }, { once: true });
    
    // 开始播放
    try {
        console.log(`[播放调试] 开始加载音频...`);
        
        // 使用Promise包装，避免回调地狱
        const playPromise = new Promise((resolve, reject) => {
            const loadTimeout = setTimeout(() => {
                reject(new Error('音频加载超时'));
            }, 8000);
            
            const canPlayHandler = () => {
                clearTimeout(loadTimeout);
                resolve();
            };
            
            const errorHandler = (e) => {
                clearTimeout(loadTimeout);
                reject(e);
            };
            
            audioPlayer.addEventListener('canplay', canPlayHandler, { once: true });
            audioPlayer.addEventListener('error', errorHandler, { once: true });
        });
        
        await playPromise;
        
        console.log(`[播放调试] 音频可以播放，开始播放...`);
        await audioPlayer.play();
        isPlaying = true;
        updatePlayButton();
        
        // 添加到播放列表（如果不在其中）
        if (!playerPlaylist.includes(songId)) {
            playerPlaylist.push(songId);
            playerCurrentIndex = playerPlaylist.length - 1;
        } else {
            playerCurrentIndex = playerPlaylist.indexOf(songId);
        }
        
        // 记录播放行为
        recordBehavior(songId, 'play', 0.5);
        
        console.log(`[播放调试] 播放成功: ${songId}`);
        showNotification('开始播放', 'success');
        
    } catch (error) {
        console.error('[播放调试] 播放失败:', error);
        isPlaying = false;
        updatePlayButton();
        window.currentPlayingRequest = null;
        
        // 更友好的错误提示
        let userMsg = '播放失败';
        if (error.message && error.message.includes('超时')) {
            userMsg = '音频加载超时，可能服务器忙或文件不存在';
        }
        
        showNotification(userMsg, 'error');
        
        // 【修复】播放失败时不自动下一首，让用户手动选择
        // 删除自动播放下一首的代码
    }
}

// 在script.js中添加
function clearAudioCache() {
    if (audioPlayer) {
        audioPlayer.pause();
        audioPlayer.src = '';
        audioPlayer.load();
        console.log('[调试] 音频缓存已清除');
    }
    
    // 清除播放器状态
    isPlaying = false;
    updatePlayButton();
    document.querySelector('.progress-fill').style.width = '0%';
    document.getElementById('current-time').textContent = '0:00';
    document.getElementById('total-time').textContent = '0:00';
    
    showNotification('播放器已重置', 'info');
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
    
    // 避免重复播放同一首
    if (nextIndex === playerCurrentIndex) {
        showNotification('已是播放列表最后一首', 'info');
        return;
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
            
            // 获取推荐解释
            let explanation = null;
            if (currentUser) {
                try {
                    const recResponse = await fetch(
                        `${API_BASE_URL}/explain/${currentUser}/${songId}`
                    );
                    if (recResponse.ok) {
                        const recData = await recResponse.json();
                        if (recData.success && recData.data) {
                            explanation = recData.data.explanation || recData.data;
                        }
                    }
                } catch (e) {
                    console.log('获取个性化解释失败:', e);
                }
            }
            
            // 显示歌曲详情
            displaySongDetail(song, explanation);
            
            // 延迟加载评论，确保DOM已更新
            setTimeout(() => {
                loadSongComments(songId);
            }, 100);
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
    // 设置歌曲基本信息
    document.getElementById('detail-song-name').textContent = song.song_name || '未知歌曲';
    document.getElementById('detail-artists').textContent = song.artists || '未知艺术家';
    document.getElementById('detail-genre').textContent = song.genre || '未知流派';
    document.getElementById('detail-popularity').textContent = `流行度: ${song.popularity || 50}`;
    
    // 提取音频特征：优先使用 audio_features 对象，否则直接从 song 取
    const features = song.audio_features || song;
    
    // 安全获取浮点数的辅助函数
    const getFloat = (val, defaultValue = 0.5) => {
        if (val === undefined || val === null) return defaultValue;
        const num = parseFloat(val);
        return isNaN(num) ? defaultValue : num;
    };
    
    const danceability = getFloat(features.danceability);
    const energy = getFloat(features.energy);
    const valence = getFloat(features.valence);
    const tempo = getFloat(features.tempo, 120); // 节奏默认120
    
    // 更新UI
    document.getElementById('danceability-value').textContent = danceability.toFixed(2);
    document.getElementById('danceability-bar').style.width = `${danceability * 100}%`;
    
    document.getElementById('energy-value').textContent = energy.toFixed(2);
    document.getElementById('energy-bar').style.width = `${energy * 100}%`;
    
    document.getElementById('valence-value').textContent = valence.toFixed(2);
    document.getElementById('valence-bar').style.width = `${valence * 100}%`;
    
    document.getElementById('tempo-value').textContent = `${Math.round(tempo)} BPM`;
    
    // 处理推荐解释（原代码不变）
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
    
    // 设置播放按钮
    const playBtn = document.getElementById('play-now-btn');
    playBtn.dataset.songId = song.song_id;
    playBtn.onclick = function() {
        playSong(song.song_id);
        document.getElementById('song-modal').classList.remove('active');
    };
    
    // 显示模态框
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
            showNotification(feedback === 'like' ? '感谢您的认可！' : '我们会改进推荐', 'success');
            const card = document.querySelector(`[data-song-id="${songId}"]`);
            if (card) {
                const thumbs = card.querySelectorAll('.feedback-btn-thumb');
                thumbs.forEach(btn => btn.classList.remove('active'));
                if (feedback === 'like') thumbs[0].classList.add('active');
                if (feedback === 'dislike') thumbs[1].classList.add('active');
            }
            
            // 新增：如果是喜欢，记录歌曲喜欢行为
            if (feedback === 'like') {
                recordBehavior(songId, 'like', 1.0);
            }
        }
    } catch (e) {
        console.error('反馈提交失败:', e);
    }
}

// ========== 其他功能 ==========
async function savePreferences() {
    console.log('[保存偏好] 开始保存');
    
    try {
        const defaultAlgorithm = document.getElementById('default-algorithm').value;
        const defaultCount = document.getElementById('default-count').value;
        const diversityValue = document.getElementById('diversity-slider').value;
        
        // 更新前端设置
        document.getElementById('algorithm-select').value = defaultAlgorithm;
        document.getElementById('rec-algorithm-select').value = defaultAlgorithm;
        document.getElementById('rec-count-select').value = defaultCount;
        currentAlgorithm = defaultAlgorithm;
        
        // 保存到本地存储
        const preferences = {
            defaultAlgorithm,
            defaultCount,
            diversityValue,
            savedAt: new Date().toISOString()
        };
        
        localStorage.setItem('musicRec_preferences', JSON.stringify(preferences));
        
        // 尝试保存到后端（如果有API）
        try {
            if (currentUser && currentUser !== '1001') { // 不是默认用户
                const response = await fetch(`${API_BASE_URL}/users/${currentUser}/preferences`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        default_algorithm: defaultAlgorithm,
                        default_count: parseInt(defaultCount),
                        diversity_value: parseInt(diversityValue)
                    })
                });
                
                if (response.ok) {
                    console.log('[保存偏好] 已保存到服务器');
                }
            }
        } catch (error) {
            console.log('[保存偏好] 服务器保存失败，使用本地存储:', error);
        }
        
        showNotification('偏好设置已保存', 'success');
        
        // 记录活动
        addActivity(
            'settings',
            `更新了偏好设置：${getAlgorithmName(defaultAlgorithm)}，${defaultCount}首`,
            'fa-cogs',
            '#7209b7'
        );
        
        console.log('[保存偏好] 保存完成:', preferences);
        
    } catch (error) {
        console.error('[保存偏好] 保存失败:', error);
        showNotification('保存失败，请重试', 'error');
    }
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
    
    // 生成 HTML
    let html = `<button class="genre-tag-btn active" data-genre="all">全部</button>`;
    
    DISPLAY_GENRES.forEach(genre => {
        html += `<button class="genre-tag-btn" data-genre="${genre}">${genre}</button>`;
    });
    
    container.innerHTML = html;
    console.log('[成功] 流派标签已渲染，共', DISPLAY_GENRES.length + 1, '个');
    
    // 绑定事件
    bindGenreEvents();
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
    if (genreEventsBound) {
        console.log('[流派事件] 已绑定，跳过');
        return;
    }
    
    const container = document.getElementById('genre-tags');
    if (!container) {
        console.error('[错误] 找不到容器 #genre-tags');
        return;
    }
    
    container.addEventListener('click', function(e) {
        if (e.target.classList.contains('genre-tag-btn')) {
            e.preventDefault();
            e.stopPropagation();
            
            const genre = e.target.dataset.genre;
            console.log('点击流派:', genre);
            
            // UI状态更新
            document.querySelectorAll('.genre-tag-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            e.target.classList.add('active');
            
            // 调用筛选函数
            if (typeof filterSongsByGenre === 'function') {
                filterSongsByGenre(genre);
            }
        }
    });
    
    genreEventsBound = true;
    console.log('流派标签事件绑定完成');
}

// ================== 修复版流派筛选（支持自动从后端加载） ==================

async function filterSongsByGenre(genre) {
    console.log(`[流派筛选] 选择: ${genre}`);
    
    const container = document.getElementById('explore-container');
    const loadMoreContainer = document.getElementById('explore-load-more');
    
    if (!container) return;
    
    // 隐藏加载更多按钮（先隐藏，后面再决定是否显示）
    if (loadMoreContainer) {
        loadMoreContainer.style.display = 'none';
    }
    
    // 如果是"全部"，显示热门歌曲
    if (genre === 'all') {
        if (currentHotSongs.length > 0) {
            displayExploreSongs(currentHotSongs);
            // 显示加载更多按钮
            if (loadMoreContainer) {
                loadMoreContainer.style.display = 'flex';
                // 绑定加载更多热门歌曲的事件
                bindLoadMoreForAll();
            }
        } else {
            await loadHotSongs('all');
        }
        return;
    }
    
    // 显示加载中
    container.innerHTML = '<div class="empty-state"><i class="fas fa-spinner fa-spin"></i><p>正在筛选...</p></div>';
    
    try {
        const sourceGenres = REVERSE_MAP[genre];
        if (!sourceGenres) {
            container.innerHTML = '<div class="empty-state"><p>未知分类</p></div>';
            return;
        }
        
        console.log(`[流派筛选] ${genre} -> 查询:`, sourceGenres);
        
        // 使用第一个子流派作为查询条件（确保精确匹配）
        const genreParam = encodeURIComponent(sourceGenres[0]); // 使用第一个流派
        const response = await fetch(`${API_BASE_URL}/songs/by-genre?genre=${genreParam}&limit=30`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.data.songs.length > 0) {
            displayExploreSongs(data.data.songs);
            
            // 如果有更多数据，显示加载更多按钮
            if (data.data.has_more && loadMoreContainer) {
                loadMoreContainer.style.display = 'flex';
                // 绑定加载更多流派歌曲的事件
                bindLoadMoreForGenre(genre, data.data.songs.length);
            }
            
            showNotification(`${genre}: 找到 ${data.data.songs.length} 首歌曲`, 'success');
        } else {
            container.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-music"></i>
                    <p>暂无"${genre}"流派的歌曲</p>
                    <p style="color: var(--text-secondary); margin-top: 0.5rem;">
                        尝试浏览其他流派
                    </p>
                </div>
            `;
        }
    } catch (error) {
        console.error('[流派筛选] 错误:', error);
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-exclamation-circle"></i>
                <p>筛选失败: ${error.message}</p>
                <button class="btn btn-primary" onclick="filterSongsByGenre('all')" style="margin-top:1rem">
                    返回热门歌曲
                </button>
            </div>
        `;
    }
}

// 绑定流派加载更多事件
function bindLoadMoreForGenre(genre, currentCount) {
    const loadMoreBtn = document.getElementById('load-more-btn');
    if (!loadMoreBtn) return;
    
    // 移除旧事件
    loadMoreBtn.replaceWith(loadMoreBtn.cloneNode(true));
    const newBtn = document.getElementById('load-more-btn');
    
    newBtn.addEventListener('click', async () => {
        console.log(`[加载更多] 流派: ${genre}, 当前: ${currentCount}`);
        
        newBtn.disabled = true;
        newBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加载中...';
        
        try {
            const sourceGenres = REVERSE_MAP[genre];
            const genreParam = encodeURIComponent(sourceGenres[0]);
            const response = await fetch(
                `${API_BASE_URL}/songs/by-genre?genre=${genreParam}&limit=20&offset=${currentCount}`
            );
            
            if (response.ok) {
                const data = await response.json();
                if (data.success && data.data.songs.length > 0) {
                    // 追加显示歌曲
                    const container = document.getElementById('explore-container');
                    const html = data.data.songs.map(song => createExploreSongCard(song)).join('');
                    container.insertAdjacentHTML('beforeend', html);
                    currentCount += data.data.songs.length;
                    
                    if (!data.data.has_more) {
                        const loadMoreContainer = document.getElementById('explore-load-more');
                        if (loadMoreContainer) {
                            loadMoreContainer.style.display = 'none';
                        }
                        showNotification('已加载全部歌曲', 'info');
                    } else {
                        showNotification(`已加载 ${data.data.songs.length} 首更多歌曲`, 'success');
                    }
                } else {
                    showNotification('没有更多歌曲了', 'info');
                    const loadMoreContainer = document.getElementById('explore-load-more');
                    if (loadMoreContainer) {
                        loadMoreContainer.style.display = 'none';
                    }
                }
            }
        } catch (error) {
            console.error('加载更多失败:', error);
            showNotification('加载失败', 'error');
        } finally {
            newBtn.disabled = false;
            newBtn.innerHTML = '<i class="fas fa-plus"></i> 加载更多';
        }
    });
}

// 绑定全部热门歌曲加载更多
function bindLoadMoreForAll() {
    const loadMoreBtn = document.getElementById('load-more-btn');
    if (!loadMoreBtn) return;
    
    loadMoreBtn.replaceWith(loadMoreBtn.cloneNode(true));
    const newBtn = document.getElementById('load-more-btn');
    
    newBtn.addEventListener('click', async () => {
        console.log('[加载更多] 全部热门歌曲');
        
        newBtn.disabled = true;
        newBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加载中...';
        
        try {
            // 当前已显示的数量作为 offset
            const currentCount = document.querySelectorAll('#explore-container .song-card').length;
            const response = await fetch(`${API_BASE_URL}/songs/hot?tier=all&n=20&offset=${currentCount}`);
            const data = await response.json();
            
            if (data.success && data.data.songs.length > 0) {
                // 追加显示
                const container = document.getElementById('explore-container');
                const html = data.data.songs.map(song => createExploreSongCard(song)).join('');
                container.insertAdjacentHTML('beforeend', html);
                
                // 更新当前热门歌曲数据
                currentHotSongs = [...currentHotSongs, ...data.data.songs];
                
                showNotification(`已加载 ${data.data.songs.length} 首更多歌曲`, 'success');
            } else {
                showNotification('没有更多歌曲了', 'info');
                const loadMoreContainer = document.getElementById('explore-load-more');
                if (loadMoreContainer) {
                    loadMoreContainer.style.display = 'none';
                }
            }
        } catch (error) {
            console.error('加载更多失败:', error);
            showNotification('加载失败', 'error');
        } finally {
            newBtn.disabled = false;
            newBtn.innerHTML = '<i class="fas fa-plus"></i> 加载更多';
        }
    });
}

// 创建探索页面歌曲卡片
function createExploreSongCard(song) {
    // 显示真实的流行度
    const popularity = song.popularity ? Math.round(song.popularity) : 50;
    
    return `
    <div class="song-card explore-card" data-song-id="${song.song_id}">
        <div class="song-card-header" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
            <i class="fas fa-compact-disc"></i>
            <span>发现</span>
            ${song.has_audio ? 
                '<span class="audio-badge" title="可播放"><i class="fas fa-volume-up"></i></span>' : 
                '<span class="no-audio-badge">预览</span>'
            }
        </div>
        <div class="song-card-body">
            <h3 class="song-title" title="${song.song_name || '未知歌曲'}">${song.song_name || '未知歌曲'}</h3>
            <p class="song-artist" title="${song.artists || '未知艺术家'}">${song.artists || '未知艺术家'}</p>
            <div class="song-meta">
                <span class="genre-tag">${song.genre || '未知流派'}</span>
                <span class="popularity-badge">${popularity}</span>
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

// 更新流派加载更多功能
function updateLoadMoreForGenre(genre, currentCount) {
    const loadMoreBtn = document.getElementById('load-more-btn');
    if (!loadMoreBtn) return;
    
    // 移除旧事件
    loadMoreBtn.replaceWith(loadMoreBtn.cloneNode(true));
    const newBtn = document.getElementById('load-more-btn');
    
    newBtn.addEventListener('click', async () => {
        console.log(`[加载更多] 流派: ${genre}, 当前: ${currentCount}`);
        
        newBtn.disabled = true;
        newBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加载中...';
        
        try {
            const sourceGenres = REVERSE_MAP[genre] || [genre];
            const genreParam = encodeURIComponent(sourceGenres.join(','));
            const response = await fetch(
                `${API_BASE_URL}/songs/by-genre?genre=${genreParam}&limit=20&offset=${currentCount}&fuzzy=1`
            );
            
            if (response.ok) {
                const data = await response.json();
                if (data.success && data.data.songs.length > 0) {
                    appendExploreSongs(data.data.songs);
                    currentCount += data.data.songs.length;
                    
                    if (!data.data.has_more) {
                        const loadMoreContainer = document.getElementById('explore-load-more');
                        if (loadMoreContainer) {
                            loadMoreContainer.style.display = 'none';
                        }
                        showNotification('已加载全部歌曲', 'info');
                    } else {
                        showNotification(`已加载 ${data.data.songs.length} 首更多歌曲`, 'success');
                    }
                } else {
                    showNotification('没有更多歌曲了', 'info');
                    const loadMoreContainer = document.getElementById('explore-load-more');
                    if (loadMoreContainer) {
                        loadMoreContainer.style.display = 'none';
                    }
                }
            }
        } catch (error) {
            console.error('加载更多失败:', error);
            showNotification('加载失败', 'error');
        } finally {
            newBtn.disabled = false;
            newBtn.innerHTML = '<i class="fas fa-plus"></i> 加载更多';
        }
    });
}

// 备用流派筛选
async function fallbackGenreFilter(genre, sourceGenres) {
    const container = document.getElementById('explore-container');
    
    // 确保有热门歌曲数据
    if (currentHotSongs.length === 0) {
        await loadHotSongs('all');
    }
    
    // 从前端数据中过滤
    const filtered = currentHotSongs.filter(song => {
        if (!song || !song.genre) return false;
        return sourceGenres.includes(song.genre);
    });
    
    if (filtered.length > 0) {
        displayExploreSongs(filtered);
        showNotification(`${genre}: 找到 ${filtered.length} 首歌曲`, 'success');
    } else {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-music"></i>
                <p>暂无"${genre}"流派的歌曲</p>
                <p style="color: var(--text-secondary); margin-top: 0.5rem;">
                    尝试浏览其他流派
                </p>
            </div>
        `;
        showNotification(`暂无${genre}流派歌曲`, 'info');
    }
}

// 新增排序函数
function sortSongsByAudioAndPopularity(songs) {
    return [...songs].sort((a, b) => {
        // 首先按是否有音频排序（有音频的在前）
        const aHasAudio = a.has_audio !== false;
        const bHasAudio = b.has_audio !== false;
        
        if (aHasAudio && !bHasAudio) return -1;
        if (!aHasAudio && bHasAudio) return 1;
        
        // 都有音频或都没有音频时，按流行度降序
        return (b.popularity || 0) - (a.popularity || 0);
    });
}

// 【新增】从后端数据库加载指定流派的歌曲
async function loadSongsByGenreFromBackend(sourceGenres, displayGenre) {
    const container = document.getElementById('explore-container');
    container.innerHTML = '<div class="empty-state"><i class="fas fa-spinner fa-spin"></i><p>正在从数据库加载...</p></div>';
    
    try {
        // 【修复】移除sort_by_audio参数
        const genreParam = encodeURIComponent(sourceGenres.join(','));
        const response = await fetch(`${API_BASE_URL}/songs/by-genre?genre=${genreParam}&limit=50&offset=0`);
        const data = await response.json();
        
        if (data.success && data.data.songs && data.data.songs.length > 0) {
            // 【修复】在前端排序
            const sortedSongs = sortSongsByAudioAndPopularity(data.data.songs);
            displayExploreSongs(sortedSongs);
            
            // 统计信息
            const audioCount = sortedSongs.filter(s => s.has_audio).length;
            showNotification(`${displayGenre}: 找到 ${sortedSongs.length} 首歌曲（${audioCount}首可播放）`, 'success');
        } else {
            throw new Error('API返回空数据');
        }
        
    } catch (error) {
        console.warn('API加载失败，尝试备用方案:', error);
        
        // 备用方案：加载热门歌曲
        try {
            container.innerHTML = '<div class="empty-state"><i class="fas fa-spinner fa-spin"></i><p>尝试加载更多数据...</p></div>';
            
            const response = await fetch(`${API_BASE_URL}/songs/hot?tier=all&n=100`);
            const data = await response.json();
            
            if (data.success && data.data.songs) {
                // 更新当前数据
                currentHotSongs = data.data.songs;
                
                // 过滤当前流派的歌曲
                const filteredSongs = currentHotSongs.filter(song => {
                    if (!song || !song.genre) return false;
                    return sourceGenres.includes(song.genre);
                });
                
                if (filteredSongs.length > 0) {
                    // 前端排序
                    const sortedSongs = sortSongsByAudioAndPopularity(filteredSongs);
                    displayExploreSongs(sortedSongs);
                    
                    const audioCount = sortedSongs.filter(s => s.has_audio).length;
                    showNotification(`${displayGenre}: 找到 ${sortedSongs.length} 首（${audioCount}首可播放）`, 'success');
                } else {
                    // 显示全部热门歌曲
                    const sortedSongs = sortSongsByAudioAndPopularity(currentHotSongs);
                    displayExploreSongs(sortedSongs);
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
    console.log('[探索] 加载内容');
    
    const container = document.getElementById('explore-container');
    const loadMoreBtn = document.getElementById('explore-load-more');
    
    if (loadMoreBtn) loadMoreBtn.style.display = 'none';
    
    // 如果有搜索词，显示搜索结果
    const searchInput = document.getElementById('explore-search-input');
    if (searchInput && searchInput.value.trim()) {
        simpleSearch(searchInput.value.trim());
        return;
    }
    
    // 否则显示热门歌曲
    if (currentHotSongs.length === 0) {
        await loadHotSongs('all');
    }
    
    if (currentHotSongs.length > 0) {
        displayExploreSongs(currentHotSongs);
        loadGenres();
    } else {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-exclamation-circle"></i>
                <p>暂无歌曲数据</p>
                <button class="btn btn-primary" onclick="loadHotSongs('all')" style="margin-top:1rem">
                    加载热门歌曲
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
        <div class="song-card explore-card" data-song-id="${song.song_id}" data-has-audio="${song.has_audio !== false}">
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

async function updateStats() {
    console.log('[统计] 开始更新统计数据');
    
    try {
        // 【修改】使用正确的端点：/api/v1/songs/stats
        const response = await fetch(`${API_BASE_URL}/songs/stats`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            // 使用真实数据
            const stats = data.data;
            
            // 确保数字是有效的
            const activeUsers = Math.max(1, stats.active_users || 43355);
            const totalSongs = Math.max(1, stats.total_songs || 16588);
            const todayRecommends = Math.max(1, stats.today_recommends || 500);
            
            animateCount('user-count', activeUsers);
            animateCount('song-count', totalSongs);
            animateCount('rec-count', todayRecommends);
            
            console.log('[统计] 更新成功:', {
                activeUsers,
                totalSongs,
                todayRecommends,
                audioSongs: stats.audio_songs
            });
        } else {
            console.warn('[统计] API返回失败，使用默认值');
            animateCount('user-count', 43355);
            animateCount('song-count', 16588);
            animateCount('rec-count', 500);
        }
    } catch (error) {
        console.error('[统计] 获取失败:', error);
        // 使用默认值
        animateCount('user-count', 43355);
        animateCount('song-count', 16588);
        animateCount('rec-count', 500);
    }
}

function animateCount(elementId, target) {
    const element = document.getElementById(elementId);
    if (!element) {
        console.error(`[统计] 找不到元素: ${elementId}`);
        return;
    }
    
    const current = parseInt(element.textContent.replace(/,/g, '')) || 0;
    
    // 如果目标值和当前值相同，直接设置
    if (current === target) {
        element.textContent = target.toLocaleString();
        return;
    }
    
    const increment = Math.ceil((target - current) / 50);
    let count = current;
    
    const timer = setInterval(() => {
        count += increment;
        if ((increment > 0 && count >= target) || (increment < 0 && count <= target)) {
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

// 全局变量记录各流派的加载偏移量
const genreOffsets = {};

function initLoadMore() {
    if (loadMoreInitialized) {
        console.log('[加载更多] 已初始化，跳过');
        return;
    }
    
    const loadMoreBtn = document.getElementById('load-more-btn');
    if (!loadMoreBtn) {
        console.error('[错误] 找不到加载更多按钮');
        return;
    }
    
    console.log('[调试] 初始化加载更多按钮');
    
    // 使用事件委托，避免重复绑定
    document.addEventListener('click', function(e) {
        if (e.target.id === 'load-more-btn' || e.target.closest('#load-more-btn')) {
            e.preventDefault();
            e.stopPropagation();
            
            console.log('[点击] 加载更多按钮');
            
            const activeBtn = document.querySelector('.genre-tag-btn.active');
            if (!activeBtn) {
                console.error('[错误] 没有激活的流派按钮');
                return;
            }
            
            const currentGenre = activeBtn.dataset.genre;
            console.log(`[加载更多] 当前流派: ${currentGenre}`);
            
            handleLoadMore(currentGenre);
        }
    });
    
    loadMoreInitialized = true;
    console.log('[成功] 加载更多按钮事件已绑定');
}

// 添加统一的加载更多处理函数
async function handleLoadMore(currentGenre) {
    const loadMoreBtn = document.getElementById('load-more-btn');
    if (!loadMoreBtn) return;
    
    // 禁用按钮防止重复点击
    loadMoreBtn.disabled = true;
    loadMoreBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加载中...';
    
    try {
        if (currentGenre === 'all') {
            await loadMoreHotSongs();
        } else {
            await loadMoreByGenre(currentGenre);
        }
    } catch (error) {
        console.error('[加载更多] 错误:', error);
        showNotification('加载失败: ' + error.message, 'error');
    } finally {
        // 恢复按钮
        loadMoreBtn.disabled = false;
        loadMoreBtn.innerHTML = '<i class="fas fa-plus"></i> 加载更多';
    }
}

// 加载更多特定流派歌曲
async function loadMoreByGenre(genre) {
    console.log(`[加载更多] 流派: ${genre}`);
    
    // 获取当前已显示的数量
    const container = document.getElementById('explore-container');
    const currentCount = container.querySelectorAll('.song-card').length;
    
    // 计算偏移量
    const offset = currentCount;
    const limit = 12; // 每次加载12首
    
    try {
        // 获取该流派对应的原始流派列表
        const sourceGenres = REVERSE_MAP[genre];
        if (!sourceGenres) {
            throw new Error(`未知流派: ${genre}`);
        }
        
        // 使用第一个流派作为查询条件
        const genreParam = encodeURIComponent(sourceGenres[0]);
        
        console.log(`[加载更多] 查询: ${genreParam}, offset: ${offset}, limit: ${limit}`);
        
        const response = await fetch(
            `${API_BASE_URL}/songs/by-genre?genre=${genreParam}&limit=${limit}&offset=${offset}`
        );
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.data.songs && data.data.songs.length > 0) {
            // 追加显示
            const newSongs = data.data.songs;
            const html = newSongs.map(song => createExploreSongCard(song)).join('');
            container.insertAdjacentHTML('beforeend', html);
            
            console.log(`[加载更多] 成功添加 ${newSongs.length} 首歌曲`);
            showNotification(`已加载 ${newSongs.length} 首更多歌曲`, 'success');
            
            // 检查是否还有更多
            if (!data.data.has_more) {
                const loadMoreContainer = document.getElementById('explore-load-more');
                if (loadMoreContainer) {
                    loadMoreContainer.style.display = 'none';
                }
                showNotification('已加载全部歌曲', 'info');
            }
        } else {
            showNotification('没有更多歌曲了', 'info');
            const loadMoreContainer = document.getElementById('explore-load-more');
            if (loadMoreContainer) {
                loadMoreContainer.style.display = 'none';
            }
        }
    } catch (error) {
        console.error('[加载更多] 失败:', error);
        showNotification('加载失败: ' + error.message, 'error');
    }
}

// 追加渲染歌曲（不清空已有）
function appendExploreSongs(newSongs) {
    const container = document.getElementById('explore-container');
    
    const html = newSongs.map(song => `
        <div class="song-card explore-card" data-song-id="${song.song_id}">
            <div class="song-card-header" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                <i class="fas fa-compact-disc"></i>
                <span>${song.genre || '音乐'}</span>
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
    
    container.insertAdjacentHTML('beforeend', html);
}

// 加载更多热门歌曲（全部）
async function loadMoreHotSongs() {
    // 当前已显示的数量作为 offset
    const currentCount = currentHotSongs.length;
    const limit = 20;
    
    try {
        const response = await fetch(`${API_BASE_URL}/songs/hot?tier=all&n=${limit}&offset=${currentCount}`);
        const data = await response.json();
        
        if (data.success && data.data.songs.length > 0) {
            // 追加到现有数据
            currentHotSongs = [...currentHotSongs, ...data.data.songs];
            
            // 如果是当前显示"全部"，则更新视图
            const activeGenre = document.querySelector('.genre-tag-btn.active')?.dataset.genre;
            if (activeGenre === 'all') {
                displayExploreSongs(currentHotSongs);
            }
            
            showNotification(`已加载 ${data.data.songs.length} 首更多歌曲`, 'success');
        } else {
            showNotification('没有更多歌曲了', 'info');
        }
    } catch (error) {
        console.error('加载更多失败:', error);
        showNotification('加载失败', 'error');
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

// 在 script.js 末尾添加
window.debugAudio = function(songId) {
    console.log('=== 音频调试 ===');
    console.log('歌曲ID:', songId);
    console.log('音频URL:', `${API_BASE_URL}/songs/${songId}/audio`);
    
    // 直接播放
    const audio = new Audio();
    audio.src = `${API_BASE_URL}/songs/${songId}/audio`;
    audio.crossOrigin = 'anonymous';
    
    audio.oncanplay = () => {
        console.log('直接Audio对象可以播放');
        audio.play().then(() => {
            console.log('直接Audio播放成功');
        }).catch(e => {
            console.error('直接Audio播放失败:', e);
        });
    };
    
    audio.onerror = (e) => {
        console.error('直接Audio错误:', audio.error);
    };
    
    audio.load();
};

window.debugPlaylist = function() {
    console.log('当前播放列表:', playerPlaylist);
    console.log('当前播放索引:', playerCurrentIndex);
    console.log('音频播放器状态:', audioPlayer ? {
        paused: audioPlayer.paused,
        src: audioPlayer.src,
        currentTime: audioPlayer.currentTime,
        duration: audioPlayer.duration,
        error: audioPlayer.error
    } : '无音频播放器');
};

// 更新歌曲卡片的音频状态显示
function updateSongCardAudioStatus(songId, hasAudio) {
    const songCard = document.querySelector(`[data-song-id="${songId}"]`);
    if (!songCard) return;
    
    const header = songCard.querySelector('.song-card-header');
    if (!header) return;
    
    // 移除旧的音频标记
    const oldBadge = header.querySelector('.audio-badge, .no-audio-badge');
    if (oldBadge) oldBadge.remove();
    
    // 添加新的音频标记
    if (hasAudio) {
        const audioBadge = document.createElement('span');
        audioBadge.className = 'audio-badge';
        audioBadge.title = '可播放';
        audioBadge.innerHTML = '<i class="fas fa-volume-up"></i>';
        header.appendChild(audioBadge);
        songCard.classList.remove('no-audio');
    } else {
        const noAudioBadge = document.createElement('span');
        noAudioBadge.className = 'no-audio-badge';
        noAudioBadge.textContent = '预览';
        header.appendChild(noAudioBadge);
        songCard.classList.add('no-audio');
    }
}

// ========== 完整搜索功能 ==========

// 搜索状态
let currentSearchQuery = '';
let searchResults = [];
let searchOffset = 0;
const SEARCH_PAGE_SIZE = 20;

// 初始化搜索
function initSearch() {
    const searchInput = document.getElementById('explore-search-input');
    const searchBtn = document.getElementById('search-btn-execute');
    const clearBtn = document.getElementById('clear-search-btn');
    
    if (!searchInput) return;
    
    // 点击搜索按钮
    if (searchBtn) {
        searchBtn.addEventListener('click', () => {
            const query = searchInput.value.trim();
            if (query) {
                performSearch(query);
            } else {
                showNotification('请输入搜索内容', 'warning');
            }
        });
    }
    
    // Enter键搜索
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            const query = searchInput.value.trim();
            if (query) {
                performSearch(query);
            }
        }
    });
    
    // 清除搜索
    if (clearBtn) {
        clearBtn.addEventListener('click', clearSearch);
    }
}

// 执行搜索
async function performSearch(query) {
    console.log(`[搜索] 执行搜索: "${query}"`);
    
    if (!query || query.trim().length < 2) {
        showNotification('请输入至少2个字符', 'warning');
        return;
    }
    
    currentSearchQuery = query;
    searchOffset = 0;
    
    // 显示加载状态
    const container = document.getElementById('explore-container');
    container.innerHTML = `
        <div class="empty-state" style="grid-column: 1 / -1;">
            <i class="fas fa-spinner fa-spin"></i>
            <p>正在搜索 "${query}"...</p>
        </div>
    `;
    
    // 显示搜索信息栏
    const resultsInfo = document.getElementById('explore-results-info');
    if (resultsInfo) {
        resultsInfo.style.display = 'block';
        document.getElementById('search-result-count').textContent = '...';
    }
    
    try {
        // 1. 首先在前端搜索
        let results = searchInFrontend(query);
        
        // 2. 如果前端没有结果，尝试从后端搜索
        if (results.length === 0) {
            console.log('[搜索] 前端无结果，尝试后端搜索');
            results = await searchFromBackend(query, 0, SEARCH_PAGE_SIZE);
        }
        
        // 3. 显示结果
        searchResults = results;
        
        if (resultsInfo) {
            document.getElementById('search-result-count').textContent = searchResults.length;
        }
        
        if (searchResults.length > 0) {
            displaySearchResults(searchResults, query);
            showNotification(`找到 ${searchResults.length} 个结果`, 'success');
            
            // 如果有后端结果，显示加载更多按钮
            if (results.length >= SEARCH_PAGE_SIZE) {
                const loadMoreContainer = document.getElementById('explore-load-more');
                if (loadMoreContainer) {
                    loadMoreContainer.innerHTML = `
                        <button id="load-more-search-btn" class="btn btn-secondary">
                            <i class="fas fa-plus"></i> 加载更多搜索结果
                        </button>
                    `;
                    loadMoreContainer.style.display = 'flex';
                    
                    document.getElementById('load-more-search-btn').addEventListener('click', () => {
                        loadMoreSearchResults();
                    });
                }
            }
        } else {
            container.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-search"></i>
                    <p>未找到匹配 "${query}" 的歌曲</p>
                    <div style="margin-top: 1rem;">
                        <p style="color: var(--text-secondary);">尝试：</p>
                        <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem; justify-content: center; flex-wrap: wrap;">
                            <span class="genre-tag" onclick="performSearch('流行')">流行</span>
                            <span class="genre-tag" onclick="performSearch('摇滚')">摇滚</span>
                            <span class="genre-tag" onclick="performSearch('电子')">电子</span>
                        </div>
                    </div>
                </div>
            `;
            showNotification('未找到相关歌曲', 'info');
        }
        
    } catch (error) {
        console.error('[搜索] 搜索失败:', error);
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-exclamation-circle"></i>
                <p>搜索失败: ${error.message}</p>
                <button class="btn btn-primary" onclick="performSearch('${query}')" style="margin-top:1rem">
                    重试搜索
                </button>
            </div>
        `;
    }
}

// 前端搜索
function searchInFrontend(query) {
    const lowerQuery = query.toLowerCase().trim();
    
    if (!lowerQuery || lowerQuery.length < 2) return [];
    
    // 从所有已加载的数据中搜索
    const allSongs = [...currentHotSongs, ...currentRecommendations];
    
    // 去重
    const seen = new Set();
    const uniqueSongs = [];
    
    for (const song of allSongs) {
        if (!song || !song.song_id) continue;
        if (seen.has(song.song_id)) continue;
        seen.add(song.song_id);
        uniqueSongs.push(song);
    }
    
    // 搜索逻辑
    return uniqueSongs.filter(song => {
        // 歌曲名
        if (song.song_name && song.song_name.toLowerCase().includes(lowerQuery)) {
            return true;
        }
        // 艺术家
        if (song.artists && song.artists.toLowerCase().includes(lowerQuery)) {
            return true;
        }
        // 专辑
        if (song.album && song.album.toLowerCase().includes(lowerQuery)) {
            return true;
        }
        // 流派
        if (song.genre && song.genre.toLowerCase().includes(lowerQuery)) {
            return true;
        }
        return false;
    });
}

// 后端搜索
async function searchFromBackend(query, offset = 0, limit = 20) {
    console.log(`[后端搜索] 查询: "${query}", offset: ${offset}, limit: ${limit}`);
    
    try {
        // 【修正】使用正确的API端点
        const response = await fetch(`${API_BASE_URL}/songs/search?q=${encodeURIComponent(query)}&limit=${limit}&offset=${offset}`);
        
        if (!response.ok) {
            console.log(`[后端搜索] API不可用 (${response.status})，使用备用方案`);
            
            // 备用方案：使用热门歌曲API
            const hotResponse = await fetch(`${API_BASE_URL}/songs/hot?tier=all&n=100`);
            const hotData = await hotResponse.json();
            
            if (hotData.success && hotData.data.songs) {
                currentHotSongs = hotData.data.songs;
                return searchInFrontend(query);
            }
            
            return [];
        }
        
        const data = await response.json();
        
        if (data.success && data.data.songs) {
            console.log(`[后端搜索] 返回 ${data.data.songs.length} 个结果`);
            return data.data.songs;
        }
        
        return [];
        
    } catch (error) {
        console.error('[后端搜索] 错误:', error);
        return [];
    }
}

// 加载更多搜索结果
async function loadMoreSearchResults() {
    if (!currentSearchQuery) return;
    
    const loadMoreBtn = document.getElementById('load-more-search-btn');
    if (loadMoreBtn) {
        loadMoreBtn.disabled = true;
        loadMoreBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 加载中...';
    }
    
    try {
        searchOffset += SEARCH_PAGE_SIZE;
        const moreResults = await searchFromBackend(currentSearchQuery, searchOffset, SEARCH_PAGE_SIZE);
        
        if (moreResults.length > 0) {
            searchResults = [...searchResults, ...moreResults];
            
            // 更新显示
            displaySearchResults(searchResults, currentSearchQuery);
            
            // 更新计数
            const resultsInfo = document.getElementById('explore-results-info');
            if (resultsInfo) {
                document.getElementById('search-result-count').textContent = searchResults.length;
            }
            
            showNotification(`已加载 ${moreResults.length} 个更多结果`, 'success');
        } else {
            showNotification('没有更多搜索结果了', 'info');
            const loadMoreContainer = document.getElementById('explore-load-more');
            if (loadMoreContainer) {
                loadMoreContainer.style.display = 'none';
            }
        }
    } catch (error) {
        console.error('[搜索] 加载更多失败:', error);
        showNotification('加载更多失败', 'error');
    } finally {
        if (loadMoreBtn) {
            loadMoreBtn.disabled = false;
            loadMoreBtn.innerHTML = '<i class="fas fa-plus"></i> 加载更多搜索结果';
        }
    }
}

// 显示搜索结果
function displaySearchResults(songs, query) {
    const container = document.getElementById('explore-container');
    
    if (!songs || songs.length === 0) {
        container.innerHTML = '<div class="empty-state"><p>无结果</p></div>';
        return;
    }
    
    container.innerHTML = songs.map(song => {
        const hasAudio = song.has_audio !== false;
        
        return `
        <div class="song-card explore-card" data-song-id="${song.song_id}" data-has-audio="${hasAudio}">
            <div class="song-card-header" style="background: linear-gradient(135deg, ${hasAudio ? '#4361ee' : '#6c757d'} 0%, ${hasAudio ? '#7209b7' : '#495057'} 100%);">
                <i class="fas fa-search"></i>
                <span>搜索</span>
                ${hasAudio ? 
                    '<span class="audio-badge"><i class="fas fa-volume-up"></i></span>' : 
                    '<span class="no-audio-badge">预览</span>'
                }
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
    `}).join('');
}

// 清除搜索
function clearSearch() {
    console.log('[搜索] 清除搜索');
    
    const searchInput = document.getElementById('explore-search-input');
    const resultsInfo = document.getElementById('explore-results-info');
    const loadMoreContainer = document.getElementById('explore-load-more');
    
    if (searchInput) searchInput.value = '';
    if (resultsInfo) resultsInfo.style.display = 'none';
    if (loadMoreContainer) loadMoreContainer.style.display = 'none';
    
    currentSearchQuery = '';
    searchResults = [];
    searchOffset = 0;
    
    // 回到探索页面
    loadExploreContent();
    
    showNotification('已清除搜索', 'info');
}

async function searchGenreDirectly(genre) {
    try {
        // 尝试多种方式搜索流派
        const attempts = [
            // 1. 直接搜索流派名
            `${API_BASE_URL}/songs/search?q=${encodeURIComponent(genre)}&limit=100`,
            // 2. 使用 by-genre API
            `${API_BASE_URL}/songs/by-genre?genre=${encodeURIComponent(genre)}&limit=100`,
            // 3. 搜索热门歌曲中过滤
            `${API_BASE_URL}/songs/hot?tier=all&n=200`
        ];
        
        for (const url of attempts) {
            try {
                const response = await fetch(url);
                if (!response.ok) continue;
                
                const data = await response.json();
                if (data.success && data.data.songs && data.data.songs.length > 0) {
                    let songs = data.data.songs;
                    
                    // 如果不是直接搜索，需要过滤流派
                    if (!url.includes('search')) {
                        songs = songs.filter(song => {
                            const songGenre = (song.genre || '').toLowerCase();
                            return songGenre.includes(genre.toLowerCase());
                        });
                    }
                    
                    if (songs.length > 0) {
                        console.log(`[流派搜索] 通过 ${url} 找到 ${songs.length} 首歌曲`);
                        return songs;
                    }
                }
            } catch (e) {
                console.warn(`尝试 ${url} 失败:`, e);
            }
        }
        
        return [];
    } catch (error) {
        console.error('[流派搜索] 错误:', error);
        return [];
    }
}

// ========== 评论功能函数 ==========

async function loadSongComments(songId) {
    console.log(`[评论] 加载歌曲评论: ${songId}`);
    
    currentSongIdForComments = songId;
    commentsCurrentPage = 1;
    
    try {
        const response = await fetch(
            `${API_BASE_URL}/songs/${songId}/comments?page=${commentsCurrentPage}&sort_by=${commentsSortBy}&order=desc`
        );
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            currentCommentsData = data.data;
            displayComments(data.data);
            
            // 更新统计信息
            updateCommentsStats(data.data.stats);
        } else {
            throw new Error(data.message || '获取评论失败');
        }
    } catch (error) {
        console.error('[评论] 加载失败:', error);
        showCommentsError();
    }
}

function displayComments(data) {
    const commentsList = document.getElementById('comments-list');
    const pagination = document.getElementById('comments-pagination');
    const sentimentChart = document.getElementById('sentiment-chart');
    
    if (!commentsList) return;
    
    // 显示/隐藏情感图表
    if (data.stats.total_comments > 0) {
        sentimentChart.style.display = 'grid';
    } else {
        sentimentChart.style.display = 'none';
    }
    
    // 显示/隐藏分页
    if (data.pagination.pages > 1) {
        pagination.style.display = 'flex';
        updatePaginationInfo(data.pagination);
    } else {
        pagination.style.display = 'none';
    }
    
    // 渲染评论列表
    if (!data.comments || data.comments.length === 0) {
        commentsList.innerHTML = `
            <div class="comments-empty">
                <i class="fas fa-comment-slash"></i>
                <p>暂无评论，快来发表第一条评论吧！</p>
            </div>
        `;
        return;
    }
    
    commentsList.innerHTML = data.comments.map(comment => {
        const timeStr = comment.comment_time ? 
            formatCommentTime(comment.comment_time) : '刚刚';
        
        const sentimentClass = getSentimentClass(comment.sentiment_score);
        const sentimentText = getSentimentText(comment.sentiment_score);
        
        // 用户头像首字母
        const userInitial = comment.user_nickname ? 
            comment.user_nickname.charAt(0).toUpperCase() : 'A';
        
        return `
            <div class="comment-item" data-comment-id="${comment.comment_id}">
                <div class="comment-header">
                    <div class="comment-user">
                        <div class="user-avatar">${userInitial}</div>
                        <div class="user-info">
                            <h4>${comment.user_nickname}</h4>
                            <span class="time">${timeStr}</span>
                        </div>
                    </div>
                    <div class="comment-actions">
                        <button class="comment-like-btn" onclick="likeComment(${comment.comment_id}, this)">
                            <i class="fas fa-heart"></i>
                            <span class="like-count">${comment.liked_count}</span>
                        </button>
                    </div>
                </div>
                <div class="comment-content">
                    ${escapeHtml(comment.content)}
                </div>
                ${comment.sentiment_score !== null ? `
                    <div class="comment-sentiment ${sentimentClass}">
                        <i class="fas ${SENTIMENT_ICONS[sentimentClass] || 'fa-meh'}"></i>
                        ${sentimentText}
                    </div>
                ` : ''}
            </div>
            `;
        }).join('');
}

function updateCommentsStats(stats) {
    // 更新基本统计
    const totalCommentsEl = document.getElementById('total-comments');
    const totalLikesEl = document.getElementById('total-likes');
    const sentimentScoreEl = document.getElementById('sentiment-score');
    
    if (totalCommentsEl) totalCommentsEl.textContent = stats.total_comments;
    if (totalLikesEl) totalLikesEl.textContent = stats.total_likes;
    if (sentimentScoreEl) sentimentScoreEl.textContent = Math.round(stats.avg_sentiment * 100) + '%';
    
    // 更新情感图表
    const positiveCountEl = document.getElementById('positive-count');
    const neutralCountEl = document.getElementById('neutral-count');
    const negativeCountEl = document.getElementById('negative-count');
    
    if (positiveCountEl) positiveCountEl.textContent = stats.positive_count;
    if (neutralCountEl) neutralCountEl.textContent = stats.neutral_count;
    if (negativeCountEl) negativeCountEl.textContent = stats.negative_count;
}

function updatePaginationInfo(pagination) {
    const currentPageEl = document.getElementById('current-page');
    const totalPagesEl = document.getElementById('total-pages');
    
    if (currentPageEl) currentPageEl.textContent = pagination.page;
    if (totalPagesEl) totalPagesEl.textContent = pagination.pages;
    
    const prevBtn = document.querySelector('#comments-pagination .page-btn:first-child');
    const nextBtn = document.querySelector('#comments-pagination .page-btn:last-child');
    
    if (prevBtn) {
        prevBtn.disabled = pagination.page === 1;
    }
    if (nextBtn) {
        nextBtn.disabled = pagination.page === pagination.pages;
    }
}

async function loadCommentsPage(direction) {
    if (!currentSongIdForComments || !currentCommentsData) return;
    
    let newPage = commentsCurrentPage;
    
    if (direction === 'prev' && commentsCurrentPage > 1) {
        newPage = commentsCurrentPage - 1;
    } else if (direction === 'next' && commentsCurrentPage < commentsTotalPages) {
        newPage = commentsCurrentPage + 1;
    }
    
    if (newPage === commentsCurrentPage) return;
    
    commentsCurrentPage = newPage;
    
    try {
        const response = await fetch(
            `${API_BASE_URL}/songs/${currentSongIdForComments}/comments?page=${commentsCurrentPage}&sort_by=${commentsSortBy}&order=desc`
        );
        
        const data = await response.json();
        
        if (data.success) {
            currentCommentsData = data.data;
            displayComments(data.data);
        }
    } catch (error) {
        console.error('[评论] 分页加载失败:', error);
        showNotification('加载评论失败', 'error');
    }
}

function sortComments(sortType) {
    // 更新按钮状态
    document.querySelectorAll('.sort-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.dataset.sort === sortType) {
            btn.classList.add('active');
        }
    });
    
    commentsSortBy = sortType;
    commentsCurrentPage = 1;
    
    // 重新加载评论
    if (currentSongIdForComments) {
        loadSongComments(currentSongIdForComments);
    }
}

async function submitComment() {
    const textarea = document.getElementById('comment-textarea');
    const content = textarea.value.trim();
    const submitBtn = document.getElementById('submit-comment-btn');
    
    if (!content) {
        showNotification('请输入评论内容', 'warning');
        return;
    }
    
    if (!currentSongIdForComments) {
        showNotification('请先选择歌曲', 'warning');
        return;
    }
    
    // 获取当前用户
    const userId = currentUser || 'anonymous_' + Date.now();
    const nickname = currentUser ? `用户${currentUser}` : '匿名用户';
    
    // 禁用按钮防止重复提交
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 发布中...';
    
    try {
        const commentData = {
            content: content,
            user_id: userId,
            nickname: nickname
        };
        
        const response = await fetch(
            `${API_BASE_URL}/songs/${currentSongIdForComments}/comments`,
            {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(commentData)
            }
        );
        
        const data = await response.json();
        
        if (data.success) {
            // 清空输入框
            textarea.value = '';
            updateCharCount();
            
            // 重新加载评论
            await loadSongComments(currentSongIdForComments);
            
            showNotification('评论发布成功！', 'success');
        } else {
            throw new Error(data.message || '评论发布失败');
        }
    } catch (error) {
        console.error('[评论] 发布失败:', error);
        showNotification(`评论发布失败: ${error.message}`, 'error');
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = '<i class="fas fa-paper-plane"></i> 发布评论';
    }
}

async function likeComment(commentId, button) {
    if (!currentUser) {
        showNotification('请先登录', 'info');
        return;
    }
    
    const isLiked = button.classList.contains('liked');
    const action = isLiked ? 'cancel' : 'like';
    
    try {
        const response = await fetch(
            `${API_BASE_URL}/comments/${commentId}/like`,
            {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ action: action, user_id: currentUser })
            }
        );
        
        const data = await response.json();
        
        if (data.success) {
            // 根据后端返回的 user_has_liked 设置状态
            const userHasLiked = data.data.user_has_liked;
            if (userHasLiked) {
                button.classList.add('liked');
                button.querySelector('i').className = 'fas fa-heart';
            } else {
                button.classList.remove('liked');
                button.querySelector('i').className = 'far fa-heart'; // 使用空心图标表示未点赞
            }
            const countSpan = button.querySelector('.like-count');
            if (countSpan) {
                countSpan.textContent = data.data.liked_count;
            }
            // 动画效果
            button.querySelector('i').style.animation = 'heartBeat 0.3s';
            setTimeout(() => {
                button.querySelector('i').style.animation = '';
            }, 300);
        }
    } catch (error) {
        console.error('[评论] 点赞失败:', error);
        showNotification('操作失败，请重试', 'error');
    }
}

// 字符计数
function initCommentCharCount() {
    const textarea = document.getElementById('comment-textarea');
    if (!textarea) return;
    
    textarea.addEventListener('input', updateCharCount);
    textarea.addEventListener('focus', updateCharCount);
    
    // 初始更新
    updateCharCount();
}

function updateCharCount() {
    const textarea = document.getElementById('comment-textarea');
    const charCount = document.getElementById('char-remaining');
    const countDiv = document.querySelector('.char-count');
    
    if (!textarea || !charCount) return;
    
    const currentLength = textarea.value.length;
    const maxLength = 1000;
    const remaining = maxLength - currentLength;
    
    charCount.textContent = remaining;
    
    // 更新样式
    if (countDiv) {
        countDiv.classList.remove('warning', 'error');
        
        if (remaining <= 100 && remaining > 20) {
            countDiv.classList.add('warning');
        } else if (remaining <= 20) {
            countDiv.classList.add('error');
        }
    }
}

// 辅助函数
function formatCommentTime(timestamp) {
    const now = new Date();
    const commentTime = new Date(timestamp);
    const diffMs = now - commentTime;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return '刚刚';
    if (diffMins < 60) return `${diffMins}分钟前`;
    if (diffHours < 24) return `${diffHours}小时前`;
    if (diffDays < 7) return `${diffDays}天前`;
    
    return commentTime.toLocaleDateString('zh-CN');
}

function getSentimentClass(score) {
    if (score === null || score === undefined || score === 0) return 'sentiment-neutral';
    if (score > 0.6) return 'sentiment-positive';
    if (score < 0.4) return 'sentiment-negative';
    return 'sentiment-neutral';
}

function getSentimentText(score) {
    if (score === null || score === undefined || score === 0) return '中性';
    if (score > 0.6) return `正面 (${Math.round(score * 100)}%)`;
    if (score < 0.4) return `负面 (${Math.round(score * 100)}%)`;
    return `中性 (${Math.round(score * 100)}%)`;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showCommentsError() {
    const commentsList = document.getElementById('comments-list');
    if (!commentsList) return;
    
    commentsList.innerHTML = `
        <div class="comments-empty">
            <i class="fas fa-exclamation-circle"></i>
            <p>评论加载失败</p>
            <button class="btn btn-primary" onclick="loadSongComments('${currentSongIdForComments}')" 
                    style="margin-top: 1rem; padding: 0.5rem 1rem;">
                重试
            </button>
        </div>
    `;
}

// 清理评论状态（当模态框关闭时）
function clearCommentsState() {
    const textarea = document.getElementById('comment-textarea');
    if (textarea) textarea.value = '';
    
    const commentsList = document.getElementById('comments-list');
    if (commentsList) {
        commentsList.innerHTML = `
            <div class="comments-empty">
                <i class="fas fa-comment-slash"></i>
                <p>暂无评论，快来发表第一条评论吧！</p>
            </div>
        `;
    }
    
    updateCharCount();
}

// 为模态框关闭按钮添加清理功能
document.addEventListener('DOMContentLoaded', function() {
    const closeButtons = document.querySelectorAll('.close-modal');
    closeButtons.forEach(button => {
        button.addEventListener('click', function() {
            clearCommentsState();
        });
    });
    
    // 点击模态框背景关闭时也清理
    const modals = document.querySelectorAll('.modal');
    modals.forEach(modal => {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                clearCommentsState();
            }
        });
    });
});