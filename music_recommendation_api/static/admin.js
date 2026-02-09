// ==================== 配置区域 ====================
const API_BASE_URL = "http://127.0.0.1:5000/api/v1";
let adminToken = localStorage.getItem('admin_token');
let currentPage = { songs: 1, users: 1 };
let totalPages = { songs: 1, users: 1 };
let songsData = [];
let usersData = [];
let sortConfig = { field: 'song_id', direction: 'asc' };
let weights = { itemcf: 35, usercf: 25, content: 25, mf: 15 };
let chartInstances = {};
let isABTestEnabled = localStorage.getItem('ab_test_enabled') === 'true';

// ==================== 评论管理相关变量 ====================
let currentSongComments = [];
let currentCommentsPage = 1;
let commentsTotalPages = 1;

// ==================== 初始化 ====================
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM Loaded, initializing...');
    
    if (adminToken) {
        showDashboard();
        initDashboard();
    } else {
        showLogin();
    }
    
    // 登录表单绑定
    const loginForm = document.getElementById('admin-login-form');
    if (loginForm) {
        loginForm.addEventListener('submit', handleLogin);
    }
    
    // 实时时间
    setInterval(() => {
        const now = new Date();
        const timeEl = document.getElementById('current-time');
        if (timeEl) {
            timeEl.textContent = now.toLocaleString('zh-CN', { hour12: false });
        }
    }, 1000);

    // 修复：用户活跃度筛选绑定 - 使用更可靠的选择器
    const activityFilter = document.getElementById('user-activity-filter');
    if (activityFilter) {
        console.log('Binding activity filter change event');
        activityFilter.addEventListener('change', function(e) {
            console.log('Activity filter changed to:', e.target.value);
            loadUsersList(1);
        });
    } else {
        console.error('Activity filter element not found!');
    }

    // 歌曲搜索回车键
    const songSearchInput = document.getElementById('song-search-input');
    if (songSearchInput) {
        songSearchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') searchSongs();
        });
    }

    // 用户搜索回车键
    const userSearchInput = document.getElementById('user-search-input');
    if (userSearchInput) {
        userSearchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') searchUsers();
        });
    }

    // 点击弹窗外部关闭
    document.querySelectorAll('.modal-overlay').forEach(modal => {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.remove('active');
            }
        });
    });

    // 键盘事件
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal-overlay.active').forEach(modal => {
                modal.classList.remove('active');
            });
        }
    });
    
    // 初始更新一次权重显示
    updateWeights();
    syncABTestIndicator();
    setupAlgorithmSelectors();
    loadSystemConfig();

    // 【修改】延迟加载系统配置
    setTimeout(() => {
        loadSystemConfig();
    }, 1000);
});

// ==================== 页面切换 ====================
function switchPage(page, element) {
    // 更新导航样式
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));
    if (element) element.classList.add('active');
    
    // 切换页面内容
    document.querySelectorAll('.page-content').forEach(p => p.classList.remove('active'));
    const targetPage = document.getElementById(`page-${page}`);
    if (targetPage) {
        targetPage.classList.add('active');
    } else {
        console.error(`目标页面 #page-${page} 未找到`);
    }
    
    // 更新标题
    const titles = {
        dashboard: '概览面板', 
        songs: '歌曲管理', 
        users: '用户管理', 
        config: '系统配置'
    };
    const titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.textContent = titles[page] || page;
    
    // 延迟加载数据，确保DOM完全渲染
    setTimeout(() => {
        if (page === 'songs') {
            console.log('切换到歌曲管理页面');
            loadSongsList(1);
        }
        if (page === 'users') {
            console.log('切换到用户管理页面');
            loadUsersList(1);
        }
        if (page === 'config') {
            console.log('切换到系统配置页面');
            loadSystemConfig();
        }
    }, 100);
}

// ==================== 登录/退出 ====================
async function handleLogin(e) {
    e.preventDefault();
    const username = document.getElementById('admin-username').value;
    const password = document.getElementById('admin-password').value;
    const loginText = document.getElementById('login-text');
    
    loginText.innerHTML = '<span class="loading"></span> 登录中...';
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/login`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({username, password})
        });
        const data = await res.json();
        
        if (data.success) {
            adminToken = data.data.token;
            localStorage.setItem('admin_token', adminToken);
            localStorage.setItem('admin_name', data.data.admin_name);
            showDashboard();
            initDashboard();
            showNotification('登录成功', 'success');
        } else {
            showNotification(data.message || '登录失败', 'error');
            loginText.textContent = '登录';
        }
    } catch (err) {
        showNotification('网络错误：' + err.message, 'error');
        loginText.textContent = '登录';
    }
}

function logout() {
    localStorage.removeItem('admin_token');
    localStorage.removeItem('admin_name');
    location.reload();
}

function showLogin() {
    document.getElementById('login-page').classList.remove('hidden');
    document.getElementById('admin-dashboard').classList.add('hidden');
}

function showDashboard() {
    document.getElementById('login-page').classList.add('hidden');
    document.getElementById('admin-dashboard').classList.remove('hidden');
    const adminName = localStorage.getItem('admin_name') || '管理员';
    document.getElementById('admin-name').textContent = adminName;
}

// ==================== Dashboard 数据 ====================
// 在 initDashboard 中添加新图表加载
async function initDashboard() {
    console.log('初始化Dashboard...');
    
    // 加载基础统计数据
    await loadDashboardData();
    
    // 加载高级统计数据
    await loadAdvancedStats();
    
    // 初始化图表
    initCharts();
    initAdvancedCharts();
    
    // 恢复配置状态
    loadSystemConfig();
    
    console.log('Dashboard初始化完成');
}

// 加载高级统计数据
async function loadAdvancedStats() {
    try {
        const res = await fetch(`${API_BASE_URL}/admin/dashboard/advanced-stats`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        const result = await res.json();
        
        if (result.success) {
            window.advancedStats = result.data;
        }
    } catch (err) {
        console.error('加载高级统计失败:', err);
    }
}

// 修改 initAdvancedCharts 函数，避免重复声明
function initAdvancedCharts() {
    if (!window.advancedStats) {
        console.warn('高级统计数据未加载');
        return;
    }
    
    const data = window.advancedStats;
    
    // 销毁旧实例（避免内存泄漏）
    ['activity', 'gender', 'age', 'behavior', 'algoUsage', 'audio', 'yearDist', 'ctr', 'provinceMap'].forEach(name => {
        if (chartInstances[name]) {
            chartInstances[name].dispose();
        }
    });
    
    // ========== 1. 活跃度分布饼图 ==========
    if (document.getElementById('chart-activity-dist')) {
        chartInstances.activity = echarts.init(document.getElementById('chart-activity-dist'));
        const activityChart = chartInstances.activity;
        
        const activityData = data.activity_distribution || [];
        activityChart.setOption({
            tooltip: { 
                trigger: 'item', 
                formatter: '{b}: {c} ({d}%)',
                backgroundColor: 'rgba(255,255,255,0.9)',
                borderColor: '#eee',
                borderWidth: 1
            },
            legend: { 
                bottom: 0,
                itemWidth: 12,
                itemHeight: 12,
                textStyle: { fontSize: 11 }
            },
            color: ['#4361ee', '#06d6a0', '#ffd166', '#ef476f', '#7209b7', '#4cc9f0'],
            series: [{
                type: 'pie',
                radius: ['35%', '65%'],
                center: ['50%', '45%'],
                avoidLabelOverlap: true,
                itemStyle: { 
                    borderRadius: 10, 
                    borderColor: '#fff', 
                    borderWidth: 2 
                },
                label: { 
                    show: true,
                    formatter: '{b}\n{d}%',
                    fontSize: 11
                },
                emphasis: {
                    label: { 
                        show: true, 
                        fontSize: 14, 
                        fontWeight: 'bold' 
                    },
                    itemStyle: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                },
                data: activityData
            }]
        });
    }
    
    // ========== 2. 性别分布饼图 ==========
    if (document.getElementById('chart-gender-dist')) {
        chartInstances.gender = echarts.init(document.getElementById('chart-gender-dist'));
        const genderChart = chartInstances.gender;
        
        const genderData = data.gender_distribution || [];
        genderChart.setOption({
            tooltip: { 
                trigger: 'item',
                formatter: '{b}: {c} ({d}%)'
            },
            color: ['#4361ee', '#f72585', '#7209b7'],
            legend: {
                bottom: 0,
                data: genderData.map(d => d.name)
            },
            series: [{
                type: 'pie',
                radius: '60%',
                center: ['50%', '45%'],
                data: genderData,
                label: {
                    formatter: '{b}\n{d}%',
                    fontSize: 12
                },
                emphasis: {
                    itemStyle: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                }
            }]
        });
    }
    
    // ========== 3. 年龄段分布柱状图 ==========
    if (document.getElementById('chart-age-dist')) {
        chartInstances.age = echarts.init(document.getElementById('chart-age-dist'));
        const ageChart = chartInstances.age;
        
        const ageData = data.age_distribution || [];
        ageChart.setOption({
            tooltip: { 
                trigger: 'axis',
                axisPointer: { type: 'shadow' }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                top: '10%',
                containLabel: true
            },
            xAxis: { 
                type: 'category', 
                data: ageData.map(d => d.name),
                axisLabel: { 
                    interval: 0, 
                    rotate: 30,
                    fontSize: 11
                },
                axisTick: {
                    alignWithLabel: true
                }
            },
            yAxis: { 
                type: 'value',
                name: '用户数',
                axisLine: { show: true },
                splitLine: {
                    lineStyle: {
                        type: 'dashed',
                        color: 'rgba(0,0,0,0.1)'
                    }
                }
            },
            series: [{
                data: ageData.map(d => d.value),
                type: 'bar',
                barWidth: '60%',
                itemStyle: { 
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: '#4361ee' },
                        { offset: 1, color: '#7209b7' }
                    ]),
                    borderRadius: [4, 4, 0, 0]
                },
                label: {
                    show: true,
                    position: 'top',
                    formatter: '{c}',
                    fontSize: 10
                }
            }]
        });
    }
    
    // ========== 4. 交互行为统计 ==========
    if (document.getElementById('chart-behavior')) {
        chartInstances.behavior = echarts.init(document.getElementById('chart-behavior'));
        const behaviorChart = chartInstances.behavior;
        
        const behaviorData = data.behavior_stats || [];
        behaviorChart.setOption({
            tooltip: { 
                trigger: 'item',
                formatter: '{b}: {c}次 ({d}%)'
            },
            legend: {
                bottom: 0,
                data: behaviorData.map(d => d.name)
            },
            color: ['#4361ee', '#06d6a0', '#ffd166', '#ef476f', '#7209b7'],
            series: [{
                type: 'pie',
                radius: ['35%', '65%'],
                center: ['50%', '45%'],
                data: behaviorData,
                label: { 
                    formatter: '{b}\n{d}%',
                    fontSize: 11
                },
                emphasis: {
                    itemStyle: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                }
            }]
        });
    }
    
    // ========== 5. 算法总使用情况（玫瑰图）- 【新增】==========
    if (document.getElementById('chart-algorithm-usage')) {
        chartInstances.algoUsage = echarts.init(document.getElementById('chart-algorithm-usage'));
        const algoChart = chartInstances.algoUsage;
        
        // 使用总使用情况数据（如果后端提供了）或模拟数据
        let algorithmTotalUsage = data.algorithm_total_usage || [];
        // 如果没有数据，显示提示
        if (algorithmTotalUsage.length === 0) {
            // 显示无数据提示
            algoChart.setOption({
                title: {
                    text: '暂无算法使用数据',
                    subtext: '请确保推荐系统已生成推荐记录',
                    left: 'center',
                    top: 'center',
                    textStyle: {
                        color: '#999',
                        fontSize: 16
                    }
                }
            });
            return;
        }
        
        // 计算总推荐次数
        const total = algorithmTotalUsage.reduce((sum, item) => sum + item.value, 0);
        
        algoChart.setOption({
            tooltip: {
                trigger: 'item',
                formatter: function(params) {
                    const percent = ((params.value / total) * 100).toFixed(1);
                    return `${params.name}<br/>使用次数: ${params.value.toLocaleString()}<br/>占比: ${percent}%`;
                }
            },
            legend: {
                type: 'scroll',
                orient: 'vertical',
                right: 10,
                top: 30,
                bottom: 20,
                textStyle: {
                    fontSize: 12
                }
            },
            series: [{
                name: '算法总使用',
                type: 'pie',
                radius: ['15%', '80%'],
                center: ['35%', '55%'],
                roseType: 'area',
                itemStyle: {
                    borderRadius: 8,
                    borderColor: '#fff',
                    borderWidth: 2
                },
                label: {
                    show: false
                },
                emphasis: {
                    label: {
                        show: true,
                        fontSize: 14,
                        fontWeight: 'bold'
                    }
                },
                data: algorithmTotalUsage
            }]
        });
    }
    
    // ========== 6. 音频特征平均值 ==========
    if (document.getElementById('chart-audio-features')) {
        chartInstances.audio = echarts.init(document.getElementById('chart-audio-features'));
        const audioChart = chartInstances.audio;
        
        const avg = data.audio_features_avg || {};
        
        // 构建雷达图指标
        const radarIndicators = [
            { name: '舞曲性\nDanceability', max: 1 },
            { name: '能量感\nEnergy', max: 1 },
            { name: '情绪值\nValence', max: 1 },
            { name: '原声度\nAcousticness', max: 1 },
            { name: '节奏速度\nTempo', max: 200 }
        ];
        
        const radarValues = [
            avg.danceability || 0,
            avg.energy || 0,
            avg.valence || 0,
            avg.acousticness || 0,
            (avg.tempo || 120)
        ];
        
        audioChart.setOption({
            tooltip: {
                formatter: function(params) {
                    const labels = ['舞曲性', '能量感', '情绪值', '原声度', '节奏速度'];
                    const values = params.value;
                    let html = '<div style="font-weight:bold;margin-bottom:5px;">音频特征平均值</div>';
                    labels.forEach((label, i) => {
                        const val = values[i];
                        const displayVal = i === 4 ? Math.round(val) + ' BPM' : val.toFixed(3);
                        html += `<div>${label}: ${displayVal}</div>`;
                    });
                    return html;
                }
            },
            radar: {
                indicator: radarIndicators,
                shape: 'polygon',
                splitNumber: 4,
                axisName: {
                    color: '#666',
                    fontSize: 10,
                    lineHeight: 14
                },
                splitLine: {
                    lineStyle: {
                        color: 'rgba(67, 97, 238, 0.2)'
                    }
                },
                splitArea: {
                    show: true,
                    areaStyle: {
                        color: ['rgba(67, 97, 238, 0.05)', 'rgba(67, 97, 238, 0.1)']
                    }
                },
                axisLine: {
                    lineStyle: {
                        color: 'rgba(67, 97, 238, 0.3)'
                    }
                }
            },
            series: [{
                type: 'radar',
                data: [{
                    value: radarValues,
                    name: '平均值',
                    areaStyle: { 
                        color: 'rgba(67, 97, 238, 0.3)' 
                    },
                    lineStyle: { 
                        color: '#4361ee',
                        width: 2
                    },
                    itemStyle: { 
                        color: '#4361ee',
                        borderWidth: 2,
                        borderColor: '#fff'
                    },
                    label: {
                        show: true,
                        formatter: function(params) {
                            if (params.dimensionIndex < 4) {
                                return params.value.toFixed(2);
                            }
                            return '';
                        },
                        fontSize: 10,
                        color: '#4361ee'
                    }
                }]
            }]
        });
    }
    
    // ========== 7. 发行年份分布柱状图 ==========
    if (document.getElementById('chart-year-dist')) {
        chartInstances.yearDist = echarts.init(document.getElementById('chart-year-dist'));
        const yearChart = chartInstances.yearDist;
        
        const yearData = data.year_distribution || [];
        
        if (yearData.length > 0) {
            yearChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    formatter: '{b}年<br/>歌曲数: {c}首',
                    backgroundColor: 'rgba(255,255,255,0.9)',
                    borderColor: '#eee',
                    borderWidth: 1
                },
                grid: { 
                    left: '3%', 
                    right: '4%', 
                    bottom: '15%', 
                    top: '15%',
                    containLabel: true 
                },
                xAxis: {
                    type: 'category',
                    data: yearData.map(d => d.year),
                    axisLabel: { 
                        rotate: 45,
                        fontSize: 11,
                        interval: yearData.length > 15 ? 'auto' : 0
                    },
                    axisTick: {
                        alignWithLabel: true
                    }
                },
                yAxis: { 
                    type: 'value',
                    name: '歌曲数量',
                    nameTextStyle: {
                        padding: [0, 0, 0, 30]
                    },
                    axisLine: { show: true },
                    splitLine: {
                        lineStyle: {
                            type: 'dashed',
                            color: 'rgba(0,0,0,0.1)'
                        }
                    }
                },
                dataZoom: yearData.length > 12 ? [{
                    type: 'inside',
                    start: Math.max(0, 100 - Math.floor(12 / yearData.length * 100)),
                    end: 100
                }, {
                    type: 'slider',
                    show: true,
                    bottom: 0,
                    height: 20,
                    start: Math.max(0, 100 - Math.floor(12 / yearData.length * 100)),
                    end: 100
                }] : undefined,
                series: [{
                    data: yearData.map(d => d.count),
                    type: 'bar',
                    barWidth: '60%',
                    itemStyle: {
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: '#06d6a0' },
                            { offset: 1, color: '#118ab2' }
                        ]),
                        borderRadius: [4, 4, 0, 0]
                    },
                    label: {
                        show: true,
                        position: 'top',
                        formatter: '{c}',
                        fontSize: 10,
                        color: '#666'
                    },
                    emphasis: {
                        itemStyle: {
                            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                { offset: 0, color: '#ffd166' },
                                { offset: 1, color: '#ef476f' }
                            ])
                        }
                    }
                }]
            });
        } else {
            yearChart.setOption({
                title: {
                    text: '暂无年份数据\n请检查 publish_year 字段',
                    left: 'center',
                    top: 'center',
                    textStyle: {
                        color: '#999',
                        fontSize: 14,
                        lineHeight: 20
                    }
                },
                graphic: [{
                    type: 'text',
                    left: 'center',
                    top: '60%',
                    style: {
                        text: 'SQL: SELECT publish_year FROM enhanced_song_features',
                        fill: '#ccc',
                        fontSize: 11
                    }
                }]
            });
        }
    }
    
    // ========== 8. 每日CTR趋势 ==========
    if (document.getElementById('chart-daily-ctr')) {
        chartInstances.ctr = echarts.init(document.getElementById('chart-daily-ctr'));
        const ctrChart = chartInstances.ctr;
        
        const ctrData = data.daily_ctr || [];
        ctrChart.setOption({
            tooltip: { 
                trigger: 'axis', 
                formatter: function(params) {
                    const dataPoint = ctrData[params[0].dataIndex];
                    return `${params[0].name}<br/>
                    点击率: ${params[0].value}%<br/>
                    点击次数: ${dataPoint?.clicks || 0}<br/>
                    推荐次数: ${dataPoint?.total || 0}`;
                },
                backgroundColor: 'rgba(255,255,255,0.9)',
                borderColor: '#eee',
                borderWidth: 1
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                top: '15%',
                containLabel: true
            },
            xAxis: { 
                type: 'category', 
                data: ctrData.map(d => d.date),
                axisLabel: { fontSize: 11 }
            },
            yAxis: { 
                type: 'value',
                name: '点击率(%)',
                axisLabel: { formatter: '{value}%' },
                splitLine: {
                    lineStyle: {
                        type: 'dashed',
                        color: 'rgba(0,0,0,0.1)'
                    }
                }
            },
            series: [{
                data: ctrData.map(d => d.ctr),
                type: 'line',
                smooth: true,
                symbol: 'circle',
                symbolSize: 8,
                lineStyle: { 
                    color: '#06d6a0', 
                    width: 3 
                },
                areaStyle: { 
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(6, 214, 160, 0.3)' },
                        { offset: 1, color: 'rgba(6, 214, 160, 0.05)' }
                    ])
                },
                itemStyle: { 
                    color: '#06d6a0',
                    borderWidth: 2,
                    borderColor: '#fff'
                },
                label: { 
                    show: true, 
                    formatter: '{c}%', 
                    position: 'top',
                    fontSize: 10,
                    color: '#06d6a0'
                }
            }]
        });
    }
    
    // ========== 9. 用户地理分布柱状图（保留原有省份处理）==========
    const mapContainer = document.getElementById('chart-province-map');
    if (mapContainer) {
        chartInstances.provinceMap = echarts.init(mapContainer);
        const provinceChart = chartInstances.provinceMap;
        
        const provinceData = data.province_distribution || [];
        
        if (provinceData.length > 0) {
            // 按用户数排序，取前15个省份
            const sortedData = provinceData.sort((a, b) => b.value - a.value).slice(0, 15);
            
            provinceChart.setOption({
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'shadow'
                    },
                    formatter: '{b}: {c}人'
                },
                grid: {
                    left: '3%',
                    right: '4%',
                    bottom: '3%',
                    top: '15%',
                    containLabel: true
                },
                xAxis: {
                    type: 'category',
                    data: sortedData.map(d => d.name),
                    axisLabel: {
                        rotate: 45,
                        fontSize: 11
                    },
                    axisTick: {
                        alignWithLabel: true
                    }
                },
                yAxis: {
                    type: 'value',
                    name: '用户数',
                    axisLine: { show: true },
                    splitLine: {
                        lineStyle: {
                            type: 'dashed',
                            color: 'rgba(0,0,0,0.1)'
                        }
                    }
                },
                series: [{
                    data: sortedData.map(d => d.value),
                    type: 'bar',
                    barWidth: '60%',
                    itemStyle: {
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: '#4361ee' },
                            { offset: 1, color: '#7209b7' }
                        ]),
                        borderRadius: [4, 4, 0, 0]
                    },
                    label: {
                        show: true,
                        position: 'top',
                        formatter: '{c}',
                        fontSize: 10
                    },
                    emphasis: {
                        itemStyle: {
                            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                { offset: 0, color: '#06d6a0' },
                                { offset: 1, color: '#118ab2' }
                            ])
                        }
                    }
                }]
            });
        } else {
            provinceChart.setOption({
                title: {
                    text: '暂无省份数据',
                    subtext: '用户省份信息未收集',
                    left: 'center',
                    top: 'center',
                    textStyle: { color: '#999' }
                }
            });
        }
    }
    // ========== 10. 用户听歌数目分布饼图 ==========
    if (document.getElementById('chart-song-count-dist')) {
        chartInstances.songCountDist = echarts.init(document.getElementById('chart-song-count-dist'));
        const songCountChart = chartInstances.songCountDist;
        
        const songCountData = data.song_count_distribution || [];
        
        songCountChart.setOption({
            tooltip: { 
                trigger: 'item', 
                formatter: '{b}: {c}人 ({d}%)',
                backgroundColor: 'rgba(255,255,255,0.9)'
            },
            legend: { 
                bottom: 0,
                textStyle: { fontSize: 11 }
            },
            color: ['#4361ee', '#06d6a0', '#ffd166', '#ef476f', '#7209b7', '#4cc9f0'],
            series: [{
                type: 'pie',
                radius: ['35%', '65%'],
                center: ['50%', '45%'],
                avoidLabelOverlap: true,
                itemStyle: { 
                    borderRadius: 10, 
                    borderColor: '#fff', 
                    borderWidth: 2 
                },
                label: { 
                    show: true,
                    formatter: '{b}\n{d}%',
                    fontSize: 11,
                    lineHeight: 14
                },
                emphasis: {
                    label: { 
                        show: true, 
                        fontSize: 14, 
                        fontWeight: 'bold' 
                    },
                    itemStyle: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                },
                data: songCountData
            }]
        });
    }

    // ========== 11. 歌曲出版年份分布柱状图 ==========
    if (document.getElementById('chart-year-dist')) {
        chartInstances.yearDist = echarts.init(document.getElementById('chart-year-dist'));
        const yearChart = chartInstances.yearDist;
        
        const yearData = data.year_distribution || [];
        
        if (yearData.length > 0) {
            yearChart.setOption({
                tooltip: { 
                    trigger: 'axis',
                    formatter: '{b}年<br/>歌曲数: {c}首',
                    backgroundColor: 'rgba(255,255,255,0.9)',
                    borderColor: '#eee',
                    borderWidth: 1
                },
                grid: { 
                    left: '3%', 
                    right: '4%', 
                    bottom: '15%', 
                    top: '15%',
                    containLabel: true 
                },
                xAxis: {
                    type: 'category',
                    data: yearData.map(d => d.year),
                    axisLabel: { 
                        rotate: 45,
                        fontSize: 11,
                        interval: yearData.length > 15 ? 'auto' : 0
                    },
                    axisTick: {
                        alignWithLabel: true
                    }
                },
                yAxis: { 
                    type: 'value',
                    name: '歌曲数量',
                    nameTextStyle: {
                        padding: [0, 0, 0, 30]
                    },
                    axisLine: { show: true },
                    splitLine: {
                        lineStyle: {
                            type: 'dashed',
                            color: 'rgba(0,0,0,0.1)'
                        }
                    }
                },
                dataZoom: yearData.length > 12 ? [{
                    type: 'inside',
                    start: Math.max(0, 100 - Math.floor(12 / yearData.length * 100)),
                    end: 100
                }, {
                    type: 'slider',
                    show: true,
                    bottom: 0,
                    height: 20,
                    start: Math.max(0, 100 - Math.floor(12 / yearData.length * 100)),
                    end: 100
                }] : undefined,
                series: [{
                    data: yearData.map(d => d.count),
                    type: 'bar',
                    barWidth: '60%',
                    itemStyle: {
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: '#06d6a0' },
                            { offset: 1, color: '#118ab2' }
                        ]),
                        borderRadius: [4, 4, 0, 0]
                    },
                    label: {
                        show: true,
                        position: 'top',
                        formatter: '{c}',
                        fontSize: 10,
                        color: '#666'
                    },
                    emphasis: {
                        itemStyle: {
                            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                { offset: 0, color: '#ffd166' },
                                { offset: 1, color: '#ef476f' }
                            ])
                        }
                    }
                }]
            });
        } else {
            yearChart.setOption({
                title: {
                    text: '暂无年份数据\n请检查 publish_year 字段',
                    left: 'center',
                    top: 'center',
                    textStyle: {
                        color: '#999',
                        fontSize: 14,
                        lineHeight: 20
                    }
                }
            });
        }
    }

    // ========== 12. 最受好评歌曲水平条形图 ==========
    if (document.getElementById('chart-top-rated-songs')) {
        chartInstances.topRatedSongs = echarts.init(document.getElementById('chart-top-rated-songs'));
        const topRatedChart = chartInstances.topRatedSongs;
        
        const topRatedData = data.top_rated_songs || [];
        
        if (topRatedData.length > 0) {
            // 取前10名并反转顺序（从高到低显示）
            const displayData = topRatedData.slice(0, 10).reverse();
            
            topRatedChart.setOption({
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'shadow'
                    },
                    formatter: function(params) {
                        const dataItem = displayData[params[0].dataIndex];
                        return `
                        <strong>${dataItem.song_name}</strong><br/>
                        艺术家: ${dataItem.artists}<br/>
                        平均情感值: ${dataItem.avg_sentiment.toFixed(3)}<br/>
                        评论数量: ${dataItem.comment_count}条
                        `;
                    },
                    backgroundColor: 'rgba(255,255,255,0.95)',
                    borderColor: '#ddd',
                    borderWidth: 1,
                    textStyle: {
                        color: '#333'
                    }
                },
                grid: { 
                    left: '3%', 
                    right: '8%', 
                    bottom: '3%', 
                    top: '10%',
                    containLabel: true 
                },
                xAxis: {
                    type: 'value',
                    name: '情感值',
                    min: 0,
                    max: 1,
                    axisLabel: {
                        formatter: function(value) {
                            return value.toFixed(2);
                        }
                    },
                    splitLine: {
                        lineStyle: {
                            color: '#f0f0f0'
                        }
                    }
                },
                yAxis: {
                    type: 'category',
                    data: displayData.map(d => d.display_name),
                    axisLabel: {
                        fontSize: 11,
                        width: 120,
                        overflow: 'truncate'
                    }
                },
                series: [{
                    type: 'bar',
                    data: displayData.map(d => d.avg_sentiment),
                    barWidth: '60%',
                    itemStyle: {
                        borderRadius: [0, 4, 4, 0],
                        color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                            { offset: 0, color: '#ffd166' },
                            { offset: 1, color: '#ef476f' }
                        ])
                    },
                    label: {
                        show: true,
                        position: 'right',
                        formatter: function(params) {
                            return params.value.toFixed(3);
                        },
                        fontSize: 10,
                        color: '#666'
                    }
                }]
            });
        } else {
            topRatedChart.setOption({
                title: {
                    text: '暂无评论情感数据',
                    subtext: '需要先有歌曲评论并计算情感值',
                    left: 'center',
                    top: 'center',
                    textStyle: {
                        color: '#999',
                        fontSize: 14
                    }
                }
            });
        }
    }
}

async function loadDashboardData() {
    try {
        const res = await fetch(`${API_BASE_URL}/admin/dashboard/stats`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        const result = await res.json();
        
        if (!result.success) {
            showNotification('获取统计数据失败', 'error');
            return;
        }
        
        const data = result.data;
        document.getElementById('stat-total-users').textContent = data.total_users.toLocaleString();
        document.getElementById('stat-total-songs').textContent = data.total_songs.toLocaleString();
        document.getElementById('stat-plays-today').textContent = (data.plays_today || 0).toLocaleString();
        
        // 【修复2】使用真实推荐成功率
        const successRate = data.success_rate || 87.0;
        document.getElementById('stat-success-rate').textContent = successRate.toFixed(1) + '%';
        
        // 根据成功率显示不同状态
        const rateTrend = document.getElementById('rate-trend');
        if (rateTrend) {
            if (successRate >= 85) {
                rateTrend.innerHTML = '<i class="fas fa-check-circle"></i> 运行正常';
                rateTrend.style.color = 'var(--success-color)';
            } else if (successRate >= 70) {
                rateTrend.innerHTML = '<i class="fas fa-exclamation-triangle"></i> 一般';
                rateTrend.style.color = 'var(--warning-color)';
            } else {
                rateTrend.innerHTML = '<i class="fas fa-exclamation-circle"></i> 待优化';
                rateTrend.style.color = 'var(--danger-color)';
            }
        }
        
        window.dashboardData = data;
    } catch (err) {
        console.error('加载Dashboard失败:', err);
        showNotification('加载统计数据失败', 'error');
    }
}

function normalizeGenreDistribution(rawData) {
    const GENRE_NORMALIZATION = {
        // 电子类
        'Electronic': '电子',
        '电子': '电子',
        
        // 流行类
        'Pop': '流行',
        '华语流行': '流行',
        '欧美流行': '流行',
        '日本流行': '流行',
        'K-Pop': '流行',
        '翻唱': '流行',
        
        // 摇滚类
        'Rock': '摇滚',
        '摇滚': '摇滚',
        'Punk': '摇滚',
        'Metal': '摇滚',
        
        // 民谣类
        'Folk': '民谣',
        '民谣': '民谣',
        'Country': '民谣',
        
        // 说唱类
        'Rap': '说唱',
        '说唱': '说唱',
        'RnB': '说唱',
        
        // 爵士/蓝调
        'Jazz': '爵士蓝调',
        'Blues': '爵士蓝调',
        
        // 世界音乐
        'World': '世界音乐',
        'Latin': '世界音乐',
        'Reggae': '世界音乐',
        'New Age': '世界音乐',
        
        // 其他
        '影视原声': '影视原声',
        '现场': '现场录音'
    };
    
    const normalized = {};
    
    rawData.forEach(item => {
        const normalizedName = GENRE_NORMALIZATION[item.name] || item.name;
        if (!normalized[normalizedName]) {
            normalized[normalizedName] = 0;
        }
        normalized[normalizedName] += item.value;
    });
    
    return Object.entries(normalized)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => b.value - a.value);
}

// 转换省份代码为名称
function normalizeProvince(provinceCodeOrName) {
    if (!provinceCodeOrName) return '未知';
    
    // 如果是纯数字6位代码
    if (/^\d{6}$/.test(provinceCodeOrName)) {
        return PROVINCE_CODE_MAP[provinceCodeOrName] || '其他';
    }
    
    // 如果已经是中文，直接返回
    if (/[\u4e00-\u9fa5]/.test(provinceCodeOrName)) {
        return provinceCodeOrName;
    }
    
    return '其他';
}

async function initCharts() {
    if (!window.dashboardData) return;
    const data = window.dashboardData;
    
    // ========== 1. 流派分布饼图（修复：先声明变量）==========
    const genreChart = echarts.init(document.getElementById('chart-genres'));
    chartInstances.genre = genreChart;
    
    // 【关键修复】先归一化流派数据
    const rawGenreData = data.genre_distribution || [];
    const normalizedGenreData = normalizeGenreDistribution(rawGenreData);
    
    genreChart.setOption({
        tooltip: { 
            trigger: 'item', 
            formatter: '{b}: {c} ({d}%)',
            backgroundColor: 'rgba(255,255,255,0.9)'
        },
        legend: { 
            show: false 
        },
        series: [{
            type: 'pie',
            radius: ['40%', '70%'],
            center: ['50%', '50%'],
            avoidLabelOverlap: false,
            itemStyle: { 
                borderRadius: 10, 
                borderColor: '#fff', 
                borderWidth: 2 
            },
            label: { 
                show: false 
            },
            emphasis: {
                label: { 
                    show: true, 
                    fontSize: 16, 
                    fontWeight: 'bold', 
                    formatter: '{b}\n{c} ({d}%)' 
                }
            },
            data: normalizedGenreData  // 使用归一化后的数据
        }]
    });

    // ========== 2. 用户增长折线图 ==========
    const growthChart = echarts.init(document.getElementById('chart-growth'));
    chartInstances.growth = growthChart;
    
    const dates = (data.user_growth_7d || []).map(d => d.date.slice(5));
    const counts = (data.user_growth_7d || []).map(d => d.count);
    
    growthChart.setOption({
        tooltip: { 
            trigger: 'axis', 
            backgroundColor: 'rgba(255,255,255,0.9)', 
            formatter: '{b}<br/>新增用户: {c}人' 
        },
        grid: { 
            left: '3%', 
            right: '4%', 
            bottom: '3%', 
            containLabel: true 
        },
        xAxis: { 
            type: 'category', 
            boundaryGap: false, 
            data: dates 
        },
        yAxis: { 
            type: 'value', 
            splitLine: { 
                lineStyle: { 
                    color: '#f0f0f0' 
                } 
            } 
        },
        series: [{
            data: counts,
            type: 'line',
            smooth: true,
            symbol: 'circle',
            symbolSize: 8,
            lineStyle: { 
                color: '#4361ee', 
                width: 3 
            },
            areaStyle: { 
                color: 'rgba(67, 97, 238, 0.3)' 
            },
            itemStyle: { 
                color: '#4361ee' 
            }
        }]
    });

    // ========== 3. 热门歌曲 ==========
    const hotChart = echarts.init(document.getElementById('chart-hot-songs'));
    chartInstances.hot = hotChart;
    const hotSongs = (data.hot_songs_top10 || []).reverse();
    
    hotChart.setOption({
        tooltip: { 
            trigger: 'axis', 
            axisPointer: { 
                type: 'shadow' 
            }, 
            formatter: '{b}<br/>播放次数: {c}' 
        },
        grid: { 
            left: '3%', 
            right: '8%', 
            bottom: '3%', 
            containLabel: true 
        },
        xAxis: { 
            type: 'value', 
            splitLine: { 
                lineStyle: { 
                    color: '#f0f0f0' 
                } 
            } 
        },
        yAxis: { 
            type: 'category', 
            data: hotSongs.map(d => d.name.split(' - ')[0].substring(0, 12))
        },
        series: [{
            type: 'bar',
            data: hotSongs.map(d => d.value),
            barWidth: '60%',
            itemStyle: { 
                borderRadius: [0, 4, 4, 0], 
                color: '#4361ee' 
            },
            label: { 
                show: true, 
                position: 'right', 
                formatter: '{c}次' 
            }
        }]
    });

        // ========== 4. 算法对比雷达图 ==========
        if (document.getElementById('chart-algorithm-compare')) {
            const algoChart = echarts.init(document.getElementById('chart-algorithm-compare'));
            chartInstances.algo = algoChart;
            
            // 从后端获取真实算法性能数据
            fetch(`${API_BASE_URL}/admin/algorithm-performance`, {
                headers: {'Authorization': `Bearer ${adminToken}`}
            })
            .then(res => res.json())
            .then(result => {
                if (result.success && Object.keys(result.data).length > 0) {
                    const algorithmData = result.data;
                    
                    // 算法名称映射
                    const algorithmNames = {
                        'hybrid': '混合推荐',
                        'itemcf': 'ItemCF',
                        'usercf': 'UserCF',
                        'content': '内容推荐',
                        'mf': '矩阵分解',
                        'cf': '协同过滤'
                    };
                    
                    // 指标列表
                    const indicators = [
                        { name: '召回率', max: 100 },
                        { name: '准确率', max: 100 },
                        { name: '多样性', max: 100 },
                        { name: '点击率', max: 20 },
                        { name: '收听率', max: 20 }
                    ];
                    
                    // 构建数据系列
                    const seriesData = [];
                    const algorithms = ['hybrid', 'itemcf', 'usercf', 'content', 'mf'];
                    const colors = ['#4361ee', '#06d6a0', '#f72585', '#ffd166', '#7209b7'];
                    
                    algorithms.forEach((alg, index) => {
                        const data = algorithmData[alg];
                        if (data) {
                            const values = [
                                data['召回率'] || 0,
                                data['准确率'] || 0,
                                data['多样性'] || 0,
                                data['点击率'] || 0,
                                data['收听率'] || 0
                            ];
                            
                            seriesData.push({
                                value: values,
                                name: algorithmNames[alg] || alg,
                                itemStyle: {
                                    color: colors[index]
                                },
                                lineStyle: {
                                    color: colors[index],
                                    width: 2
                                }
                            });
                        }
                    });
                    
                    // 如果有数据，绘制图表
                    if (seriesData.length > 0) {
                        algoChart.setOption({
                            tooltip: {
                                trigger: 'item',
                                formatter: function(params) {
                                    let result = `${params.name}<br/>`;
                                    const values = params.value;
                                    indicators.forEach((indicator, i) => {
                                        const val = values[i];
                                        const unit = indicator.name === '点击率' || indicator.name === '收听率' ? '%' : '';
                                        result += `${indicator.name}: ${val}${unit}<br/>`;
                                    });
                                    return result;
                                }
                            },
                            legend: {
                                data: seriesData.map(s => s.name),
                                bottom: 0,
                                left: 'center'
                            },
                            radar: {
                                indicator: indicators,
                                shape: 'circle',
                                splitNumber: 5,
                                axisName: {
                                    color: '#666',
                                    fontSize: 11
                                },
                                splitLine: {
                                    lineStyle: {
                                        color: 'rgba(0,0,0,0.1)'
                                    }
                                },
                                splitArea: {
                                    show: true,
                                    areaStyle: {
                                        color: ['rgba(255,255,255,0.8)', 'rgba(255,255,255,0.6)']
                                    }
                                }
                            },
                            series: [{
                                type: 'radar',
                                data: seriesData,
                                symbol: 'circle',
                                symbolSize: 6,
                                lineStyle: {
                                    width: 2
                                },
                                areaStyle: {
                                    opacity: 0.1
                                },
                                emphasis: {
                                    lineStyle: {
                                        width: 3
                                    },
                                    areaStyle: {
                                        opacity: 0.2
                                    }
                                }
                            }]
                        });
                    } else {
                        // 没有数据，显示提示
                        algoChart.setOption({
                            title: {
                                text: '暂无算法性能数据',
                                subtext: '推荐系统需要运行一段时间才能生成性能数据',
                                left: 'center',
                                top: 'center',
                                textStyle: {
                                    color: '#999',
                                    fontSize: 16
                                }
                            }
                        });
                    }
                } else {
                    // 没有数据，显示提示
                    algoChart.setOption({
                        title: {
                            text: '暂无算法性能数据',
                            subtext: result.note || '请确保推荐系统已运行并生成数据',
                            left: 'center',
                            top: 'center',
                            textStyle: {
                                color: '#999',
                                fontSize: 16
                            }
                        }
                    });
                }
            })
            .catch(err => {
                console.error('加载算法性能对比失败:', err);
                // 显示错误提示
                algoChart.setOption({
                    title: {
                        text: '数据加载失败',
                        subtext: err.message || '请检查网络连接',
                        left: 'center',
                        top: 'center',
                        textStyle: {
                            color: '#999',
                            fontSize: 16
                        }
                    }
                });
            });
        }
}

function renderSongsTable() {
    const tbody = document.getElementById('songs-table-body');
    
    if (!songsData || songsData.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="empty-state">
                    <i class="fas fa-inbox"></i>
                    <p>暂无歌曲数据</p>
                </td>
            </tr>
        `;
        return;
    }
    
    tbody.innerHTML = songsData.map((song) => {
        // 获取流行度值，支持多种字段名
        let popValue = 50; // 默认值
        
        if (song.final_popularity !== undefined && song.final_popularity !== null) {
            popValue = parseFloat(song.final_popularity);
        } else if (song.popularity !== undefined && song.popularity !== null) {
            popValue = parseFloat(song.popularity);
        } else if (song.finalPopularity !== undefined && song.finalPopularity !== null) {
            popValue = parseFloat(song.finalPopularity);
        }
        
        // 确保是有效数字
        if (isNaN(popValue)) popValue = 50;
        
        return `
        <tr>
            <td><strong>${song.song_id || song.id || 'N/A'}</strong></td>
            <td>
                <div style="font-weight: 600;">${song.song_name || '未知'}</div>
                <div style="font-size: 0.85rem; color: var(--text-secondary);">${song.album || '未知专辑'}</div>
            </td>
            <td>${song.artists || '未知'}</td>
            <td><span class="genre-tag">${song.genre || song.genre_clean || '其他'}</span></td>
            <td>
                <div class="popularity-slider-container">
                    <input type="range" class="popularity-slider" 
                           min="0" max="100" value="${Math.round(popValue)}"
                           onchange="updateSongPopularity('${song.song_id}', this.value)"
                           title="当前: ${Math.round(popValue)}">
                    <span class="popularity-value" id="pop-value-${song.song_id}">${Math.round(popValue)}</span>
                </div>
            </td>
            <td>
                <div class="action-btns">
                    <button class="btn btn-sm btn-secondary" onclick="openEditSongModal('${song.song_id}')" title="编辑">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="deleteSong('${song.song_id}')" title="删除">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </td>
        </tr>
        `;
    }).join('');
}

// 修复：排序功能，添加调试日志
function sortSongs(field) {
    console.log('sortSongs called with field:', field);
    console.log('Current sort config:', sortConfig);
    
    if (sortConfig.field === field) {
        sortConfig.direction = sortConfig.direction === 'asc' ? 'desc' : 'asc';
    } else {
        sortConfig.field = field;
        sortConfig.direction = 'asc';
    }
    
    console.log('New sort config:', sortConfig);
    
    updateSortIcons();
    loadSongsList(1);
}

function updateSortIcons() {
    // 清除所有排序样式
    document.querySelectorAll('#songs-table th').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
    });
    
    // 只保留允许的排序字段
    const thMap = {
        'song_id': 0,
        // 'song_name': 1,  // 【删除】歌曲名不再支持排序
        'final_popularity': 4
    };
    
    const index = thMap[sortConfig.field];
    if (index !== undefined) {
        const th = document.querySelectorAll('#songs-table th')[index];
        if (th) {
            th.classList.add(`sort-${sortConfig.direction}`);
        }
    }
}

function searchSongs() {
    loadSongsList(1);
}

function refreshSongs() {
    document.getElementById('song-search-input').value = '';
    document.getElementById('song-genre-filter').value = '';
    sortConfig = { field: 'song_id', direction: 'asc' };
    loadSongsList(1);
    showNotification('已刷新', 'success');
}

async function updateSongPopularity(songId, value) {
    try {
        const res = await fetch(`${API_BASE_URL}/admin/songs/${songId}`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${adminToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ final_popularity: parseFloat(value) })
        });
        
        const data = await res.json();
        if (data.success) {
            showNotification('流行度已更新', 'success');
            // 更新显示的数值
            const displayEl = document.getElementById(`pop-value-${songId}`);
            if (displayEl) displayEl.textContent = value;
            
            // 更新本地数据
            const song = songsData.find(s => s.song_id === songId);
            if (song) song.final_popularity = parseFloat(value);
        } else {
            throw new Error(data.message);
        }
    } catch (err) {
        showNotification('更新失败: ' + err.message, 'error');
    }
}

// 打开编辑歌曲模态框（扩展版）
// ==================== 修改 openEditSongModal 函数（修复版） ====================
async function openEditSongModal(songId) {
    try {
        // 【关键修复】先获取完整的歌曲数据
        const res = await fetch(`${API_BASE_URL}/admin/songs/${songId}`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const result = await res.json();
        
        if (!result.success) {
            showNotification('获取歌曲数据失败', 'error');
            return;
        }
        
        const song = result.data;
        
        // 设置表单值
        document.getElementById('edit-song-id').value = songId;
        
        // 设置基本信息
        document.getElementById('edit-song-name').value = song.song_name || '';
        document.getElementById('edit-artists').value = song.artists || '';
        document.getElementById('edit-album').value = song.album || '';
        
        // 设置流派
        const genreSelect = document.getElementById('edit-genre');
        if (genreSelect) {
            const genre = song.genre || 'Pop';
            if (genreSelect.querySelector(`option[value="${genre}"]`)) {
                genreSelect.value = genre;
            } else {
                genreSelect.value = 'Pop';
            }
        }
        
        // 设置语言
        const languageSelect = document.getElementById('edit-language');
        if (languageSelect) {
            languageSelect.value = song.language || 'chinese';
        }
        
        // 【修复5】设置发行年份
        const yearInput = document.getElementById('edit-publish-year');
        if (yearInput) {
            // 确保年份是有效的数字
            const year = parseInt(song.publish_year);
            if (!isNaN(year) && year >= 1900 && year <= new Date().getFullYear()) {
                yearInput.value = year;
            } else {
                yearInput.value = '';
            }
        }
        
        // 设置时长（毫秒）
        const durationInput = document.getElementById('edit-duration');
        if (durationInput) {
            durationInput.value = song.duration_ms || '';
        }
        
        // 设置流行度
        let popValue = song.final_popularity || song.popularity || 50;
        const popularitySlider = document.getElementById('edit-popularity');
        const popularityValue = document.getElementById('edit-popularity-value');
        
        if (popularitySlider && popularityValue) {
            popularitySlider.value = Math.round(popValue);
            popularityValue.textContent = Math.round(popValue);
        }
        
        // 设置音频特征
        const features = song.audio_features || {};
        
        const danceSlider = document.getElementById('edit-danceability');
        const danceValue = document.getElementById('edit-dance-val');
        if (danceSlider && danceValue) {
            danceSlider.value = features.danceability || 0.5;
            danceValue.textContent = features.danceability || 0.5;
        }
        
        const energySlider = document.getElementById('edit-energy');
        const energyValue = document.getElementById('edit-energy-val');
        if (energySlider && energyValue) {
            energySlider.value = features.energy || 0.5;
            energyValue.textContent = features.energy || 0.5;
        }
        
        const valenceSlider = document.getElementById('edit-valence');
        const valenceValue = document.getElementById('edit-valence-val');
        if (valenceSlider && valenceValue) {
            valenceSlider.value = features.valence || 0.5;
            valenceValue.textContent = features.valence || 0.5;
        }
        
        const tempoSlider = document.getElementById('edit-tempo');
        const tempoValue = document.getElementById('edit-tempo-val');
        if (tempoSlider && tempoValue) {
            tempoSlider.value = features.tempo || 120;
            tempoValue.textContent = features.tempo || 120;
        }
        
        // 【关键修复】确保切换到歌曲信息选项卡
        setTimeout(() => {
            switchTab('song-info-tab');
        }, 50);
        
        // 加载评论统计
        loadSongCommentsStats(songId);
        
        // 打开弹窗
        openModal('edit-song-modal');
        
    } catch (error) {
        console.error('打开编辑弹窗失败:', error);
        showNotification('打开编辑页面失败: ' + error.message, 'error');
    }
}

// 保存歌曲编辑（扩展字段）
async function saveSongEdit() {
    const songId = document.getElementById('edit-song-id').value;
    const data = {
        song_name: document.getElementById('edit-song-name').value,
        artists: document.getElementById('edit-artists').value,
        album: document.getElementById('edit-album').value,
        genre: document.getElementById('edit-genre').value,
        language: document.getElementById('edit-language').value,
        publish_year: document.getElementById('edit-publish-year').value ? parseInt(document.getElementById('edit-publish-year').value) : null,
        duration_ms: document.getElementById('edit-duration').value ? parseInt(document.getElementById('edit-duration').value) : null,
        final_popularity: parseFloat(document.getElementById('edit-popularity').value),
        danceability: parseFloat(document.getElementById('edit-danceability').value),
        energy: parseFloat(document.getElementById('edit-energy').value),
        valence: parseFloat(document.getElementById('edit-valence').value),
        tempo: parseFloat(document.getElementById('edit-tempo').value)
    };
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/songs/${songId}`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${adminToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        const result = await res.json();
        if (result.success) {
            showNotification('保存成功', 'success');
            closeModal('edit-song-modal');
            loadSongsList(currentPage.songs);
        } else {
            throw new Error(result.message);
        }
    } catch (err) {
        showNotification('保存失败: ' + err.message, 'error');
    }
}

async function deleteSong(songId) {
    if (!confirm('确定要删除这首歌曲吗？')) return;
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/songs/${songId}`, {
            method: 'DELETE',
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const data = await res.json();
        if (data.success) {
            showNotification('删除成功', 'success');
            loadSongsList(currentPage.songs);
        } else {
            throw new Error(data.message);
        }
    } catch (err) {
        showNotification('删除失败: ' + err.message, 'error');
    }
}

// ==================== 用户管理（修复筛选） ====================
async function loadUsersList(page = 1) {
    currentPage.users = page;
    
    // 【关键修复】添加元素存在性检查
    const searchInput = document.getElementById('user-search-input');
    const activityFilter = document.getElementById('user-activity-filter');
    
    const keyword = searchInput ? searchInput.value.trim() : '';
    const activity = activityFilter ? activityFilter.value : '';
    
    console.log('Loading users - Page:', page, 'Keyword:', keyword, 'Activity:', activity);
    
    try {
        const params = new URLSearchParams({
            page: page,
            per_page: 10
        });
        
        if (keyword) params.append('keyword', keyword);
        if (activity) params.append('activity_level', activity);
        
        const url = `${API_BASE_URL}/admin/users?${params.toString()}`;
        console.log('Fetching URL:', url);
        
        const res = await fetch(url, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }
        
        const result = await res.json();
        console.log('Users API Response:', result);
        
        if (result.success) {
            usersData = result.data.users || [];
            const total = result.data.total || 0;
            totalPages.users = Math.ceil(total / 10) || 1;
            renderUsersTable();
            renderPagination('users', totalPages.users, page);
        } else {
            throw new Error(result.message);
        }
    } catch (err) {
        console.error('加载用户失败:', err);
        showNotification('加载失败: ' + err.message, 'error');
    }
}

// 添加搜索按钮的点击事件
function searchUsers() {
    loadUsersList(1);
}

// 确保搜索框的回车事件也触发搜索
document.addEventListener('DOMContentLoaded', () => {
    const userSearchInput = document.getElementById('user-search-input');
    if (userSearchInput) {
        userSearchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                searchUsers();
            }
        });
    }
});

function renderUsersTable() {
    const tbody = document.getElementById('users-table-body');
    
    if (!usersData || usersData.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" class="empty-state"><i class="fas fa-inbox"></i><p>暂无用户数据</p></td></tr>`;
        return;
    }
    
    tbody.innerHTML = usersData.map(user => `
        <tr>
            <td><code>${user.user_id}</code></td>
            <td>
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <div style="width: 32px; height: 32px; background: var(--primary-color); color: white; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: 600;">
                        ${(user.nickname || 'U')[0].toUpperCase()}
                    </div>
                    <span style="font-weight: 500;">${user.nickname || '未命名'}</span>
                </div>
            </td>
            <td>
                <span class="role-badge ${user.role === 'admin' ? 'role-admin' : 'role-user'}">
                    <i class="fas fa-${user.role === 'admin' ? 'user-shield' : 'user'}"></i>
                    ${user.role === 'admin' ? '管理员' : '用户'}
                </span>
            </td>
            <td>
                <span class="status-badge badge-${getActivityLevelClass(user.activity_level)}">
                    ${user.activity_level || '普通用户'}
                </span>
            </td>
            <td>${user.unique_songs || 0} 首</td>
            <td>${user.created_at ? new Date(user.created_at).toLocaleDateString() : '-'}</td>
            <td>
                <div class="action-btns">
                    <button class="btn btn-sm btn-secondary" onclick="openEditUserModal('${user.user_id}')" title="编辑">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="deleteUser('${user.user_id}')" title="删除">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
}

function getActivityLevelClass(level) {
    if (!level) return 'secondary';
    const map = {
        '高活跃': 'success',
        '中高活跃': 'info',
        '普通用户': 'secondary',
        '中低活跃': 'warning',
        '低活跃': 'danger',
        '新用户': 'info'
    };
    return map[level] || 'secondary';
}

// 打开编辑用户模态框（扩展版）
// 打开编辑用户模态框（扩展版）
async function openEditUserModal(userId) {
    try {
        // 【关键修复】先获取完整的用户数据
        const res = await fetch(`${API_BASE_URL}/admin/users/${userId}`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const result = await res.json();
        
        if (!result.success) {
            showNotification('获取用户数据失败', 'error');
            return;
        }
        
        const user = result.data;
        
        document.getElementById('edit-user-id').value = user.user_id;
        document.getElementById('edit-user-id-display').value = user.user_id;
        document.getElementById('edit-nickname').value = user.nickname || '';
        
        // 【修复6】设置性别（处理数字转换）
        const genderSelect = document.getElementById('edit-gender');
        if (genderSelect) {
            let genderValue = '';
            if (user.gender === 1 || user.gender === '1' || user.gender === '男') {
                genderValue = '1';
            } else if (user.gender === 2 || user.gender === '2' || user.gender === '女') {
                genderValue = '2';
            }
            genderSelect.value = genderValue;
        }
        
        // 设置年龄
        const ageInput = document.getElementById('edit-age');
        if (ageInput) {
            const age = parseInt(user.age);
            if (!isNaN(age) && age >= 12 && age <= 100) {
                ageInput.value = age;
            } else {
                ageInput.value = '';
            }
        }
        
        // 设置省份
        document.getElementById('edit-province').value = user.province || '';
        
        // 设置城市
        document.getElementById('edit-city').value = user.city || '';
        
        // 设置角色
        document.getElementById('edit-role').value = user.role || 'user';
        
        // 设置活跃度
        const activitySelect = document.getElementById('edit-activity');
        if (activitySelect) {
            // 清理活跃度字符串中的空格
            const cleanActivity = (user.activity_level || '普通用户').trim();
            
            // 检查是否有匹配的选项
            let found = false;
            for (let option of activitySelect.options) {
                if (option.value === cleanActivity) {
                    activitySelect.value = cleanActivity;
                    found = true;
                    break;
                }
            }
            
            // 如果没有匹配的选项，使用默认值
            if (!found) {
                activitySelect.value = '普通用户';
            }
        }
        
        openModal('edit-user-modal');
        
    } catch (error) {
        console.error('打开用户编辑弹窗失败:', error);
        showNotification('加载用户信息失败: ' + error.message, 'error');
    }
}

// 保存用户编辑（扩展字段）
async function saveUserEdit() {
    const userId = document.getElementById('edit-user-id').value;
    const data = {
        nickname: document.getElementById('edit-nickname').value,
        gender: document.getElementById('edit-gender').value || null,
        age: document.getElementById('edit-age').value ? parseInt(document.getElementById('edit-age').value) : null,
        province: document.getElementById('edit-province').value,
        city: document.getElementById('edit-city').value,
        role: document.getElementById('edit-role').value,
        activity_level: document.getElementById('edit-activity').value
    };
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/users/${userId}`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${adminToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        const result = await res.json();
        if (result.success) {
            showNotification('用户更新成功', 'success');
            closeModal('edit-user-modal');
            loadUsersList(currentPage.users);
        } else {
            throw new Error(result.message);
        }
    } catch (err) {
        showNotification('更新失败: ' + err.message, 'error');
    }
}

async function deleteUser(userId) {
    if (!confirm('确定要删除该用户吗？')) return;
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/users/${userId}`, {
            method: 'DELETE',
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const data = await res.json();
        if (data.success) {
            showNotification('删除成功', 'success');
            loadUsersList(currentPage.users);
        } else {
            throw new Error(data.message);
        }
    } catch (err) {
        showNotification('删除失败: ' + err.message, 'error');
    }
}

function searchUsers() {
    loadUsersList(1);
}

function refreshUsers() {
    document.getElementById('user-search-input').value = '';
    document.getElementById('user-activity-filter').value = '';
    loadUsersList(1);
    showNotification('已刷新', 'success');
}

// ==================== 分页组件 ====================
function renderPagination(type, total, current) {
    const container = document.getElementById(`${type}-pagination`);
    if (!container) return;
    
    if (total <= 1) {
        container.innerHTML = '';
        return;
    }
    
    let html = `<button class="page-btn" onclick="load${type.charAt(0).toUpperCase() + type.slice(1)}List(${current - 1})" ${current === 1 ? 'disabled' : ''}><i class="fas fa-chevron-left"></i></button>`;
    
    const maxButtons = 5;
    let startPage = Math.max(1, current - Math.floor(maxButtons / 2));
    let endPage = Math.min(total, startPage + maxButtons - 1);
    
    if (endPage - startPage < maxButtons - 1) {
        startPage = Math.max(1, endPage - maxButtons + 1);
    }
    
    if (startPage > 1) {
        html += `<button class="page-btn" onclick="load${type.charAt(0).toUpperCase() + type.slice(1)}List(1)">1</button>`;
        if (startPage > 2) html += `<span class="page-info">...</span>`;
    }
    
    for (let i = startPage; i <= endPage; i++) {
        html += `<button class="page-btn ${i === current ? 'active' : ''}" onclick="load${type.charAt(0).toUpperCase() + type.slice(1)}List(${i})">${i}</button>`;
    }
    
    if (endPage < total) {
        if (endPage < total - 1) html += `<span class="page-info">...</span>`;
        html += `<button class="page-btn" onclick="load${type.charAt(0).toUpperCase() + type.slice(1)}List(${total})">${total}</button>`;
    }
    
    html += `<button class="page-btn" onclick="load${type.charAt(0).toUpperCase() + type.slice(1)}List(${current + 1})" ${current === total ? 'disabled' : ''}><i class="fas fa-chevron-right"></i></button>`;
    html += `<span class="page-info">共 ${total} 页</span>`;
    
    container.innerHTML = html;
}

// ==================== 系统配置（A/B测试修复） ====================
function updateWeights() {
    const itemcf = 35;
    const usercf = 25;
    const content = 25;
    const mf = 15;
    
    const total = itemcf + usercf + content + mf;
    
    // 只更新存在的元素，避免报错
    const updateIfExists = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    };
    
    // 只更新实际存在的元素
    updateIfExists('itemcf-value', itemcf + '%');
    updateIfExists('itemcf-display', itemcf + '%');
    updateIfExists('usercf-value', usercf + '%');
    updateIfExists('usercf-display', usercf + '%');
    updateIfExists('content-value', content + '%');
    updateIfExists('content-display', content + '%');
    updateIfExists('mf-value', mf + '%');
    updateIfExists('mf-display', mf + '%');
    
    updateIfExists('total-weight', total + '%');
    
    const weightWarningEl = document.getElementById('weight-warning');
    if (weightWarningEl) {
        weightWarningEl.classList.add('hidden');
    }
}

// 修复：A/B测试切换时更新全局状态并刷新概览页
// 修改 toggleABTest 函数
function toggleABTest() {
    const toggle = document.getElementById('ab-test-toggle');
    const config = document.getElementById('ab-test-config');
    
    isABTestEnabled = toggle.checked;
    localStorage.setItem('ab_test_enabled', isABTestEnabled);
    
    // 【关键】立即同步右上角指示器
    syncABTestIndicator();
    
    if (toggle.checked) {
        config.classList.remove('hidden');
        const resultsPanel = document.getElementById('ab-results-config');
        if (resultsPanel) resultsPanel.style.display = 'block';
        showNotification('A/B测试已开启', 'success');
    } else {
        config.classList.add('hidden');
        const resultsPanel = document.getElementById('ab-results-config');
        if (resultsPanel) resultsPanel.style.display = 'none';
        showNotification('A/B测试已关闭', 'info');
    }
}

// 修改 refreshABTestResults 函数
async function refreshABTestResults() {
    try {
        // 显示加载状态
        const loadingHTML = '<i class="fas fa-spinner fa-spin"></i>';
        document.getElementById('ab-config-a-ctr').innerHTML = loadingHTML;
        document.getElementById('ab-config-b-ctr').innerHTML = loadingHTML;
        
        // 从后端获取真实数据
        const res = await fetch(`${API_BASE_URL}/admin/ab-test/stats`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const result = await res.json();
        
        if (!result.success) {
            throw new Error(result.message || '获取数据失败');
        }
        
        const stats = result.data;
        const groupA = document.getElementById('ab-group-a')?.value || 'hybrid';
        const groupB = document.getElementById('ab-group-b')?.value || 'itemcf';
        
        // 查找对应的算法数据
        const algoData = {};
        if (stats.algorithm_performance && stats.algorithm_performance.length > 0) {
            stats.algorithm_performance.forEach(item => {
                algoData[item.algorithm] = item;
            });
        }
        
        // 获取当前选中的A组和B组算法数据
        const algoAData = algoData[groupA] || { ctr: 0, avg_duration: 0, total: 0, clicks: 0, listens: 0 };
        const algoBData = algoData[groupB] || { ctr: 0, avg_duration: 0, total: 0, clicks: 0, listens: 0 };
        
        // 确保ctr是数字
        const ctrA = parseFloat(algoAData.ctr) || 0;
        const ctrB = parseFloat(algoBData.ctr) || 0;
        
        // 更新显示
        updateABTestDisplay(algoAData, algoBData, groupA, groupB);
        
        showNotification('A/B测试数据已刷新', 'success');
        
    } catch (error) {
        console.error('获取A/B测试数据失败:', error);
        showNotification('A/B测试数据获取失败: ' + error.message, 'error');
        
        // 显示错误状态
        document.getElementById('ab-config-a-ctr').textContent = '--';
        document.getElementById('ab-config-b-ctr').textContent = '--';
        document.getElementById('ab-winner-text').textContent = '数据获取失败';
    }
}

function getDefaultAlgorithmData(algorithm) {
    // 默认数据映射
    const defaults = {
        'hybrid': { ctr: 12.5, total: 1000, avg_duration: 225, clicks: 125, listens: 200 },
        'itemcf': { ctr: 10.2, total: 800, avg_duration: 192, clicks: 82, listens: 150 },
        'usercf': { ctr: 9.8, total: 600, avg_duration: 210, clicks: 59, listens: 120 },
        'content': { ctr: 8.5, total: 700, avg_duration: 198, clicks: 60, listens: 110 },
        'mf': { ctr: 9.5, total: 500, avg_duration: 185, clicks: 48, listens: 95 },
        'cf': { ctr: 10.0, total: 750, avg_duration: 200, clicks: 75, listens: 140 }
    };
    return defaults[algorithm] || { ctr: 10.0, total: 500, avg_duration: 180, clicks: 50, listens: 80 };
}

function updateABTestDisplay(algoAData, algoBData, groupA, groupB) {
    // 计算收藏转化率
    const conversionA = algoAData.total > 0 ? ((algoAData.listens || 0) / algoAData.total * 100).toFixed(1) : '0.0';
    const conversionB = algoBData.total > 0 ? ((algoBData.listens || 0) / algoBData.total * 100).toFixed(1) : '0.0';
    
    // 更新显示
    document.getElementById('ab-config-a-ctr').textContent = parseFloat(algoAData.ctr || 0).toFixed(1) + '%';
    document.getElementById('ab-config-a-duration').textContent = formatDuration(algoAData.avg_duration || 180);
    document.getElementById('ab-config-a-conversion').textContent = conversionA + '%';
    
    document.getElementById('ab-config-b-ctr').textContent = parseFloat(algoBData.ctr || 0).toFixed(1) + '%';
    document.getElementById('ab-config-b-duration').textContent = formatDuration(algoBData.avg_duration || 180);
    document.getElementById('ab-config-b-conversion').textContent = conversionB + '%';
    
    // 显示样本量
    document.getElementById('ab-config-a-ctr').title = `样本量: ${algoAData.total || 0}次推荐`;
    document.getElementById('ab-config-b-ctr').title = `样本量: ${algoBData.total || 0}次推荐`;
    
    // 计算胜出者
    const ctrA = parseFloat(algoAData.ctr) || 0;
    const ctrB = parseFloat(algoBData.ctr) || 0;
    const diff = (ctrA - ctrB).toFixed(1);
    
    let winnerText = '';
    if (parseFloat(diff) > 0) {
        winnerText = `A组 (${getAlgorithmDisplayName(groupA)}) 点击率高出 ${Math.abs(diff)}%`;
    } else if (parseFloat(diff) < 0) {
        winnerText = `B组 (${getAlgorithmDisplayName(groupB)}) 点击率高出 ${Math.abs(diff)}%`;
    } else {
        winnerText = '两组表现持平';
    }
    document.getElementById('ab-winner-text').textContent = winnerText;
}

// 添加模拟数据回退函数
function useMockABTestData(groupA, groupB) {
    // 模拟数据
    const mockData = {
        hybrid: { ctr: 12.5, total: 1000, avg_duration: 225, clicks: 125, listens: 200 },
        itemcf: { ctr: 10.2, total: 800, avg_duration: 192, clicks: 82, listens: 150 },
        usercf: { ctr: 9.8, total: 600, avg_duration: 210, clicks: 59, listens: 120 },
        content: { ctr: 8.5, total: 700, avg_duration: 198, clicks: 60, listens: 110 },
        mf: { ctr: 9.5, total: 500, avg_duration: 185, clicks: 48, listens: 95 }
    };
    
    const algoAData = mockData[groupA] || mockData.hybrid;
    const algoBData = mockData[groupB] || mockData.itemcf;
    
    // 更新显示
    document.getElementById('ab-config-a-ctr').textContent = algoAData.ctr.toFixed(1) + '%';
    document.getElementById('ab-config-a-duration').textContent = formatDuration(algoAData.avg_duration);
    document.getElementById('ab-config-a-conversion').textContent = ((algoAData.listens / algoAData.total) * 100).toFixed(1) + '%';
    
    document.getElementById('ab-config-b-ctr').textContent = algoBData.ctr.toFixed(1) + '%';
    document.getElementById('ab-config-b-duration').textContent = formatDuration(algoBData.avg_duration);
    document.getElementById('ab-config-b-conversion').textContent = ((algoBData.listens / algoBData.total) * 100).toFixed(1) + '%';
    
    // 显示样本量
    document.getElementById('ab-config-a-ctr').title = `样本量: ${algoAData.total}次推荐`;
    document.getElementById('ab-config-b-ctr').title = `样本量: ${algoBData.total}次推荐`;
    
    // 计算胜出者
    const diff = (algoAData.ctr - algoBData.ctr).toFixed(1);
    let winnerText = '';
    if (parseFloat(diff) > 0) {
        winnerText = `A组 (${getAlgorithmDisplayName(groupA)}) 点击率高出 ${Math.abs(diff)}%`;
    } else if (parseFloat(diff) < 0) {
        winnerText = `B组 (${getAlgorithmDisplayName(groupB)}) 点击率高出 ${Math.abs(diff)}%`;
    } else {
        winnerText = '两组表现持平';
    }
    document.getElementById('ab-winner-text').textContent = winnerText;
    
    showNotification('使用模拟数据（真实数据获取失败）', 'warning');
}


function formatDuration(seconds) {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}:${secs < 10 ? '0' : ''}${secs}`;
}

// 新增：更新A/B测试面板数据
function updateABTestDashboard() {
    if (!isABTestEnabled) return;
    
    // 模拟加载A/B测试数据（实际应从后端API获取）
    document.getElementById('ab-a-ctr').textContent = (12.5 + Math.random() * 2).toFixed(1) + '%';
    document.getElementById('ab-b-ctr').textContent = (10.8 + Math.random() * 2).toFixed(1) + '%';
}

async function saveConfig() {
    const config = {
        recommendation: {
            cache_ttl: parseInt(document.getElementById('cache-ttl').value),
            mmr_enabled: document.getElementById('mmr-toggle').checked,
            cold_start_strategy: document.getElementById('cold-start-strategy').value,
            max_recommend_count: parseInt(document.getElementById('max-recommend-count').value)
        },
        system: {
            enable_ab_test: document.getElementById('ab-test-toggle').checked,
            ab_test_group_a: document.getElementById('ab-group-a').value,
            ab_test_group_b: document.getElementById('ab-group-b').value,
            default_algorithm: document.getElementById('default-algorithm').value
        },
        user: {
            min_songs_for_personalization: parseInt(document.getElementById('min-songs-threshold').value)
        },
        content: {
            similarity_threshold: parseFloat(document.getElementById('similarity-threshold').value)
        }
    };
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/config/system`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${adminToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        });
        
        const data = await res.json();
        if (data.success) {
            showNotification('系统配置已保存成功', 'success');
            
            // 更新A/B测试状态
            syncABTestIndicator();
            
            // 重新加载相关数据
            if (document.getElementById('page-dashboard').classList.contains('active')) {
                await loadDashboardData();
                await loadAdvancedStats();
                initCharts();
                initAdvancedCharts();
            }
        } else {
            throw new Error(data.message);
        }
    } catch (err) {
        showNotification('保存失败: ' + err.message, 'error');
    }
}

// ==================== 工具函数 ====================
function openModal(modalId) {
    document.getElementById(modalId).classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

function showNotification(msg, type = 'info') {
    const existing = document.querySelectorAll('.notification');
    existing.forEach(el => el.remove());
    
    const div = document.createElement('div');
    div.className = `notification ${type}`;
    
    const icons = {
        success: 'check-circle',
        error: 'exclamation-circle',
        warning: 'exclamation-triangle',
        info: 'info-circle'
    };
    
    div.innerHTML = `<i class="fas fa-${icons[type]}"></i> ${msg}`;
    document.body.appendChild(div);
    
    setTimeout(() => div.classList.add('show'), 10);
    setTimeout(() => {
        div.classList.remove('show');
        setTimeout(() => div.remove(), 300);
    }, 3000);
}

// 在页面加载时同步A/B测试状态到右上角
function syncABTestIndicator() {
    const indicator = document.getElementById('ab-test-indicator');
    const toggle = document.getElementById('ab-test-toggle');
    
    if (!indicator) return;
    
    // 从localStorage读取状态
    const isEnabled = localStorage.getItem('ab_test_enabled') === 'true';
    
    if (isEnabled) {
        indicator.innerHTML = '<i class="fas fa-flask"></i> A/B测试: 运行中';
        indicator.classList.remove('inactive');
        indicator.style.background = 'rgba(6, 214, 160, 0.1)';
        indicator.style.color = 'var(--success-color)';
        indicator.style.border = '1px solid rgba(6, 214, 160, 0.2)';
    } else {
        indicator.innerHTML = '<i class="fas fa-flask"></i> A/B测试: 关闭';
        indicator.classList.add('inactive');
        indicator.style.background = 'rgba(239, 71, 111, 0.1)';
        indicator.style.color = 'var(--danger-color)';
        indicator.style.border = '1px solid rgba(239, 71, 111, 0.2)';
    }
    
    // 同步toggle状态
    if (toggle) toggle.checked = isEnabled;
}

// 监听算法选择变化
function setupAlgorithmSelectors() {
    const selectA = document.getElementById('ab-group-a');
    const selectB = document.getElementById('ab-group-b');
    
    if (selectA) {
        selectA.addEventListener('change', function() {
            // 保存到localStorage
            localStorage.setItem('ab_group_a', this.value);
            showNotification(`对照组已切换为: ${getAlgorithmDisplayName(this.value)}`, 'success');
            // 立即刷新结果
            if (isABTestEnabled) refreshABTestResults();
        });
        
        // 恢复保存的值
        const savedA = localStorage.getItem('ab_group_a');
        if (savedA) selectA.value = savedA;
    }
    
    if (selectB) {
        selectB.addEventListener('change', function() {
            localStorage.setItem('ab_group_b', this.value);
            showNotification(`实验组已切换为: ${getAlgorithmDisplayName(this.value)}`, 'success');
            if (isABTestEnabled) refreshABTestResults();
        });
        
        const savedB = localStorage.getItem('ab_group_b');
        if (savedB) selectB.value = savedB;
    }
}

function getAlgorithmDisplayName(alg) {
    const names = {
        'hybrid': 'Hybrid混合推荐',
        'itemcf': '纯ItemCF',
        'usercf': '纯UserCF',
        'content': '纯Content',
        'mf': '矩阵分解'
    };
    return names[alg] || alg;
}

async function loadSystemConfig() {
    try {
        const res = await fetch(`${API_BASE_URL}/admin/config/system`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        const result = await res.json();
        
        if (result.success) {
            const config = result.data;
            
            console.log('加载系统配置:', config);
            
            // 【关键修复】添加元素存在性检查
            const safeSetValue = (id, value) => {
                const element = document.getElementById(id);
                if (element) {
                    element.value = value;
                } else {
                    console.warn(`元素 ${id} 不存在`);
                }
            };
            
            const safeSetText = (id, text) => {
                const element = document.getElementById(id);
                if (element) {
                    element.textContent = text;
                } else {
                    console.warn(`元素 ${id} 不存在`);
                }
            };
            
            const safeSetChecked = (id, checked) => {
                const element = document.getElementById(id);
                if (element) {
                    element.checked = checked;
                } else {
                    console.warn(`元素 ${id} 不存在`);
                }
            };
            
            // 设置控件值
            if (config.system) {
                safeSetValue('default-algorithm', config.system.default_algorithm || 'hybrid');
                safeSetChecked('ab-test-toggle', config.system.enable_ab_test || false);
                safeSetValue('ab-group-a', config.system.ab_test_group_a || 'hybrid');
                safeSetValue('ab-group-b', config.system.ab_test_group_b || 'itemcf');
            }
            
            if (config.user) {
                const minSongs = config.user.min_songs_for_personalization || 10;
                safeSetValue('min-songs-threshold', minSongs);
                safeSetText('min-songs-value', minSongs + '首');
            }
            
            if (config.content) {
                const similarity = config.content.similarity_threshold || 0.6;
                safeSetValue('similarity-threshold', similarity);
                safeSetText('similarity-value', similarity.toString());
            }
            
            if (config.recommendation) {
                safeSetValue('max-recommend-count', config.recommendation.max_recommend_count || 50);
                safeSetValue('cache-ttl', config.recommendation.cache_ttl || 30);
                safeSetChecked('mmr-toggle', config.recommendation.mmr_enabled !== false);
                safeSetValue('cold-start-strategy', config.recommendation.cold_start_strategy || 'hot');
            }
            
            // 触发A/B测试显示更新
            toggleABTest();
        }
    } catch (err) {
        console.error('加载系统配置失败:', err);
        showNotification('加载系统配置失败，使用默认配置', 'warning');
    }
}

async function saveConfig() {
    try {
        // 获取所有配置值
        const config = {
            recommendation: {
                cache_ttl: parseInt(document.getElementById('cache-ttl').value),
                mmr_enabled: document.getElementById('mmr-toggle').checked,
                cold_start_strategy: document.getElementById('cold-start-strategy').value,
            },
            system: {
                enable_ab_test: document.getElementById('ab-test-toggle').checked,
                ab_test_group_a: document.getElementById('ab-group-a').value,
                ab_test_group_b: document.getElementById('ab-group-b').value,
            }
        };
        
        showNotification('保存配置中...', 'info');
        
        // 这里应该调用后端保存配置的接口
        // 目前先模拟保存成功
        console.log('保存配置:', config);
        
        // 保存到localStorage
        localStorage.setItem('admin_config', JSON.stringify(config));
        
        // 如果是A/B测试配置，更新全局状态
        if (config.system.enable_ab_test !== isABTestEnabled) {
            isABTestEnabled = config.system.enable_ab_test;
            localStorage.setItem('ab_test_enabled', isABTestEnabled);
            syncABTestIndicator();
        }
        
        // 模拟保存延迟
        setTimeout(() => {
            showNotification('配置保存成功', 'success');
        }, 500);
        
    } catch (err) {
        console.error('保存配置失败:', err);
        showNotification('保存失败: ' + err.message, 'error');
    }
}

// ==================== 歌曲管理（新增完整函数） ====================
async function loadSongsList(page = 1) {
    currentPage.songs = page;
    
    // 【关键修复】添加元素存在性检查
    const searchInput = document.getElementById('song-search-input');
    const genreFilter = document.getElementById('song-genre-filter');
    
    const keyword = searchInput ? searchInput.value.trim() : '';
    const genreFilterValue = genreFilter ? genreFilter.value : '';
    const sortField = sortConfig.field;
    const sortOrder = sortConfig.direction;
    
    console.log('Loading songs - Page:', page, 'Keyword:', keyword, 'Genre:', genreFilterValue, 'Sort:', sortField, sortOrder);
    
    try {
        const params = new URLSearchParams({
            page: page,
            per_page: 20,
            sort_by: sortField,
            sort_order: sortOrder
        });
        
        if (keyword) params.append('keyword', keyword);
        if (genreFilterValue) params.append('genre', genreFilterValue);
        
        const url = `${API_BASE_URL}/admin/songs?${params.toString()}`;
        console.log('Fetching songs URL:', url);
        
        const res = await fetch(url, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }
        
        const result = await res.json();
        console.log('Songs API Response:', result);
        
        if (result.success) {
            songsData = result.data.songs || [];
            const total = result.data.total || 0;
            totalPages.songs = Math.ceil(total / 20) || 1;
            renderSongsTable();
            renderPagination('songs', totalPages.songs, page);
        } else {
            throw new Error(result.message);
        }
    } catch (err) {
        console.error('加载歌曲失败:', err);
        showNotification('加载歌曲失败: ' + err.message, 'error');
        
        // 显示空状态
        const tbody = document.getElementById('songs-table-body');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="empty-state">
                        <i class="fas fa-exclamation-triangle"></i>
                        <p>加载失败: ${err.message}</p>
                        <button class="btn btn-primary" onclick="loadSongsList(1)" style="margin-top: 1rem;">
                            重试
                        </button>
                    </td>
                </tr>
            `;
        }
    }
}

// 在admin.js中添加评论管理功能

// ==================== 选项卡切换函数 ====================
function switchTab(tabId) {
    console.log('切换选项卡到:', tabId);
    
    // 隐藏所有选项卡内容
    document.querySelectorAll('.edit-tab').forEach(tab => {
        tab.classList.remove('active');
        tab.style.display = 'none';
    });
    
    // 移除所有选项卡按钮的激活状态
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // 显示选中的选项卡内容
    const targetTab = document.getElementById(tabId);
    if (targetTab) {
        targetTab.classList.add('active');
        targetTab.style.display = 'block';
        
        // 激活对应的选项卡按钮
        const activeBtn = document.querySelector(`[onclick="switchTab('${tabId}')"]`);
        if (activeBtn) {
            activeBtn.classList.add('active');
        }
        
        // 如果是评论管理选项卡，加载评论数据
        if (tabId === 'comments-tab') {
            const songId = document.getElementById('edit-song-id').value;
            if (songId) {
                console.log('切换到评论管理，加载歌曲ID:', songId);
                // 先加载统计
                loadSongCommentsStats(songId);
                // 再加载评论列表
                setTimeout(() => {
                    loadSongComments(songId, 1);
                }, 100);
            }
        }
    } else {
        console.error('找不到选项卡:', tabId);
    }
}

// ==================== 加载歌曲评论统计 ====================
async function loadSongCommentsStats(songId) {
    try {
        const res = await fetch(`${API_BASE_URL}/admin/songs/${songId}/comments/stats`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const result = await res.json();
        
        if (result.success) {
            const stats = result.data.stats;
            
            // 更新统计显示
            document.getElementById('comments-total').textContent = stats.total_comments;
            document.getElementById('comments-likes').textContent = stats.total_likes;
            document.getElementById('avg-sentiment').textContent = stats.avg_sentiment.toFixed(2);
            document.getElementById('positive-ratio').textContent = `${stats.positive_ratio}%`;
            document.getElementById('negative-ratio').textContent = `${stats.negative_ratio}%`;
            document.getElementById('neutral-ratio').textContent = `${stats.neutral_ratio}%`;
            
            // 如果有评论，加载评论列表
            if (stats.total_comments > 0) {
                loadSongComments(songId, 1);
            }
        }
    } catch (err) {
        console.error('加载评论统计失败:', err);
        // 不显示错误通知，避免干扰用户
    }
}

// ==================== 加载歌曲评论列表 ====================
async function loadSongComments(songId, page = 1) {
    currentCommentsPage = page;
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/songs/${songId}/comments?page=${page}&per_page=10`, {
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const result = await res.json();
        
        if (result.success) {
            currentSongComments = result.data.comments;
            const total = result.data.pagination.total;
            commentsTotalPages = result.data.pagination.pages;
            
            renderCommentsTable();
            renderCommentsPagination(total, page);
            
            // 确保展开按钮工作正常
            setTimeout(() => {
                bindCommentExpansionEvents();
            }, 100);
        } else {
            throw new Error(result.message);
        }
    } catch (err) {
        console.error('加载评论失败:', err);
        
        // 显示空状态
        const tbody = document.getElementById('song-comments-body');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="empty-state">
                        <i class="fas fa-comment-slash"></i>
                        <p>暂无评论</p>
                    </td>
                </tr>
            `;
        }
    }
}

function bindCommentExpansionEvents() {
    // 事件委托方式处理展开按钮点击
    const tableBody = document.getElementById('song-comments-body');
    if (tableBody) {
        tableBody.addEventListener('click', (e) => {
            if (e.target.classList.contains('expand-toggle') || 
                e.target.closest('.expand-toggle')) {
                const expandBtn = e.target.classList.contains('expand-toggle') ? 
                    e.target : e.target.closest('.expand-toggle');
                if (expandBtn && expandBtn.id) {
                    const commentId = expandBtn.id.replace('expand-btn-', '');
                    toggleCommentExpansion(parseInt(commentId));
                }
            }
        });
    }
}

// ==================== 渲染评论表格 ====================
function renderCommentsTable() {
    const tbody = document.getElementById('song-comments-body');
    
    if (!currentSongComments || currentSongComments.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="empty-state">
                    <i class="fas fa-comment-slash"></i>
                    <p>暂无评论</p>
                </td>
            </tr>
        `;
        return;
    }
    
    tbody.innerHTML = currentSongComments.map(comment => {
        // 确定情感标签类别
        let sentimentClass = 'sentiment-neutral';
        let sentimentLabel = '中性';
        
        if (comment.is_positive === true || comment.sentiment_score > 0.6) {
            sentimentClass = 'sentiment-positive';
            sentimentLabel = '正面';
        } else if (comment.is_positive === false || comment.sentiment_score < 0.4) {
            sentimentClass = 'sentiment-negative';
            sentimentLabel = '负面';
        }
        
        // 格式化时间
        const commentTime = comment.comment_time ? new Date(comment.comment_time).toLocaleString() : '-';
        
        // 获取评论内容，处理可能的换行
        let content = comment.content || '';
        content = content.replace(/\n/g, '<br>');
        
        // 计算是否需要展开按钮（基于内容长度和高度）
        const contentLength = comment.content ? comment.content.length : 0;
        const needsExpand = contentLength > 100; // 超过100字符显示展开按钮
        
        return `
        <tr>
            <td><code>${comment.comment_id}</code></td>
            <td>${comment.user_nickname}</td>
            <td>
                <div class="comment-content-wrapper">
                    <div class="comment-content ${needsExpand ? '' : 'expanded'}" 
                         id="comment-content-${comment.comment_id}"
                         style="${needsExpand ? 'max-height: 60px;' : ''}">
                        ${content}
                    </div>
                    ${needsExpand ? `
                        <button class="expand-toggle" 
                                id="expand-btn-${comment.comment_id}"
                                onclick="toggleCommentExpansion(${comment.comment_id})">
                            <i class="fas fa-expand-alt"></i> 展开
                        </button>
                    ` : ''}
                </div>
            </td>
            <td>${comment.liked_count}</td>
            <td>${commentTime}</td>
            <td>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <input type="range" class="sentiment-slider" 
                           min="0" max="1" step="0.01" value="${comment.sentiment_score.toFixed(2)}"
                           style="width: 100px;"
                           onchange="updateCommentSentiment(${comment.comment_id}, this.value)">
                    <span style="min-width: 40px; font-weight: 600; color: var(--primary-color);">
                        ${comment.sentiment_score.toFixed(2)}
                    </span>
                    <span class="sentiment-badge ${sentimentClass}" style="font-size: 0.8rem;">
                        ${sentimentLabel}
                    </span>
                </div>
            </td>
            <td>
                <button class="btn btn-sm btn-danger" onclick="deleteCommentAdmin(${comment.comment_id})" title="删除">
                    <i class="fas fa-trash"></i>
                </button>
            </td>
        </tr>
        `;
    }).join('');
}

// ==================== 添加HTML转义函数 ====================
function escapeHtml(text) {
    if (!text) return '';
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;')
        .replace(/\n/g, '<br>');
}

// ==================== 评论操作函数 ====================
async function updateCommentSentiment(commentId, value) {
    try {
        const res = await fetch(`${API_BASE_URL}/admin/comments/${commentId}/sentiment`, {
            method: 'PUT',
            headers: {
                'Authorization': `Bearer ${adminToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ sentiment_score: parseFloat(value) })
        });
        
        const result = await res.json();
        
        if (result.success) {
            // 重新加载评论列表
            const songId = document.getElementById('edit-song-id').value;
            loadSongComments(songId, currentCommentsPage);
            loadSongCommentsStats(songId);
            
            showNotification('情感值已更新', 'success');
        } else {
            throw new Error(result.message);
        }
    } catch (err) {
        showNotification('更新失败: ' + err.message, 'error');
    }
}

async function deleteCommentAdmin(commentId) {
    if (!confirm('确定要删除这条评论吗？此操作不可恢复。')) return;
    
    try {
        const res = await fetch(`${API_BASE_URL}/admin/comments/${commentId}`, {
            method: 'DELETE',
            headers: {'Authorization': `Bearer ${adminToken}`}
        });
        
        const result = await res.json();
        
        if (result.success) {
            showNotification('评论已删除', 'success');
            
            // 重新加载评论列表和统计
            const songId = document.getElementById('edit-song-id').value;
            loadSongComments(songId, currentCommentsPage);
            loadSongCommentsStats(songId);
        } else {
            throw new Error(result.message);
        }
    } catch (err) {
        showNotification('删除失败: ' + err.message, 'error');
    }
}

// ==================== 评论分页 ====================
function renderCommentsPagination(total, current) {
    const container = document.getElementById('comments-pagination');
    if (!container) return;
    
    if (total <= 10) {
        container.innerHTML = '';
        return;
    }
    
    let html = `<button class="page-btn" onclick="loadCommentsPage(${current - 1})" ${current === 1 ? 'disabled' : ''}><i class="fas fa-chevron-left"></i></button>`;
    
    const maxButtons = 5;
    let startPage = Math.max(1, current - Math.floor(maxButtons / 2));
    let endPage = Math.min(commentsTotalPages, startPage + maxButtons - 1);
    
    if (endPage - startPage < maxButtons - 1) {
        startPage = Math.max(1, endPage - maxButtons + 1);
    }
    
    if (startPage > 1) {
        html += `<button class="page-btn" onclick="loadCommentsPage(1)">1</button>`;
        if (startPage > 2) html += `<span class="page-info">...</span>`;
    }
    
    for (let i = startPage; i <= endPage; i++) {
        html += `<button class="page-btn ${i === current ? 'active' : ''}" onclick="loadCommentsPage(${i})">${i}</button>`;
    }
    
    if (endPage < commentsTotalPages) {
        if (endPage < commentsTotalPages - 1) html += `<span class="page-info">...</span>`;
        html += `<button class="page-btn" onclick="loadCommentsPage(${commentsTotalPages})">${commentsTotalPages}</button>`;
    }
    
    html += `<button class="page-btn" onclick="loadCommentsPage(${current + 1})" ${current === commentsTotalPages ? 'disabled' : ''}><i class="fas fa-chevron-right"></i></button>`;
    html += `<span class="page-info">共 ${commentsTotalPages} 页，${total} 条评论</span>`;
    
    container.innerHTML = html;
}

function loadCommentsPage(page) {
    const songId = document.getElementById('edit-song-id').value;
    loadSongComments(songId, page);
}

// ==================== 选项卡切换函数 ====================
function switchEditTab(tabName) {
    // 隐藏所有选项卡
    document.querySelectorAll('.edit-tab').forEach(tab => {
        tab.style.display = 'none';
    });
    
    // 移除所有选项卡按钮的激活状态
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.style.color = 'var(--text-secondary)';
        btn.style.borderBottom = 'none';
    });
    
    // 显示选中的选项卡
    const activeTab = document.getElementById(`${tabName}-tab`);
    if (activeTab) {
        activeTab.style.display = 'block';
    }
    
    // 激活对应的选项卡按钮
    const activeBtn = document.querySelector(`[onclick="switchEditTab('${tabName}')"]`);
    if (activeBtn) {
        activeBtn.style.color = 'var(--primary-color)';
        activeBtn.style.borderBottom = '2px solid var(--primary-color)';
    }
    
    // 如果是评论选项卡，加载评论数据
    if (tabName === 'comments') {
        const songId = document.getElementById('edit-song-id').value;
        loadSongComments(songId, 1);
    }
}

// ==================== 评论展开/收起功能 ====================
function toggleCommentExpansion(commentId) {
    const contentElement = document.getElementById(`comment-content-${commentId}`);
    const expandBtn = document.getElementById(`expand-btn-${commentId}`);
    
    if (!contentElement) return;
    
    const isExpanded = contentElement.classList.contains('expanded');
    
    if (isExpanded) {
        // 收起评论
        contentElement.classList.remove('expanded');
        if (expandBtn) {
            expandBtn.innerHTML = '<i class="fas fa-expand-alt"></i> 展开';
        }
    } else {
        // 展开评论
        contentElement.classList.add('expanded');
        if (expandBtn) {
            expandBtn.innerHTML = '<i class="fas fa-compress-alt"></i> 收起';
        }
    }
}

// ==================== 修改评论表格渲染函数 ====================
function renderCommentsTable() {
    const tbody = document.getElementById('song-comments-body');
    
    if (!currentSongComments || currentSongComments.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="empty-state">
                    <i class="fas fa-comment-slash"></i>
                    <p>暂无评论</p>
                </td>
            </tr>
        `;
        return;
    }
    
    tbody.innerHTML = currentSongComments.map(comment => {
        // 确定情感标签类别
        let sentimentClass = 'sentiment-neutral';
        let sentimentLabel = '中性';
        
        if (comment.is_positive === true || comment.sentiment_score > 0.6) {
            sentimentClass = 'sentiment-positive';
            sentimentLabel = '正面';
        } else if (comment.is_positive === false || comment.sentiment_score < 0.4) {
            sentimentClass = 'sentiment-negative';
            sentimentLabel = '负面';
        }
        
        // 格式化时间
        const commentTime = comment.comment_time ? new Date(comment.comment_time).toLocaleString() : '-';
        
        // 判断是否需要展开按钮（超过100个字符或3行）
        const needsExpand = comment.content.length > 100;
        const displayContent = escapeHtml(comment.content);
        
        return `
        <tr>
            <td><code>${comment.comment_id}</code></td>
            <td>${comment.user_nickname}</td>
            <td>
                <div class="comment-content-container">
                    <div class="comment-content ${needsExpand ? '' : 'expanded'}" 
                         id="comment-content-${comment.comment_id}">
                        ${displayContent}
                    </div>
                    ${needsExpand ? `
                        <button class="expand-btn" id="expand-btn-${comment.comment_id}" 
                                onclick="toggleCommentExpansion(${comment.comment_id})">
                            <i class="fas fa-expand-alt"></i> 展开
                        </button>
                    ` : ''}
                </div>
            </td>
            <td>${comment.liked_count}</td>
            <td>${commentTime}</td>
            <td>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <input type="range" class="sentiment-slider" 
                           min="0" max="1" step="0.01" value="${comment.sentiment_score.toFixed(2)}"
                           style="width: 100px;"
                           onchange="updateCommentSentiment(${comment.comment_id}, this.value)">
                    <span style="min-width: 40px; font-weight: 600; color: var(--primary-color);">
                        ${comment.sentiment_score.toFixed(2)}
                    </span>
                    <span class="sentiment-badge ${sentimentClass}" style="font-size: 0.8rem;">
                        ${sentimentLabel}
                    </span>
                </div>
            </td>
            <td>
                <button class="btn btn-sm btn-danger" onclick="deleteCommentAdmin(${comment.comment_id})" title="删除">
                    <i class="fas fa-trash"></i>
                </button>
            </td>
        </tr>
        `;
    }).join('');
}
