-- ==========================================================
-- 数据库：MusicRecommendationDB（完整重建版）
-- 说明：保留原字段名与类型，ID 仍为 VARCHAR(50)
--       数据处理时将生成短ID（如 U000001, S000001）存入
-- ==========================================================
USE master;
GO

-- 如果数据库已存在则删除（请谨慎操作）
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'MusicRecommendationDB')
    DROP DATABASE MusicRecommendationDB;
GO

-- 创建数据库
CREATE DATABASE MusicRecommendationDB;
GO

USE MusicRecommendationDB;
GO

-- ==================== 1. 歌曲特征表 ====================
CREATE TABLE enhanced_song_features (
    song_id VARCHAR(50) PRIMARY KEY,          -- 短ID，例如 S000001
    song_name NVARCHAR(500),
    artists NVARCHAR(450),
    album NVARCHAR(500),
    duration_ms INT,
    genre NVARCHAR(200),
    popularity FLOAT DEFAULT 0.0,
    language NVARCHAR(50),
    publish_year INT,
    
    -- 统计特征（由数据处理脚本计算）
    tag_score_mean FLOAT NULL,
    tag_count INT NULL,
    avg_sentiment FLOAT NULL,
    comment_count INT NULL,
    total_likes INT NULL,
    avg_similarity FLOAT NULL,
    max_similarity FLOAT NULL,
    similar_songs_count INT NULL,
    playlist_count INT NULL,
    avg_playlist_order FLOAT NULL,
    
    -- 音频特征
    danceability FLOAT DEFAULT 0.5,
    energy FLOAT DEFAULT 0.5,
    [key] INT NULL,
    loudness FLOAT NULL,
    mode INT NULL,
    speechiness FLOAT DEFAULT 0,
    acousticness FLOAT DEFAULT 0,
    instrumentalness FLOAT DEFAULT 0,
    liveness FLOAT DEFAULT 0,
    valence FLOAT DEFAULT 0.5,
    tempo FLOAT DEFAULT 120,
    time_signature INT DEFAULT 4,
    
    -- 衍生特征（由数据处理脚本计算）
    song_age INT NULL,
    duration_minutes FLOAT NULL,
    popularity_group NVARCHAR(50) NULL,
    energy_dance FLOAT NULL,
    mood_score FLOAT NULL,
    
    -- 增强阶段计算的特征（推荐系统必需）
    final_popularity FLOAT NULL,
    final_popularity_norm FLOAT NULL,
    recency_score FLOAT NULL,
    genre_clean NVARCHAR(200) NULL,
    popularity_tier NVARCHAR(50) NULL,
    
    -- 新增歌曲统计特征（原脚本缺失，现补充）
    weight_sum FLOAT DEFAULT 0.0,
    weight_mean FLOAT DEFAULT 0.0,
    weight_std FLOAT DEFAULT 0.0,
    unique_users INT DEFAULT 0,
    
    -- 关联原始ID（用于追溯）
    original_song_id VARCHAR(100) NULL,
    
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
GO

-- ==================== 2. 用户特征表 ====================
CREATE TABLE enhanced_user_features (
    user_id VARCHAR(50) PRIMARY KEY,          -- 短ID，例如 U000001
    nickname NVARCHAR(100),
    gender INT NULL,
    age INT NULL,
    province NVARCHAR(50),
    city NVARCHAR(50),
    listen_songs INT DEFAULT 0,
    source VARCHAR(20),                      -- internal / external
    
    -- 行为统计特征（由数据处理脚本计算）
    unique_songs INT DEFAULT 0,
    total_interactions INT DEFAULT 0,
    total_weight_sum FLOAT DEFAULT 0.0,
    avg_weight FLOAT DEFAULT 0.0,
    weight_std FLOAT NULL,                  -- 标准差，单个交互时为NULL
    
    -- 衍生特征（由数据处理脚本计算）
    age_group NVARCHAR(50) NULL,
    activity_level NVARCHAR(50) NULL,
    diversity_ratio FLOAT DEFAULT 0.0,
    
    -- 用户偏好特征（由数据处理脚本计算）
    top_genre_1 NVARCHAR(200) NULL,
    top_genre_2 NVARCHAR(200) NULL,
    top_genre_3 NVARCHAR(200) NULL,
    avg_popularity_pref FLOAT NULL,
    popularity_bias FLOAT NULL,
    
    -- 关联原始ID（用于追溯）
    original_user_id VARCHAR(100) NULL,
    
    -- 角色（用于管理员）
    role VARCHAR(20) DEFAULT 'user',
    
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
GO

-- ==================== 3. 交互矩阵表 ====================
CREATE TABLE filtered_interactions (
    interaction_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    song_id VARCHAR(50) NOT NULL,
    total_weight FLOAT DEFAULT 0.0,
    interaction_types NVARCHAR(200),
    created_at DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (user_id) REFERENCES enhanced_user_features(user_id),
    FOREIGN KEY (song_id) REFERENCES enhanced_song_features(song_id)
);
GO

-- ==================== 4. 训练集表 ====================
CREATE TABLE train_interactions (
    train_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    song_id VARCHAR(50) NOT NULL,
    total_weight FLOAT DEFAULT 0.0,
    interaction_types NVARCHAR(200),
    created_at DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (user_id) REFERENCES enhanced_user_features(user_id),
    FOREIGN KEY (song_id) REFERENCES enhanced_song_features(song_id)
);
GO

-- ==================== 5. 测试集表 ====================
CREATE TABLE test_interactions (
    test_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    song_id VARCHAR(50) NOT NULL,
    total_weight FLOAT DEFAULT 0.0,
    interaction_types NVARCHAR(200),
    created_at DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (user_id) REFERENCES enhanced_user_features(user_id),
    FOREIGN KEY (song_id) REFERENCES enhanced_song_features(song_id)
);
GO

-- ==================== 6. 推荐结果表 ====================
CREATE TABLE recommendations (
    recommendation_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    song_id VARCHAR(50) NOT NULL,
    recommendation_score FLOAT DEFAULT 0.0,
    algorithm_type VARCHAR(50),
    rank_position INT DEFAULT 0,
    is_viewed BIT DEFAULT 0,
    is_clicked BIT DEFAULT 0,
    is_listened BIT DEFAULT 0,
    created_at DATETIME DEFAULT GETDATE(),
    expires_at DATETIME NULL,
    
    -- 来源标识（方便分析）
    user_source VARCHAR(10) NULL,
    song_source VARCHAR(10) NULL,
    
    FOREIGN KEY (user_id) REFERENCES enhanced_user_features(user_id),
    FOREIGN KEY (song_id) REFERENCES enhanced_song_features(song_id),
    CONSTRAINT UQ_recommendation UNIQUE(user_id, song_id, created_at)
);
GO

-- ==================== 7. 原始行为日志表 ====================
CREATE TABLE user_song_interaction (
    interaction_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    song_id VARCHAR(50) NOT NULL,
    behavior_type VARCHAR(20),
    [weight] FLOAT DEFAULT 1.0,
    [timestamp] DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (user_id) REFERENCES enhanced_user_features(user_id),
    FOREIGN KEY (song_id) REFERENCES enhanced_song_features(song_id)
);
GO

-- ==================== 8. 歌曲ID映射表 ====================
CREATE TABLE song_id_mapping (
    mapping_id INT IDENTITY(1,1) PRIMARY KEY,
    original_song_id VARCHAR(100) NOT NULL,
    unified_song_id VARCHAR(50) NOT NULL,
    song_name NVARCHAR(300) NULL,
    artists NVARCHAR(300) NULL,
    match_type VARCHAR(20) NULL,
    confidence FLOAT DEFAULT 1.0,
    source_file VARCHAR(100) NULL,
    created_at DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT UQ_song_mapping_original UNIQUE(original_song_id),
    FOREIGN KEY (unified_song_id) REFERENCES enhanced_song_features(song_id)
);
GO

-- ==================== 9. 歌曲评论表 ====================
CREATE TABLE song_comments (
    comment_id INT IDENTITY(1,1) PRIMARY KEY,
    unified_song_id VARCHAR(50) NOT NULL,
    original_comment_id VARCHAR(100) NULL,
    original_user_id VARCHAR(100) NULL,
    user_nickname NVARCHAR(150) NULL,
    content NVARCHAR(MAX) NOT NULL,
    liked_count INT DEFAULT 0,
    comment_time DATETIME NULL,
    sentiment_score FLOAT NULL,
    is_positive TINYINT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (unified_song_id) REFERENCES enhanced_song_features(song_id) ON DELETE CASCADE
);
GO

-- ==================== 10. 评论点赞表 ====================
CREATE TABLE comment_likes (
    like_id INT IDENTITY(1,1) PRIMARY KEY,
    comment_id INT NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (comment_id) REFERENCES song_comments(comment_id) ON DELETE CASCADE,
    CONSTRAINT UQ_comment_likes UNIQUE(comment_id, user_id)
);
GO

-- ==================== 11. 音频文件索引表 ====================
CREATE TABLE audio_files (
    audio_id INT IDENTITY(1,1) PRIMARY KEY,
    track_id VARCHAR(50) NOT NULL UNIQUE,
    genre NVARCHAR(100) NOT NULL,
    filename NVARCHAR(255) NOT NULL,
    file_path NVARCHAR(500) NOT NULL,
    file_exists BIT DEFAULT 1,
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- ==================== 12. 算法性能统计表 ====================
CREATE TABLE algorithm_performance_stats (
    id INT IDENTITY(1,1) PRIMARY KEY,
    algorithm_type VARCHAR(50) NOT NULL,
    metric_date DATE NOT NULL,
    recall_rate FLOAT DEFAULT 0.0,
    precision_rate FLOAT DEFAULT 0.0,
    diversity_score FLOAT DEFAULT 0.0,
    ctr_rate FLOAT DEFAULT 0.0,
    listen_rate FLOAT DEFAULT 0.0,
    total_recommendations INT DEFAULT 0,
    clicks INT DEFAULT 0,
    listens INT DEFAULT 0,
    created_at DATETIME DEFAULT GETDATE(),
    UNIQUE(algorithm_type, metric_date)
);
GO

-- ==================== 13. 系统配置表 ====================
CREATE TABLE system_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    config_key VARCHAR(100) NOT NULL,
    config_value NVARCHAR(500) NOT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    UNIQUE (config_key)
);
GO

-- ==================== 创建索引 ====================
-- 歌曲表索引
CREATE INDEX idx_songs_genre ON enhanced_song_features(genre);
CREATE INDEX idx_songs_genre_clean ON enhanced_song_features(genre_clean);
CREATE INDEX idx_songs_popularity ON enhanced_song_features(final_popularity);
CREATE INDEX idx_songs_tier ON enhanced_song_features(popularity_tier);
CREATE INDEX idx_songs_artists ON enhanced_song_features(artists);

-- 用户表索引
CREATE INDEX idx_users_age ON enhanced_user_features(age);
CREATE INDEX idx_users_gender ON enhanced_user_features(gender);
CREATE INDEX idx_users_activity ON enhanced_user_features(activity_level);
CREATE INDEX idx_users_source ON enhanced_user_features(source);
CREATE INDEX idx_users_source_type ON enhanced_user_features(source);  -- 兼容原脚本

-- 交互表索引
CREATE INDEX idx_interactions_user ON filtered_interactions(user_id);
CREATE INDEX idx_interactions_song ON filtered_interactions(song_id);
CREATE INDEX idx_interactions_user_song ON filtered_interactions(user_id, song_id);

-- 训练/测试集索引
CREATE INDEX idx_train_user ON train_interactions(user_id);
CREATE INDEX idx_test_user ON test_interactions(user_id);

-- 推荐结果索引
CREATE INDEX idx_recommendations_user ON recommendations(user_id, created_at);
CREATE INDEX idx_recommendations_created ON recommendations(created_at);
CREATE INDEX idx_recs_source ON recommendations(user_source, song_source);

-- 映射表索引
CREATE INDEX IX_song_mapping_original ON song_id_mapping(original_song_id);
CREATE INDEX IX_song_mapping_unified ON song_id_mapping(unified_song_id);

-- 评论表索引
CREATE INDEX IX_comments_song ON song_comments(unified_song_id);
CREATE INDEX IX_comments_time ON song_comments(comment_time DESC);
CREATE INDEX IX_comments_sentiment ON song_comments(sentiment_score DESC);
CREATE INDEX IX_comments_likes ON song_comments(liked_count DESC);
CREATE INDEX IX_comments_positive ON song_comments(is_positive);

-- 点赞表索引
CREATE INDEX idx_comment_likes_comment ON comment_likes(comment_id);
CREATE INDEX idx_comment_likes_user ON comment_likes(user_id);

-- 音频文件索引
CREATE INDEX idx_audio_track ON audio_files(track_id);
CREATE INDEX idx_audio_genre ON audio_files(genre);
GO

-- ==================== 插入初始管理员用户 ====================
INSERT INTO enhanced_user_features (user_id, nickname, role, source, original_user_id, created_at, updated_at)
VALUES ('admin', '系统管理员', 'admin', 'internal', 'admin', GETDATE(), GETDATE());
GO

-- ==================== 插入系统配置 ====================
INSERT INTO system_config (config_key, config_value) VALUES
('recommendation.default_count', '20'),
('recommendation.expire_days', '7'),
('model.cold_start_strategy', 'popular'),
('system.version', '2.0');
GO

PRINT '数据库重建完成，所有表、索引、约束已创建。';
GO

-- 为歌曲表添加sentiment列（FLOAT类型，允许NULL）
ALTER TABLE enhanced_song_features 
ADD sentiment FLOAT NULL;
GO

-- 同时检查source列是否存在，若不存在也添加
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('enhanced_song_features') AND name = 'source')
BEGIN
    ALTER TABLE enhanced_song_features 
    ADD source VARCHAR(20) NULL;
END
GO

-- 添加 track_id 列
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('enhanced_song_features') AND name = 'track_id')
BEGIN
    ALTER TABLE enhanced_song_features 
    ADD track_id VARCHAR(50) NULL;
    PRINT '✓ 已添加 track_id 列';
END
ELSE
BEGIN
    PRINT '! track_id 列已存在';
END
GO

-- 添加 audio_path 列
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('enhanced_song_features') AND name = 'audio_path')
BEGIN
    ALTER TABLE enhanced_song_features 
    ADD audio_path NVARCHAR(500) NULL;
    PRINT '✓ 已添加 audio_path 列';
END
ELSE
BEGIN
    PRINT '! audio_path 列已存在';
END
GO

-- 创建索引加速后续查询
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_songs_track' AND object_id = OBJECT_ID('enhanced_song_features'))
BEGIN
    CREATE INDEX idx_songs_track ON enhanced_song_features(track_id);
    PRINT '✓ 已创建 idx_songs_track 索引';
END
GO

SELECT TOP 10 song_id, song_name, original_song_id 
FROM enhanced_song_features 
WHERE original_song_id IS NOT NULL;

-- 将 audio_files.file_path 更新到 enhanced_song_features.audio_path
UPDATE e
SET e.audio_path = a.file_path,
    e.track_id = a.track_id
FROM enhanced_song_features e
INNER JOIN audio_files a ON e.original_song_id = a.track_id
WHERE a.file_exists = 1;

SELECT DISTINCT genre FROM enhanced_song_features WHERE genre IS NOT NULL ORDER BY genre;
SELECT DISTINCT genre_clean FROM enhanced_song_features WHERE genre_clean IS NOT NULL ORDER BY genre_clean;

SELECT DISTINCT activity_level FROM enhanced_user_features WHERE activity_level IS NOT NULL ORDER BY activity_level;

select * from user_song_interaction

-- 假设实际听歌数为 102
UPDATE enhanced_user_features 
SET unique_songs = 102 
WHERE user_id = 'U001532';

-- 插入虚拟歌曲用于记录系统活动
IF NOT EXISTS (SELECT 1 FROM enhanced_song_features WHERE song_id = 'recommend_generate')
BEGIN
    INSERT INTO enhanced_song_features (
        song_id, 
        song_name, 
        artists, 
        album, 
        genre, 
        popularity, 
        final_popularity,
        audio_path,
        created_at,
        updated_at
    ) VALUES (
        'recommend_generate',
        '推荐生成',
        '系统',
        '系统',
        '系统',
        0,
        0,
        NULL,
        GETDATE(),
        GETDATE()
    );
    PRINT '虚拟歌曲记录已插入';
END
ELSE
    PRINT '虚拟歌曲记录已存在';
