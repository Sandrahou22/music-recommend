#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
扫描 MP3-Example 文件夹，将音频文件信息导入 audio_files 表，
并更新 enhanced_song_features 表的 audio_path 字段。
使用 original_song_id 进行关联。
自动尝试多种 ODBC 驱动。
"""

import os
import pyodbc
from pathlib import Path
import logging

# ========== 配置区域 ==========
AUDIO_ROOT = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集1\MP3-Example"
SERVER = 'localhost'
DATABASE = 'MusicRecommendationDB'
# 常用驱动列表，按优先级排序
DRIVERS = [
    'ODBC Driver 17 for SQL Server',
    'ODBC Driver 13 for SQL Server',
    'ODBC Driver 11 for SQL Server',
    'SQL Server Native Client 11.0',
    'SQL Server'
]
# ==============================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_connection():
    """尝试使用不同驱动连接数据库，返回第一个成功的连接"""
    for driver in DRIVERS:
        try:
            conn_str = f'DRIVER={{{driver}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            logger.info(f"成功使用驱动: {driver}")
            return conn
        except Exception as e:
            logger.debug(f"驱动 {driver} 连接失败: {e}")
            continue
    raise Exception("无法找到可用的 ODBC 驱动，请安装 SQL Server ODBC 驱动（例如 ODBC Driver 17 for SQL Server）。")

def scan_audio_files(root_dir):
    """
    扫描所有子文件夹中的 .mp3 文件，返回列表，每项为 (original_song_id, genre, filename, file_path)
    文件名格式要求：流派-original_song_id.mp3，例如 Blues-TRACOHF128F1498509.mp3
    """
    audio_files = []
    root_path = Path(root_dir)
    if not root_path.exists():
        logger.error(f"目录不存在: {root_dir}")
        return audio_files

    mp3_files = list(root_path.rglob("*.mp3"))
    logger.info(f"找到 {len(mp3_files)} 个 MP3 文件")

    for file_path in mp3_files:
        genre = file_path.parent.name          # 父文件夹名作为流派
        stem = file_path.stem                  # 文件名（不含扩展名）
        # 提取 original_song_id：去掉第一个连字符及之前的部分
        if '-' in stem:
            original_song_id = stem.split('-', 1)[1]
        else:
            logger.warning(f"文件名格式异常，跳过: {file_path.name}")
            continue
        audio_files.append((original_song_id, genre, file_path.name, str(file_path.absolute())))

    logger.info(f"解析完成，有效文件 {len(audio_files)} 个")
    return audio_files

def insert_audio_file(cursor, original_song_id, genre, filename, file_path):
    """插入 audio_files 表，如果唯一键冲突则忽略"""
    sql = """
        INSERT INTO audio_files (track_id, genre, filename, file_path)
        VALUES (?, ?, ?, ?)
    """
    try:
        cursor.execute(sql, (original_song_id, genre, filename, file_path))
        return True
    except pyodbc.IntegrityError as e:
        logger.debug(f"audio_files 跳过重复 original_song_id: {original_song_id}")
        return False
    except Exception as e:
        logger.error(f"audio_files 插入失败 {original_song_id}: {e}")
        return False

def update_song_audio_path(cursor, original_song_id, audio_path):
    """更新 enhanced_song_features 表的 audio_path 字段，使用 original_song_id 匹配"""
    sql = """
        UPDATE enhanced_song_features
        SET audio_path = ?
        WHERE original_song_id = ?
    """
    try:
        cursor.execute(sql, (audio_path, original_song_id))
        if cursor.rowcount > 0:
            logger.debug(f"更新 enhanced_song_features: {original_song_id} -> {audio_path}")
            return True
        else:
            logger.warning(f"未找到对应的歌曲: original_song_id = {original_song_id}")
            return False
    except Exception as e:
        logger.error(f"更新 enhanced_song_features 失败 {original_song_id}: {e}")
        return False

def main():
    logger.info("开始扫描音频文件...")
    files = scan_audio_files(AUDIO_ROOT)
    if not files:
        logger.error("没有有效文件，退出")
        return

    try:
        conn = get_connection()
        cursor = conn.cursor()
        logger.info("数据库连接成功")
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        return

    # 检查必要表是否存在
    cursor.execute("SELECT COUNT(*) FROM sys.tables WHERE name = 'audio_files'")
    if cursor.fetchone()[0] == 0:
        logger.error("数据库中没有 audio_files 表，请先创建")
        conn.close()
        return
    cursor.execute("SELECT COUNT(*) FROM sys.tables WHERE name = 'enhanced_song_features'")
    if cursor.fetchone()[0] == 0:
        logger.error("数据库中没有 enhanced_song_features 表")
        conn.close()
        return

    inserted_audio = 0
    skipped_audio = 0
    updated_song = 0
    not_found_song = 0
    total = len(files)

    logger.info(f"开始处理 {total} 个文件...")
    for idx, (original_song_id, genre, filename, file_path) in enumerate(files, 1):
        # 插入 audio_files 表
        if insert_audio_file(cursor, original_song_id, genre, filename, file_path):
            inserted_audio += 1
        else:
            skipped_audio += 1

        # 更新 enhanced_song_features 表的 audio_path
        if update_song_audio_path(cursor, original_song_id, file_path):
            updated_song += 1
        else:
            not_found_song += 1

        # 每 100 条提交一次
        if idx % 100 == 0:
            conn.commit()
            logger.info(f"进度: {idx}/{total} (audio_files 插入: {inserted_audio}, 跳过: {skipped_audio}; "
                        f"song 更新: {updated_song}, 未找到: {not_found_song})")

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("="*50)
    logger.info(f"处理完成！总计 {total} 个文件")
    logger.info(f"audio_files 表: 插入 {inserted_audio} 条, 跳过 {skipped_audio} 条（重复）")
    logger.info(f"enhanced_song_features 表: 更新 {updated_song} 条, 未找到 {not_found_song} 条")
    logger.info("="*50)

if __name__ == "__main__":
    main()