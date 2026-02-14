#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
扫描 MP3-Example 文件夹，将音频文件信息导入 audio_files 表
依赖库：pandas, sqlalchemy, pyodbc, tqdm
"""

import os
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from tqdm import tqdm
from pathlib import Path

# ========== 配置区域（请根据实际情况修改）==========
DB_CONFIG = {
    'server': 'localhost',
    'database': 'MusicRecommendationDB',
    'username': 'sa',
    'password': '123456',      # ← 修改为您的密码
    'driver': 'ODBC Driver 18 for SQL Server'
}

AUDIO_ROOT = r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\数据集1\MP3-Example"
# =================================================

def get_db_engine():
    """创建数据库引擎"""
    conn_str = (f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
                f"?driver={DB_CONFIG['driver'].replace(' ', '+')}&Encrypt=no")
    return create_engine(conn_str, echo=False)

def scan_audio_files(root_dir):
    """
    扫描音频文件，返回列表，每项包含：
    - track_id: 从文件名提取的 ID
    - genre: 所在文件夹名称
    - filename: 完整文件名
    - file_path: 完整绝对路径
    """
    audio_files = []
    root_path = Path(root_dir)
    
    # 检查目录是否存在
    if not root_path.exists():
        print(f"❌ 目录不存在: {root_dir}")
        return audio_files
    
    # 遍历所有 .mp3 文件
    mp3_files = list(root_path.rglob("*.mp3"))
    print(f"🔍 找到 {len(mp3_files)} 个 MP3 文件，正在解析...")
    
    for file_path in tqdm(mp3_files, desc="解析文件"):
        # 获取流派（父文件夹名）
        genre = file_path.parent.name
        
        # 获取文件名（不含扩展名）
        stem = file_path.stem
        
        # 解析 track_id（格式：流派-track_id）
        # 示例：Blues-TRACOHF128F1498509 → TRACOHF128F1498509
        if '-' in stem:
            # 去掉第一个连字符及之前的部分
            track_id = stem.split('-', 1)[1]
        else:
            # 如果文件名不符合预期，跳过
            print(f"⚠️ 文件名格式异常，跳过: {file_path.name}")
            continue
        
        audio_files.append({
            'track_id': track_id,
            'genre': genre,
            'filename': file_path.name,
            'file_path': str(file_path.absolute())
        })
    
    return audio_files

def import_to_sql(engine, audio_data):
    """将音频数据批量插入 audio_files 表（存在则忽略）"""
    if not audio_data:
        print("⚠️ 没有可导入的数据")
        return 0
    
    df = pd.DataFrame(audio_data)
    
    # 去重（防止同一 track_id 重复）
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['track_id'])
    after_dedup = len(df)
    if after_dedup < before_dedup:
        print(f"🧹 去重: {before_dedup - after_dedup} 条重复 track_id")
    
    # 分批插入（每批 1000 条）
    batch_size = 1000
    total = len(df)
    success = 0
    
    print(f"⏳ 开始导入 {total} 条音频记录到 audio_files 表...")
    
    with engine.begin() as conn:
        for i in range(0, total, batch_size):
            batch = df.iloc[i:i+batch_size]
            try:
                # 使用 if_exists='append' 追加
                batch.to_sql('audio_files', conn, if_exists='append', index=False)
                success += len(batch)
                print(f"   进度: {success}/{total} ({success/total*100:.1f}%)")
            except Exception as e:
                print(f"   ❌ 批次导入失败，尝试逐条插入...")
                # 逐条插入，跳过主键冲突等错误
                for _, row in batch.iterrows():
                    try:
                        row.to_frame().T.to_sql('audio_files', conn, if_exists='append', index=False)
                        success += 1
                    except Exception as e2:
                        print(f"     跳过 {row['track_id']}: {e2}")
                print(f"   进度: {success}/{total} ({success/total*100:.1f}%)")
    
    return success

def verify_import(engine):
    """验证导入结果"""
    try:
        count_df = pd.read_sql("SELECT COUNT(*) as cnt FROM audio_files", engine)
        count = count_df.iloc[0]['cnt']
        print(f"\n📊 audio_files 表当前记录数: {count}")
        
        # 显示前5条示例
        sample = pd.read_sql("SELECT TOP 5 track_id, genre, filename FROM audio_files", engine)
        print("\n📋 示例数据:")
        print(sample.to_string(index=False))
        
        return count
    except Exception as e:
        print(f"⚠️ 验证失败: {e}")
        return 0

def main():
    print("="*80)
    print("🎵 音频文件扫描与导入工具")
    print("="*80)
    
    # 1. 扫描文件
    audio_data = scan_audio_files(AUDIO_ROOT)
    if not audio_data:
        print("❌ 未找到有效音频文件，程序退出")
        return
    
    print(f"\n✅ 解析完成，共 {len(audio_data)} 条有效记录")
    
    # 2. 连接数据库
    try:
        engine = get_db_engine()
        # 检查 audio_files 表是否存在
        inspector = inspect(engine)
        if 'audio_files' not in inspector.get_table_names():
            print("❌ 数据库中没有 audio_files 表，请先执行建表语句")
            return
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return
    
    # 3. 导入数据
    success = import_to_sql(engine, audio_data)
    
    # 4. 验证
    if success > 0:
        verify_import(engine)
    
    print("\n" + "="*80)
    print("🎉 音频文件导入完成！")
    print("="*80)

if __name__ == "__main__":
    main()