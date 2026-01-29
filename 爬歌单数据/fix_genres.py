# fix_genres_enhanced.py
import pandas as pd
import re
import os
import glob

def fix_genre_classification(filename):
    """修复单个文件的流派分类错误"""
    print(f"\n🔧 正在修复文件: {filename}")
    
    try:
        # 读取数据
        df = pd.read_csv(filename, encoding='utf-8-sig')
    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        # 尝试其他编码
        try:
            df = pd.read_csv(filename, encoding='utf-8')
        except Exception as e2:
            print(f"❌ 使用UTF-8编码也失败: {e2}")
            return None
    
    original_count = len(df)
    print(f"原始数据: {original_count} 条记录")
    
    if 'genre' not in df.columns:
        print("❌ 文件中没有 'genre' 列")
        return None
    
    print("\n原始流派分布:")
    print(df['genre'].value_counts())
    
    # 修复规则
    def fix_genre(row):
        if pd.isna(row.get('genre')):
            return '未知'
        
        genre = str(row.get('genre', ''))
        song_name = str(row.get('song_name', '')) if 'song_name' in row else ''
        artists = str(row.get('artists', '')) if 'artists' in row else ''
        album = str(row.get('album', '')) if 'album' in row else ''
        
        # 规则1: Vocaloid误判修复
        if genre == 'Vocaloid':
            # 检查是否真的是中文歌曲
            if song_name and re.search(r'[\u4e00-\u9fff]', song_name):
                # 检查是否包含日文假名（真正的日语歌）
                if not re.search(r'[\u3040-\u309f\u30a0-\u30ff]', song_name):
                    # 规则1.1: 检查是否是影视原声
                    album_lower = album.lower()
                    if any(keyword in album_lower for keyword in ['ost', '原声', '电视剧', '电影', 'tv', '剧集']):
                        return '影视原声'
                    
                    # 规则1.2: 检查歌手是否包含乐队关键词
                    artists_lower = artists.lower()
                    if any(keyword in artists_lower for keyword in ['乐队', '乐团', 'band', '组合']):
                        # 进一步判断摇滚类型
                        if any(rock_word in song_name for rock_word in ['摇滚', 'rock']):
                            return '摇滚'
                        else:
                            return '华语流行'
                    
                    # 规则1.3: 检查是否是网络歌曲
                    if any(keyword in song_name for keyword in ['抖音', '快手', '热门', '神曲']):
                        return '华语流行'
                    
                    # 规则1.4: 检查是否是民谣
                    if any(keyword in song_name for keyword in ['民谣', 'folk', '乡村']):
                        return '民谣'
                    
                    # 默认改为华语流行
                    return '华语流行'
        
        # 规则2: 影视原声识别
        if genre != '影视原声':
            # 检查专辑名或歌名是否包含影视关键词
            album_lower = album.lower()
            song_lower = song_name.lower()
            if any(keyword in album_lower for keyword in ['ost', '原声', '电视剧', '电影', 'tv', '剧集', '主题曲', '插曲', '片尾曲']):
                return '影视原声'
            elif any(keyword in song_lower for keyword in ['ost', '原声', '电视剧', '电影', '主题曲', '插曲', '片尾曲']):
                return '影视原声'
        
        # 规则3: 摇滚识别
        if genre != '摇滚':
            artists_lower = artists.lower()
            if any(keyword in artists_lower for keyword in ['乐队', '乐团', 'band']):
                return '摇滚'
        
        # 规则4: 说唱识别
        if genre != '说唱':
            artists_lower = artists.lower()
            song_lower = song_name.lower()
            if any(keyword in artists_lower for keyword in ['rapper', '说唱', 'rap']):
                return '说唱'
            elif any(keyword in song_lower for keyword in ['rap', '说唱', '嘻哈']):
                return '说唱'
        
        # 规则5: 电子音乐识别
        if genre != '电子':
            artists_lower = artists.lower()
            if any(keyword in artists_lower for keyword in ['dj', '电音', 'electronic']):
                return '电子'
        
        # 规则6: 语言识别修正
        if genre == '华语流行' or genre == 'Vocaloid':
            # 如果是日语歌但被误判
            if re.search(r'[\u3040-\u309f\u30a0-\u30ff]', song_name):
                # 检查是否是Vocaloid
                if any(keyword in song_name for keyword in ['初音', 'ミク', 'Vocaloid', 'ボカロ']):
                    return 'Vocaloid'
                else:
                    return '日本流行'
        
        return genre
    
    # 应用修复
    df['genre_fixed'] = df.apply(fix_genre, axis=1)
    
    # 统计修复情况
    fixed_mask = df['genre'] != df['genre_fixed']
    fixed_count = fixed_mask.sum()
    
    print(f"\n✅ 修复了 {fixed_count} 条记录的流派分类 ({fixed_count/original_count*100:.1f}%)")
    
    if fixed_count > 0:
        print("\n📊 修复示例（前10条）:")
        fixed_examples = df[fixed_mask].head(10)
        for idx, row in fixed_examples.iterrows():
            print(f"  {row.get('song_name', 'N/A')[:30]:<30} | "
                  f"{row.get('artists', 'N/A')[:15]:<15} | "
                  f"{row['genre']} -> {row['genre_fixed']}")
    
    # 替换原始列
    df['genre'] = df['genre_fixed']
    df = df.drop('genre_fixed', axis=1)
    
    # 保存修复后的文件
    base_name = os.path.splitext(filename)[0]
    new_filename = f"{base_name}_fixed.csv"
    
    try:
        df.to_csv(new_filename, index=False, encoding='utf-8-sig')
        print(f"\n💾 修复后的数据已保存到: {new_filename}")
        
        # 显示修复后的流派分布
        print("\n📊 修复后流派分布:")
        genre_counts = df['genre'].value_counts()
        for genre, count in genre_counts.items():
            percentage = count/len(df)*100
            print(f"  {genre:<15}: {count:>5} 首 ({percentage:>5.1f}%)")
        
        return df
    except Exception as e:
        print(f"❌ 保存文件失败: {e}")
        return None

def batch_fix_directory(directory_path):
    """批量修复目录下的所有CSV文件"""
    # 查找所有CSV文件
    pattern = os.path.join(directory_path, "*.csv")
    csv_files = glob.glob(pattern)
    
    print(f"📂 在目录 {directory_path} 中找到 {len(csv_files)} 个CSV文件")
    
    for csv_file in csv_files:
        # 跳过已经修复过的文件（文件名包含_fixed）
        if '_fixed' in csv_file:
            continue
            
        print("\n" + "="*60)
        fix_genre_classification(csv_file)
        print("="*60)
    
    print(f"\n🎉 批量修复完成！")

def main():
    import sys
    
    print("="*70)
    print("🔧 网易云音乐流派分类修复工具")
    print("="*70)
    
    if len(sys.argv) < 2:
        print("\n使用方法:")
        print("  1. 修复单个文件: python fix_genres.py <文件名>")
        print("  2. 批量修复目录: python fix_genres.py --dir <目录名>")
        print("\n示例:")
        print("  python daily_crawler\fix_genres.py daily_data\20251229_songs.csv    ！！！！！！最终能运行版")
        print("  python fix_genres.py --dir daily_data")
        return
    
    if sys.argv[1] == '--dir' and len(sys.argv) > 2:
        directory = sys.argv[2]
        batch_fix_directory(directory)
    else:
        filename = sys.argv[1]
        fix_genre_classification(filename)

if __name__ == "__main__":
    main()