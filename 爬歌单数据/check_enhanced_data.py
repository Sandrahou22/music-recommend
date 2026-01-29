# check_enhanced_data.py检查结果
import pandas as pd

def check_data_quality(filename):
    """检查增强版数据的质量"""
    try:
        df = pd.read_csv(filename)
        
        print("="*60)
        print(f"数据质量检查: {filename}")
        print(f"总记录数: {len(df)}")
        print("="*60)
        
        # 检查字段完整性
        print("\n字段完整性:")
        for col in df.columns:
            missing = df[col].isnull().sum()
            if missing > 0:
                print(f"  ❌ {col}: {missing} 个缺失值 ({missing/len(df)*100:.1f}%)")
            else:
                print(f"  ✅ {col}: 完整")
        
        # 流派分布
        if 'genre' in df.columns:
            print(f"\n流派分布:")
            genre_counts = df['genre'].value_counts()
            for genre, count in genre_counts.items():
                percentage = count/len(df)*100
                print(f"  {genre}: {count} 首 ({percentage:.1f}%)")
        
        # 评论数统计
        if 'comment_count' in df.columns:
            print(f"\n评论数统计:")
            print(f"  总评论数: {df['comment_count'].sum():,}")
            print(f"  平均每首: {df['comment_count'].mean():.0f}")
            print(f"  中位数: {df['comment_count'].median():.0f}")
            print(f"  最大值: {df['comment_count'].max():,}")
            print(f"  最小值: {df['comment_count'].min():,}")
            
            # 评论数分布
            comment_ranges = {
                '0-99': ((df['comment_count'] >= 0) & (df['comment_count'] < 100)).sum(),
                '100-999': ((df['comment_count'] >= 100) & (df['comment_count'] < 1000)).sum(),
                '1000-9999': ((df['comment_count'] >= 1000) & (df['comment_count'] < 10000)).sum(),
                '10000+': (df['comment_count'] >= 10000).sum(),
            }
            print(f"\n评论数分布:")
            for rng, count in comment_ranges.items():
                print(f"  {rng}: {count} 首 ({count/len(df)*100:.1f}%)")
        
        # 显示样本数据
        print(f"\n样本数据（前3行）:")
        print(df.head(3).to_string())
        
        return True
        
    except Exception as e:
        print(f"读取文件失败: {e}")
        return False

if __name__ == "__main__":
    check_data_quality("data/final_songs_enhanced.csv")