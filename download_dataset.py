import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

def download_dataset():
    """使用Kaggle API直接下载数据集"""
    target_dir = r"C:\Users\小侯\Desktop\学校作业\数据集"
    
    # 确保目录存在
    os.makedirs(target_dir, exist_ok=True)
    
    print(f"正在下载数据集到: {target_dir}")
    
    try:
        # 初始化Kaggle API
        api = KaggleApi()
        api.authenticate()  # 这会自动读取C:\Users\小侯\.kaggle\kaggle.json
        
        # 下载数据集
        api.dataset_download_files(
            dataset="undefinenull/million-song-dataset-spotify-lastfm",
            path=target_dir,
            unzip=False  # 先不自动解压，避免大文件解压问题
        )
        
        print("下载成功！")
        
        # 检查下载的文件
        for file in os.listdir(target_dir):
            if file.endswith('.zip'):
                print(f"下载的文件: {file}")
                # 如果需要解压
                # zip_path = os.path.join(target_dir, file)
                # with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                #     zip_ref.extractall(target_dir)
                # print(f"已解压: {file}")
        
    except Exception as e:
        print(f"下载失败，错误信息: {e}")
        print("\n可能的原因：")
        print("1. 网络连接问题")
        print("2. 数据集名称错误")
        print("3. Kaggle API密钥问题")
        return False
    
    return True

if __name__ == "__main__":
    download_dataset()
    input("按回车键退出...")