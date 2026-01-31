import os
import shutil

def create_project():
    # 基础路径（假设此脚本放在 毕业设计 文件夹下运行）
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.join(base_dir, 'music_recommendation_api')
    
    print(f"创建Flask项目于: {project_dir}")
    
    # 创建目录结构
    dirs = [
        os.path.join(project_dir, 'routes'),
        os.path.join(project_dir, 'utils'),
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        # 创建__init__.py
        init_file = os.path.join(d, '__init__.py')
        if not os.path.exists(init_file):
            with open(init_file, 'w', encoding='utf-8') as f:
                f.write('')
    
    # 创建空文件占位（防止后续导入错误）
    files = {
        os.path.join(project_dir, 'routes', 'recommendation.py'): '# 推荐路由\n',
        os.path.join(project_dir, 'routes', 'user.py'): '# 用户路由\n',
        os.path.join(project_dir, 'routes', 'song.py'): '# 歌曲路由\n',
        os.path.join(project_dir, 'utils', 'response.py'): '# 响应工具\n',
        os.path.join(project_dir, 'utils', 'validators.py'): '# 验证工具\n',
        os.path.join(project_dir, '.env'): '# 环境变量配置\n',
        os.path.join(project_dir, '.env.example'): '# 环境变量示例\n',
    }
    
    for filepath, content in files.items():
        if not os.path.exists(filepath):
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"创建: {filepath}")
    
    print("\n项目结构创建完成！")
    print(f"请将 Music Recommender Service 代码复制到: {project_dir}")

if __name__ == '__main__':
    create_project()