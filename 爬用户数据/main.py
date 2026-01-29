"""
网易云音乐用户信息爬虫 - 带路径处理和日期转换版本
可以处理任意位置的CSV文件，并将时间戳转换为日期格式
"""

import requests
import json
import time
import csv
import os
import sys
from datetime import datetime, timedelta
import random

def get_absolute_path(file_path):
    """获取文件的绝对路径"""
    # 如果是相对路径，转换为绝对路径
    if not os.path.isabs(file_path):
        # 获取当前工作目录
        current_dir = os.getcwd()
        # 尝试不同的可能路径
        possible_paths = [
            file_path,  # 直接路径
            os.path.join(current_dir, file_path),  # 当前目录下的文件
            os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path),  # 脚本目录下的文件
            os.path.join(current_dir, "数据集", "爬用户数据", file_path),  # 你的特定目录
            r"C:\\Users\\小侯\\Desktop\\学校作业\\毕业设计\\数据集\\爬用户数据\\collected_user_ids_20260119_173402.csv"  # 完整路径
        ]
        
        # 检查哪个路径存在
        for path in possible_paths:
            if os.path.exists(path):
                print(f"找到文件: {path}")
                return path
        
        # 如果都没找到，返回原始路径
        return file_path
    else:
        # 已经是绝对路径
        return file_path

class NetEaseCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://music.163.com/',
        })
        
        # 使用简单的Cookie
        self.cookies = {
            'NMTID': '00O7NExLq8UckSWGknpgjQffduQxR0AAAGb1D9htQ',
            'WM_TID': '24RrnhZfrCtEQEVAQVLQGjL7VSAb0A46',
        }
        
        # 设置cookies
        for key, value in self.cookies.items():
            self.session.cookies.set(key, value)
    
    def convert_timestamp_to_date(self, timestamp):
        """
        将时间戳转换为日期格式
        Args:
            timestamp: 毫秒时间戳（可以是字符串或数字）
        Returns:
            str: 日期字符串 (YYYY-MM-DD)
            str: 原始时间戳字符串（用于备份）
        """
        try:
            # 处理空值
            if timestamp is None or timestamp == '':
                return '', ''
            
            # 转换为整数
            ts = int(timestamp)
            
            # 处理无效时间戳（如默认值 -2209017600000）
            # 这是1900年1月1日的时间戳，通常是系统默认值
            if ts == -2209017600000:
                return '1900-01-01', str(ts)
            
            # 处理零或负数时间戳（除了上面已处理的-2209017600000）
            if ts <= 0:
                return '无效时间戳', str(ts)
            
            # 处理未来时间戳（比如超过当前时间+10年）
            current_ts = int(time.time() * 1000)
            if ts > (current_ts + 10 * 365 * 24 * 60 * 60 * 1000):  # 10年后的时间戳
                return '未来日期', str(ts)
            
            # 处理非常古老的时间戳（比如1900年之前）
            # 1970年1月1日的时间戳是0，我们设定一个合理的最小值
            min_valid_ts = -2209017600000  # 1900-01-01
            if ts < min_valid_ts:
                return '过旧日期', str(ts)
            
            # 转换为日期
            # 时间戳是毫秒，需要除以1000
            # 使用更安全的转换方式
            try:
                dt = datetime.fromtimestamp(ts / 1000)
            except (OSError, ValueError) as e:
                # 如果转换失败，尝试使用更宽松的方式
                # 打印调试信息
                print(f"  警告: 时间戳 {ts} 转换失败: {e}")
                
                # 尝试使用替代方法
                try:
                    # 从1970-01-01开始计算
                    dt = datetime(1970, 1, 1) + timedelta(seconds=ts / 1000)
                    # 检查日期是否合理
                    if dt.year < 1900 or dt.year > 2100:
                        return '日期超出范围', str(ts)
                except Exception as e2:
                    print(f"  替代方法也失败: {e2}")
                    return '转换失败', str(ts)
            
            # 格式化日期
            date_str = dt.strftime('%Y-%m-%d')
            return date_str, str(ts)
            
        except (ValueError, TypeError, OverflowError) as e:
            # 转换失败，返回原始值
            print(f"  时间戳转换异常: {e}, 原始值: {timestamp}")
            return '转换异常', str(timestamp) if timestamp else ''
    
    def get_user_age(self, birthday_date):
        """
        根据生日计算年龄（粗略计算）
        Args:
            birthday_date: 生日日期字符串 (YYYY-MM-DD)
        Returns:
            int: 年龄，如果无法计算则返回None
        """
        try:
            if not birthday_date or birthday_date == '1900-01-01' or birthday_date == '转换失败' or birthday_date == '无效日期' or birthday_date == '转换异常' or birthday_date == '未来日期' or birthday_date == '过旧日期' or birthday_date == '无效时间戳' or birthday_date == '日期超出范围':
                return None
            
            # 解析生日日期
            birth_date = datetime.strptime(birthday_date, '%Y-%m-%d')
            today = datetime.now()
            
            # 计算年龄
            age = today.year - birth_date.year
            
            # 如果今年生日还没过，年龄减1
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            
            # 检查年龄是否合理（0-120岁之间）
            if 0 <= age <= 120:
                return age
            else:
                return None
                
        except (ValueError, TypeError):
            return None
    
    def get_user_info(self, user_id):
        """获取用户信息"""
        url = f"https://music.163.com/api/v1/user/detail/{user_id}"
        
        try:
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get('code') == 200:
                        return data.get('profile', {})
                except:
                    return None
        except:
            return None
        
        return None
    
    def crawl_users(self, user_ids, output_file="user_data.csv"):
        """爬取用户信息"""
        results = []
        
        print(f"开始爬取 {len(user_ids)} 个用户...")
        
        for i, user_id in enumerate(user_ids, 1):
            print(f"[{i}/{len(user_ids)}] 处理用户 {user_id}")
            
            # 获取用户信息
            user_data = self.get_user_info(user_id)
            
            if user_data:
                # 调试信息：查看原始数据
                if i <= 5:  # 只打印前几个用户的原始数据用于调试
                    print(f"  原始数据 - birthday: {user_data.get('birthday')}, createTime: {user_data.get('createTime')}")
                
                # 转换时间戳为日期
                birthday_date, birthday_ts = self.convert_timestamp_to_date(user_data.get('birthday'))
                create_date, create_ts = self.convert_timestamp_to_date(user_data.get('createTime'))
                
                # 计算年龄
                age = self.get_user_age(birthday_date)
                
                # 整理数据
                processed_data = {
                    'user_id': user_data.get('userId', user_id),
                    'nickname': user_data.get('nickname', f'用户_{user_id}'),
                    'gender': user_data.get('gender', 0),
                    # 原始时间戳（备份）
                    'birthday_timestamp': birthday_ts,
                    # 转换后的日期
                    'birthday': birthday_date,
                    # 年龄（可选）
                    'age': age,
                    'province': user_data.get('province', ''),
                    'city': user_data.get('city', ''),
                    'signature': user_data.get('signature', ''),
                    'followeds': user_data.get('followeds', 0),
                    'follows': user_data.get('follows', 0),
                    'level': user_data.get('level', 0),
                    'vip_type': user_data.get('vipType', 0),
                    'listen_songs': user_data.get('listenSongs', 0),
                    # 原始时间戳（备份）
                    'create_time_timestamp': create_ts,
                    # 转换后的日期
                    'create_time': create_date,
                    'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                results.append(processed_data)
                
                # 显示用户信息摘要
                print(f"  ✓ {user_data.get('nickname', user_id)}")
                if birthday_date and birthday_date != '1900-01-01' and birthday_date != '转换失败' and birthday_date != '转换异常' and birthday_date != '无效时间戳' and birthday_date != '未来日期' and birthday_date != '过旧日期' and birthday_date != '日期超出范围':
                    print(f"      生日: {birthday_date}", end="")
                    if age is not None:
                        print(f" (约{age}岁)")
                    else:
                        print()
                if create_date and create_date != '1900-01-01' and create_date != '转换失败' and create_date != '转换异常' and create_date != '无效时间戳' and create_date != '未来日期' and create_date != '过旧日期' and create_date != '日期超出范围':
                    print(f"      注册: {create_date}")
            else:
                print(f"  ✗ 获取失败")
            
            # 延迟
            if i < len(user_ids):
                time.sleep(1 + random.random())
        
        return results
    
    def save_results(self, results, output_file):
        """保存结果"""
        if not results:
            print("没有数据可保存")
            return
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 定义CSV字段顺序
        fieldnames = [
            'user_id', 'nickname', 'gender', 'birthday', 'age',
            'birthday_timestamp', 'province', 'city', 'signature',
            'followeds', 'follows', 'level', 'vip_type', 'listen_songs',
            'create_time', 'create_time_timestamp', 'crawl_time'
        ]
        
        # 保存到CSV
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n数据已保存到: {output_file}")
        print(f"共 {len(results)} 条记录")
        
        # 打印数据统计
        self.print_statistics(results)
    
    def print_statistics(self, results):
        """打印数据统计信息"""
        print("\n" + "="*60)
        print("数据统计:")
        print("="*60)
        
        # 性别统计
        male = sum(1 for u in results if u['gender'] == 1)
        female = sum(1 for u in results if u['gender'] == 2)
        unknown_gender = sum(1 for u in results if u['gender'] == 0)
        
        # VIP统计
        vip = sum(1 for u in results if u['vip_type'] > 0)
        
        # 生日统计
        valid_birthdays = sum(1 for u in results if u['birthday'] and 
                              u['birthday'] != '1900-01-01' and 
                              u['birthday'] != '转换失败' and 
                              u['birthday'] != '转换异常' and
                              u['birthday'] != '无效时间戳' and
                              u['birthday'] != '未来日期' and
                              u['birthday'] != '过旧日期' and
                              u['birthday'] != '日期超出范围')
        default_birthdays = sum(1 for u in results if u['birthday'] == '1900-01-01')
        
        # 年龄统计
        valid_ages = [u['age'] for u in results if u['age'] is not None]
        if valid_ages:
            avg_age = sum(valid_ages) / len(valid_ages)
            min_age = min(valid_ages)
            max_age = max(valid_ages)
        
        # 注册时间统计
        valid_create_dates = sum(1 for u in results if u['create_time'] and 
                                 u['create_time'] != '1900-01-01' and 
                                 u['create_time'] != '转换失败' and 
                                 u['create_time'] != '转换异常' and
                                 u['create_time'] != '无效时间戳' and
                                 u['create_time'] != '未来日期' and
                                 u['create_time'] != '过旧日期' and
                                 u['create_time'] != '日期超出范围')
        
        print(f"总用户数: {len(results)}")
        print(f"\n性别分布:")
        print(f"  男性: {male} ({male/len(results)*100:.1f}%)")
        print(f"  女性: {female} ({female/len(results)*100:.1f}%)")
        print(f"  未知: {unknown_gender} ({unknown_gender/len(results)*100:.1f}%)")
        
        print(f"\nVIP用户: {vip} ({vip/len(results)*100:.1f}%)")
        
        print(f"\n生日信息:")
        print(f"  有效生日: {valid_birthdays} ({valid_birthdays/len(results)*100:.1f}%)")
        print(f"  默认生日(1900-01-01): {default_birthdays} ({default_birthdays/len(results)*100:.1f}%)")
        
        print(f"\n注册时间:")
        print(f"  有效注册时间: {valid_create_dates} ({valid_create_dates/len(results)*100:.1f}%)")
        
        if valid_ages:
            print(f"\n年龄统计:")
            print(f"  平均年龄: {avg_age:.1f}岁")
            print(f"  最小年龄: {min_age}岁")
            print(f"  最大年龄: {max_age}岁")
            print(f"  可计算年龄的用户: {len(valid_ages)} ({len(valid_ages)/len(results)*100:.1f}%)")
        
        # 注册时间分析
        create_dates = [u['create_time'] for u in results if u['create_time'] and 
                       u['create_time'] != '转换失败' and 
                       u['create_time'] != '转换异常' and
                       u['create_time'] != '无效时间戳' and
                       u['create_time'] != '未来日期' and
                       u['create_time'] != '过旧日期' and
                       u['create_time'] != '日期超出范围']
        if create_dates:
            # 转换为datetime对象
            try:
                date_objs = []
                for d in create_dates:
                    try:
                        date_obj = datetime.strptime(d, '%Y-%m-%d')
                        date_objs.append(date_obj)
                    except:
                        pass
                
                if date_objs:
                    min_date = min(date_objs).strftime('%Y-%m-%d')
                    max_date = max(date_objs).strftime('%Y-%m-%d')
                    print(f"\n注册时间范围: {min_date} 到 {max_date}")
            except:
                pass
    
    def read_user_ids(self, file_path):
        """从CSV文件读取用户ID"""
        file_path = get_absolute_path(file_path)
        
        if not os.path.exists(file_path):
            print(f"错误: 找不到文件 {file_path}")
            return []
        
        print(f"读取文件: {file_path}")
        
        try:
            user_ids = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # 查找user_id列
                user_id_index = 0
                for i, header in enumerate(headers):
                    if 'user' in header.lower() and 'id' in header.lower():
                        user_id_index = i
                        break
                
                for row in reader:
                    if len(row) > user_id_index:
                        user_id = str(row[user_id_index]).strip()
                        if user_id and user_id != 'nan':
                            user_ids.append(user_id)
            
            print(f"成功读取 {len(user_ids)} 个用户ID")
            return user_ids
            
        except Exception as e:
            print(f"读取文件失败: {e}")
            return []

def main():
    """主函数"""
    print("=" * 60)
    print("网易云音乐用户信息爬虫 - 日期转换版")
    print("=" * 60)
    
    # 用户ID文件路径
    input_file = input(f"请输入CSV文件路径 (直接回车使用默认路径): ").strip()
    
    if not input_file:
        # 使用默认路径
        default_paths = [
            "collected_user_ids_20260119_173402.csv",
            "./数据集/爬用户数据/collected_user_ids_20260119_173402.csv",
            "../数据集/爬用户数据/collected_user_ids_20260119_173402.csv",
            r"C:\Users\小侯\Desktop\学校作业\毕业设计\数据集\爬用户数据\collected_user_ids_20260119_173402.csv"
        ]
        
        for path in default_paths:
            abs_path = get_absolute_path(path)
            if os.path.exists(abs_path):
                input_file = abs_path
                print(f"使用默认路径: {input_file}")
                break
        
        if not input_file:
            print("未找到默认文件，请手动输入路径")
            return
    
    # 初始化爬虫
    crawler = NetEaseCrawler()
    
    # 读取用户ID
    user_ids = crawler.read_user_ids(input_file)
    
    if not user_ids:
        print("没有读取到用户ID，程序结束")
        return
    
    # 显示前几个用户ID
    print(f"前5个用户ID: {user_ids[:5]}")
    
    # 选择爬取数量
    print(f"\n共 {len(user_ids)} 个用户ID")
    print("请选择:")
    print("1. 测试前10个")
    print("2. 爬取全部")
    print("3. 自定义数量")
    
    choice = input("请输入选择 (1-3): ").strip()
    
    if choice == "1":
        selected_ids = user_ids[:10]
    elif choice == "2":
        selected_ids = user_ids
    elif choice == "3":
        try:
            n = int(input(f"请输入要爬取的数量 (1-{len(user_ids)}): "))
            selected_ids = user_ids[:n]
        except:
            print("输入无效，使用前20个")
            selected_ids = user_ids[:20]
    else:
        print("无效选择，使用前20个")
        selected_ids = user_ids[:20]
    
    print(f"将爬取 {len(selected_ids)} 个用户")
    
    # 设置输出文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"用户数据_{timestamp}.csv"
    
    # 开始爬取
    print("\n开始爬取...")
    start_time = time.time()
    
    results = crawler.crawl_users(selected_ids, output_file)
    
    end_time = time.time()
    
    # 保存结果
    if results:
        crawler.save_results(results, output_file)
        
        # 耗时统计
        print(f"\n爬取完成!")
        print(f"总耗时: {end_time - start_time:.1f} 秒")
        print(f"平均每个用户: {(end_time - start_time) / len(selected_ids):.2f} 秒")
    else:
        print("没有获取到任何数据")

if __name__ == "__main__":
    main()
    input("\n按Enter键退出...")