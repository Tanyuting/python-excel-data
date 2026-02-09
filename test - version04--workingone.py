import pandas as pd
import re
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

class EmailAnalyzer:
    def __init__(self, excel_file=None):
        self.excel_file = excel_file
        self.df = None
        self.data_by_search_id = {}
        self.data_by_thread_id = defaultdict(list)
        self.all_emails = []
        self.results = []
        
        if excel_file:
            self.load_data(excel_file)
    
    def load_data(self, excel_file):
        """加载Excel数据"""
        print(f"读取文件: {excel_file}")
        
        if not os.path.exists(excel_file):
            print(f"文件不存在: {excel_file}")
            return False
        
        try:
            self.df = pd.read_excel(excel_file)
            print(f"数据形状: {self.df.shape}")
            print(f"列名: {list(self.df.columns)}")
            
            # 处理数据
            self.process_data()
            return True
            
        except Exception as e:
            print(f"读取文件失败: {e}")
            return False
    
    def process_data(self):
        """处理数据，建立索引"""
        print("\n开始处理数据...")
        
        # 确定列名
        filename_col = None
        time_col = None
        
        # 尝试自动识别列名
        possible_filename_cols = ['文件名', 'File', 'file', 'filename', '邮件名', '标题', 'Subject', 'Name']
        possible_time_cols = ['日本时间', '时间', 'Time', 'time', 'JST', '日期', 'Date', '发送时间', 'Timestamp']
        
        for col in self.df.columns:
            col_str = str(col).lower()
            if not filename_col:
                for keyword in possible_filename_cols:
                    if keyword.lower() in col_str:
                        filename_col = col
                        break
            
            if not time_col:
                for keyword in possible_time_cols:
                    if keyword.lower() in col_str:
                        time_col = col
                        break
        
        # 如果没有自动识别到，使用前两列
        if not filename_col and len(self.df.columns) > 0:
            filename_col = self.df.columns[0]
            print(f"使用第一列作为文件名列: {filename_col}")
        
        if not time_col and len(self.df.columns) > 1:
            time_col = self.df.columns[1]
            print(f"使用第二列作为时间列: {time_col}")
        elif not time_col:
            time_col = self.df.columns[0]
            print(f"使用第一列作为时间列: {time_col}")
        
        print(f"使用列名: 文件名列='{filename_col}', 时间列='{time_col}'")
        
        # 重置数据结构
        self.data_by_search_id = {}
        self.data_by_thread_id = defaultdict(list)
        self.all_emails = []
        
        # 处理每一行数据
        for idx in range(len(self.df)):
            try:
                row = self.df.iloc[idx]
                
                # 获取文件名
                if filename_col not in row.index:
                    continue
                    
                filename = row[filename_col]
                if pd.isna(filename) or str(filename).strip() == '':
                    continue
                
                filename_str = str(filename).strip()
                
                # 获取时间
                time_val = None
                if time_col in row.index:
                    time_str = row[time_col]
                    if pd.notna(time_str):
                        try:
                            time_val = pd.to_datetime(time_str)
                        except:
                            # 尝试其他格式
                            try:
                                time_val = pd.to_datetime(time_str, format='%Y-%m-%d %H:%M:%S')
                            except:
                                try:
                                    time_val = pd.to_datetime(time_str, format='%Y/%m/%d %H:%M:%S')
                                except:
                                    try:
                                        time_val = pd.to_datetime(time_str, format='%Y-%m-%d %H:%M')
                                    except:
                                        try:
                                            time_val = pd.to_datetime(time_str, format='%Y/%m/%d %H:%M')
                                        except:
                                            continue
                
                if time_val is None:
                    continue
                
                # 提取各种ID
                email_id = self.extract_email_id(filename_str)
                thread_id = self.extract_thread_id(filename_str)
                search_id = self.extract_search_id(filename_str)
                reply_flag = self.is_reply(filename_str)
                
                # 创建邮件信息对象
                email_info = {
                    '原始行号': idx + 2,
                    '文件名': filename_str,
                    '时间': time_val,
                    '邮件ID': email_id if email_id else f"ID_{idx}",
                    '线程ID': thread_id,
                    '搜索ID': search_id,
                    '是回复': reply_flag,
                    '原始数据': row.to_dict()
                }
                
                self.all_emails.append(email_info)
                
                # 按搜索ID索引（如果有）
                if search_id:
                    self.data_by_search_id[search_id] = email_info
                
                # 按线程ID分组
                if thread_id and thread_id != "未知":
                    self.data_by_thread_id[thread_id].append(email_info)
                
            except Exception as e:
                continue
        
        # 按时间排序所有邮件
        self.all_emails.sort(key=lambda x: x['时间'])
        
        print(f"\n数据处理完成:")
        print(f"  有效邮件记录: {len(self.all_emails)}")
        print(f"  唯一线程ID数量: {len(self.data_by_thread_id)}")
        print(f"  包含搜索ID的记录: {len(self.data_by_search_id)}")
        
        # 显示线程ID统计（按类型）
        thread_stats = defaultdict(int)
        for thread_id in self.data_by_thread_id.keys():
            if thread_id and thread_id != "未知":
                if thread_id.startswith('A') and len(thread_id) == 4:  # Axxx
                    thread_stats['A格式'] += 1
                elif thread_id.startswith('B') and len(thread_id) == 4:  # Bxxx
                    thread_stats['B格式'] += 1
                elif thread_id.startswith('C') and len(thread_id) == 4:  # Cxxx
                    thread_stats['C格式'] += 1
                elif thread_id.startswith('INC'):
                    thread_stats['INC编号'] += 1
                elif 'C' in thread_id and len(thread_id) > 4:  # 类似C29497931的长格式
                    thread_stats['长C格式'] += 1
                else:
                    thread_stats['其他'] += 1
        
        if thread_stats:
            print(f"\n线程ID类型统计:")
            for type_name, count in thread_stats.items():
                print(f"  {type_name}: {count}个")
        
        if self.data_by_search_id:
            print(f"\n搜索ID示例:")
            for i, (sid, _) in enumerate(list(self.data_by_search_id.items())[:10]):
                print(f"  {i+1}. {sid}")
    
    def extract_email_id(self, filename):
        """从文件名中提取邮件ID"""
        if pd.isna(filename) or filename == 'nan':
            return None
        
        filename_str = str(filename)
        
        patterns = [
            r'\[.*?:(\d{5})\]',
            r'_(\d{5})\.eml',
            r'(\d{5})\.eml',
            r'\[INC(\d{8})\]',  # 8位INC编号
            r'INC(\d{8})',
            r'(\d{5})_',
            r'_(\d{5})_',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename_str)
            if match:
                return match.group(1)
        
        return None
    
    def extract_thread_id(self, filename):
        """从文件名中提取线程ID（修正版）"""
        if pd.isna(filename) or filename == 'nan':
            return "未知"
        
        filename_str = str(filename)
        
        # 首先检查是否是长C编号（如C29497931） - 这些不是真正的C格式线程ID
        # 长C编号通常是8位或更多数字
        long_c_pattern = r'[^a-zA-Z](C\d{8,})[^a-zA-Z]'
        long_c_match = re.search(long_c_pattern, filename_str)
        if long_c_match:
            # 这是长C编号，不是真正的C格式线程ID
            return "未知"
        
        # 然后检查真正的短格式线程ID
        patterns = [
            # 真正的Cxxx格式 - 需要确保是短格式且后面有合适的上下文
            r'[^a-zA-Z](C\d{3})[^\.a-zA-Z]',  # C088后面不是点或字母
            r'_C(\d{3})\.eml',  # _C088.eml
            r'問い合わせが入りました_C(\d{3})\.eml',  # 問い合わせが入りました_C088.eml
            r'【Intune切り替え】問い合わせが入りました_C(\d{3})\.eml',
            
            # Axxx格式
            r'[^a-zA-Z](A\d{3})[^\.a-zA-Z]',
            r'_A(\d{3})\.eml',
            
            # Bxxx格式
            r'[^a-zA-Z](B\d{3})[^\.a-zA-Z]',
            r'_B(\d{3})\.eml',
            
            # 通用短格式（3-4位数字）
            r'[^a-zA-Z]([A-Z]\d{3})[^\.a-zA-Z\d]',  # 字母+3位数字，后面不是点、字母或数字
            r'_([A-Z]\d{3})\.eml',  # _A553.eml格式
            
            # 最后尝试宽松匹配
            r'([A-Z]\d{3})(?![a-zA-Z\d])',  # 字母+3位数字，后面不是字母或数字
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename_str, re.IGNORECASE)
            if match:
                thread_id = match.group(1).upper()
                # 验证是真正的短格式线程ID（不是长C编号）
                if (thread_id.startswith(('A', 'B', 'C')) and 
                    len(thread_id) == 4 and  # A/B/C + 3位数字
                    thread_id[1:].isdigit()):
                    return thread_id
        
        # 检查INC编号
        inc_patterns = [
            r'\[INC(\d+)\]',
            r'INC(\d+)',
            r'【INC(\d+)】',
        ]
        
        for pattern in inc_patterns:
            match = re.search(pattern, filename_str)
            if match:
                inc_num = match.group(1)
                # 如果是8位数字，很可能是INC编号
                if len(inc_num) >= 5:  # INC编号通常较长
                    return f"INC{inc_num}"
        
        return "未知"
    
    def extract_search_id(self, filename):
        """从文件名中提取搜索ID（如mdmswitch_help:01218）"""
        if pd.isna(filename) or filename == 'nan':
            return None
        
        filename_str = str(filename)
        
        patterns = [
            r'\[(mdmswitch_help:\d+)\]',
            r'\[(\w+:\d+)\]',
            r'(\w+:\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename_str)
            if match:
                return match.group(1)
        
        return None
    
    def is_reply(self, filename):
        """判断是否是回复邮件"""
        if pd.isna(filename) or filename == 'nan':
            return False
        
        filename_str = str(filename)
        reply_indicators = ['Re:', '返信', 'RE:', 're:', '回复', '答复']
        return any(indicator in filename_str for indicator in reply_indicators)
    
    def find_closest_response(self, search_id):
        """查找指定搜索ID的最接近回复"""
        print(f"\n查找搜索ID: {search_id}")
        
        if search_id not in self.data_by_search_id:
            # 尝试模糊匹配
            matching_ids = [sid for sid in self.data_by_search_id.keys() 
                          if search_id.lower() in sid.lower()]
            if not matching_ids:
                print(f"  未找到搜索ID: {search_id}")
                return {
                    '搜索ID': search_id,
                    '目标邮件名包含': '未找到',
                    '目标邮件时间': 'N/A',
                    '最近的返信时间': '未找到邮件',
                    '回复邮件ID': 'N/A',
                    '回复间隔': 'N/A',
                    '回复间隔(小时)': 'N/A',
                    '回复类型': 'N/A',
                    '线程邮件数': 0,
                    '回复邮件数': 0,
                    '状态': '未找到搜索ID'
                }
            print(f"  找到匹配的搜索ID: {matching_ids[0]}")
            search_id = matching_ids[0]
        
        target_email = self.data_by_search_id[search_id]
        target_time = target_email['时间']
        target_thread_id = target_email['线程ID']
        target_filename = target_email['文件名']
        
        print(f"  目标邮件: {target_filename[:80]}...")
        print(f"  目标时间: {target_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  线程ID: {target_thread_id}")
        
        if target_thread_id == "未知" or target_thread_id not in self.data_by_thread_id:
            print(f"  ⚠ 未找到线程中的其他邮件 (线程ID: {target_thread_id})")
            
            # 尝试从文件名中直接提取可能的关联
            possible_thread_ids = []
            filename = target_filename
            
            # 查找可能的关联邮件编号
            thread_patterns = [
                r'[^a-zA-Z]([A-Z]\d{3})[^\.a-zA-Z]',
                r'_([A-Z]\d{3})\.eml',
            ]
            
            for pattern in thread_patterns:
                match = re.search(pattern, filename, re.IGNORECASE)
                if match:
                    tid = match.group(1).upper()
                    if tid != target_thread_id and tid in self.data_by_thread_id:
                        possible_thread_ids.append(tid)
            
            if possible_thread_ids:
                print(f"  找到可能的关联线程ID: {possible_thread_ids}")
                # 使用第一个可能的线程ID
                target_thread_id = possible_thread_ids[0]
                print(f"  使用线程ID: {target_thread_id}")
            else:
                return {
                    '搜索ID': search_id,
                    '目标邮件名包含': target_thread_id,
                    '目标邮件时间': target_time.strftime('%Y-%m-%d %H:%M:%S'),
                    '最近的返信时间': '无回复',
                    '回复邮件ID': 'N/A',
                    '回复间隔': 'N/A',
                    '回复间隔(小时)': 'N/A',
                    '回复类型': 'N/A',
                    '线程邮件数': 1,
                    '回复邮件数': 0,
                    '状态': '线程中无其他邮件'
                }
        
        thread_emails = self.data_by_thread_id[target_thread_id].copy()
        thread_emails.sort(key=lambda x: x['时间'])
        
        print(f"  找到 {len(thread_emails)} 封同一线程的邮件")
        
        # 显示线程中的邮件时间线
        if len(thread_emails) <= 10:  # 只显示少量邮件时显示时间线
            print(f"  线程 {target_thread_id} 邮件时间线:")
            for i, email in enumerate(thread_emails):
                time_str = email['时间'].strftime('%m-%d %H:%M:%S')
                is_target = " ←目标" if email.get('搜索ID') == search_id else ""
                reply_mark = " [回复]" if email['是回复'] else ""
                print(f"    {i+1:3d}. {time_str}{reply_mark}{is_target}")
        
        # 查找目标邮件之后的所有邮件
        responses_after = []
        for email in thread_emails:
            if email['时间'] > target_time:
                responses_after.append(email)
        
        print(f"  目标邮件之后的邮件: {len(responses_after)} 封")
        
        if not responses_after:
            print("  ⚠ 目标邮件之后没有其他邮件")
            return {
                '搜索ID': search_id,
                '目标邮件名包含': target_thread_id,
                '目标邮件时间': target_time.strftime('%Y-%m-%d %H:%M:%S'),
                '最近的返信时间': '无回复',
                '回复邮件ID': 'N/A',
                '回复间隔': 'N/A',
                '回复间隔(小时)': 'N/A',
                '回复类型': 'N/A',
                '线程邮件数': len(thread_emails),
                '回复邮件数': 0,
                '状态': '无回复'
            }
        
        # 优先查找回复邮件
        reply_responses = [r for r in responses_after if r['是回复']]
        
        if reply_responses:
            # 从回复邮件中找时间最近的
            nearest_response = None
            min_time_diff = None
            
            for response in reply_responses:
                time_diff = response['时间'] - target_time
                if min_time_diff is None or time_diff < min_time_diff:
                    min_time_diff = time_diff
                    nearest_response = response
            
            response_type = "回复邮件"
            print(f"  找到 {len(reply_responses)} 封回复邮件")
        else:
            # 如果没有回复邮件，找时间最近的任何邮件
            nearest_response = None
            min_time_diff = None
            
            for response in responses_after:
                time_diff = response['时间'] - target_time
                if min_time_diff is None or time_diff < min_time_diff:
                    min_time_diff = time_diff
                    nearest_response = response
            
            response_type = "非回复邮件"
            print(f"  无回复邮件，使用最近的非回复邮件")
        
        print(f"  最近回复时间: {nearest_response['时间'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  回复邮件ID: {nearest_response['邮件ID']}")
        
        # 计算时间差
        time_diff = nearest_response['时间'] - target_time
        total_hours = time_diff.total_seconds() / 3600
        
        days = time_diff.days
        hours = time_diff.seconds // 3600
        minutes = (time_diff.seconds % 3600) // 60
        
        if days > 0:
            interval_str = f"{days}天{hours}小时{minutes}分钟"
        elif hours > 0:
            interval_str = f"{hours}小时{minutes}分钟"
        else:
            interval_str = f"{minutes}分钟"
        
        print(f"  回复间隔: {interval_str} ({total_hours:.2f}小时)")
        
        return {
            '搜索ID': search_id,
            '目标邮件名包含': target_thread_id,
            '目标邮件时间': target_time.strftime('%Y-%m-%d %H:%M:%S'),
            '最近的返信时间': nearest_response['时间'].strftime('%Y-%m-%d %H:%M:%S'),
            '回复邮件ID': nearest_response['邮件ID'],
            '回复间隔': interval_str,
            '回复间隔(小时)': round(total_hours, 2),
            '回复类型': response_type,
            '线程邮件数': len(thread_emails),
            '回复邮件数': len(responses_after),
            '状态': '成功'
        }
    
    def batch_query(self, search_ids):
        """批量查询多个搜索ID"""
        print(f"\n开始批量处理 {len(search_ids)} 个搜索ID...")
        
        batch_results = []
        
        for i, search_id in enumerate(search_ids):
            print(f"[{i+1}/{len(search_ids)}] ", end="")
            
            result = self.find_closest_response(search_id)
            batch_results.append(result)
            
            if result['状态'] == '成功':
                print(f"  ✓ 找到回复: {result['最近的返信时间']} (间隔:{result['回复间隔']})")
            elif result['状态'] == '未找到搜索ID':
                print(f"  ✗ 未找到")
            else:
                print(f"  ⚠ {result['状态']}")
        
        return batch_results

# 修改文件保存函数，解决权限问题
def safe_save_excel_with_auto_rename(df, base_filename=None):
    """安全保存Excel文件，自动处理文件占用和权限问题"""
    if not base_filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"邮件查询结果_{timestamp}"
    
    # 确保是.xlsx文件
    if not base_filename.endswith('.xlsx'):
        base_filename += '.xlsx'
    
    # 尝试多个可能的保存位置
    save_locations = [
        os.path.join(os.path.expanduser("~"), "Desktop"),  # 桌面
        os.path.join(os.path.expanduser("~"), "Downloads"),  # 下载文件夹
        os.path.dirname(os.path.abspath(__file__)),  # 脚本所在目录
        "C:\\Temp",  # 临时文件夹
        ".",  # 当前目录
    ]
    
    # 尝试保存
    for location in save_locations:
        try:
            # 确保目录存在
            if not os.path.exists(location):
                continue
            
            file_path = os.path.join(location, os.path.basename(base_filename))
            
            # 如果文件已存在，添加时间戳
            if os.path.exists(file_path):
                name_part, ext = os.path.splitext(file_path)
                micro_timestamp = datetime.now().strftime("%H%M%S_%f")[:-3]
                file_path = f"{name_part}_{micro_timestamp}{ext}"
            
            # 尝试保存
            df.to_excel(file_path, index=False)
            print(f"✓ 文件保存成功: {file_path}")
            return file_path
            
        except PermissionError:
            # 当前位置权限被拒绝，尝试下一个位置
            continue
        except Exception as e:
            # 其他错误，记录但继续尝试
            print(f"  在 {location} 保存失败: {e}")
            continue
    
    # 如果所有位置都失败，尝试使用绝对唯一的文件名
    try:
        # 在当前目录创建绝对唯一的文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        unique_filename = f"邮件结果_{timestamp}.xlsx"
        
        df.to_excel(unique_filename, index=False)
        print(f"✓ 文件保存成功（使用唯一文件名）: {unique_filename}")
        return unique_filename
    except Exception as e:
        print(f"✗ 所有保存尝试均失败: {e}")
        return None

def main():
    """主程序"""
    print("邮件回复时间分析工具 - 修正版")
    print("=" * 80)
    print("修正线程ID提取逻辑，区分Cxxx格式和长C编号")
    print("=" * 80)
    
    # 固定文件路径
    excel_file = "C:\\Users\\out-tanyuting\\Desktop\\new\\邮件日本时间summary.xlsx"
    
    if not os.path.exists(excel_file):
        print(f"文件不存在: {excel_file}")
        excel_file = input("请输入Excel文件路径: ").strip()
        if not os.path.exists(excel_file):
            print("文件不存在，程序退出")
            return
    
    print(f"使用文件: {excel_file}")
    
    # 创建分析器对象
    analyzer = EmailAnalyzer(excel_file)
    
    if not analyzer.all_emails:
        print("数据加载失败，请检查文件格式")
        return
    
    # 测试一些示例
    print(f"\n测试示例:")
    test_ids = [
        'mdmswitch_help:02944',  # 之前错误识别为C29497931
        'mdmswitch_help:03533',  # B394
        'mdmswitch_help:06061',  # C088示例
    ]
    
    for test_id in test_ids:
        if test_id in analyzer.data_by_search_id:
            email_info = analyzer.data_by_search_id[test_id]
            print(f"  {test_id}: 线程ID={email_info['线程ID']}, 文件名={email_info['文件名'][:60]}...")
    
    # 主查询循环
    all_results = []
    
    while True:
        print(f"\n{'='*80}")
        print("查询选项:")
        print("1. 按搜索ID查询")
        print("2. 批量查询")
        print("3. 测试C088示例")
        print("4. 保存并退出")
        print("输入 'quit' 或 'q' 退出")
        
        choice = input("\n请选择 (1-4): ").strip().lower()
        
        if choice in ['quit', 'exit', 'q']:
            break
        
        elif choice == '1':
            user_input = input("\n请输入搜索ID（如mdmswitch_help:01218）: ").strip()
            if not user_input:
                continue
            
            result = analyzer.find_closest_response(user_input)
            if result:
                all_results.append(result)
                
                print(f"\n查询结果:")
                for key, value in result.items():
                    if value not in ['N/A', '']:
                        print(f"  {key}: {value}")
        
        elif choice == '2':
            print("\n批量查询模式")
            print("输入搜索ID，每行一个，输入空行结束:")
            
            search_ids = []
            while True:
                try:
                    line = input().strip()
                    if line == '':
                        break
                    search_ids.append(line)
                except:
                    break
            
            if search_ids:
                results = analyzer.batch_query(search_ids)
                all_results.extend(results)
                
                # 使用新的保存函数
                if results:
                    df = pd.DataFrame(results)
                    saved_file = safe_save_excel_with_auto_rename(df, "批量查询结果")
                    
                    if saved_file:
                        # 统计
                        success = len([r for r in results if r.get('状态') == '成功'])
                        print(f"\n成功: {success}/{len(results)}")
                    else:
                        print("保存失败，但结果已记录")
        
        elif choice == '3':
            # 测试C088示例
            print("\n测试C088邮件链...")
            
            # 查找所有包含C088的邮件
            c088_emails = []
            for email in analyzer.all_emails:
                if 'C088' in email['文件名'].upper():
                    c088_emails.append(email)
            
            if c088_emails:
                print(f"找到 {len(c088_emails)} 封包含C088的邮件:")
                for email in c088_emails[:5]:  # 显示前5个
                    time_str = email['时间'].strftime('%Y-%m-%d %H:%M:%S')
                    search_id = email.get('搜索ID', '无')
                    print(f"  {time_str} - {search_id} - {email['文件名'][:60]}...")
                
                # 查找相关的搜索ID
                c088_search_ids = [e.get('搜索ID') for e in c088_emails if e.get('搜索ID')]
                if c088_search_ids:
                    print(f"\n相关的搜索ID: {c088_search_ids[:10]}")
                    
                    # 测试查询
                    test_query = input("\n输入搜索ID进行测试（按回车使用第一个）: ").strip()
                    if not test_query and c088_search_ids:
                        test_query = c088_search_ids[0]
                    
                    if test_query:
                        result = analyzer.find_closest_response(test_query)
                        if result:
                            all_results.append(result)
            else:
                print("未找到C088相关邮件")
        
        elif choice == '4':
            if all_results:
                # 使用新的保存函数
                df = pd.DataFrame(all_results)
                saved_file = safe_save_excel_with_auto_rename(df, "最终查询结果")
                
                if saved_file:
                    # 统计
                    success = len([r for r in all_results if r['状态'] == '成功'])
                    print(f"\n总查询数: {len(all_results)}, 成功: {success}")
                else:
                    print("保存失败，但结果已记录")
            else:
                print("没有查询结果需要保存")
            
            print("程序退出")
            break

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"程序错误: {e}")
        import traceback
        traceback.print_exc()
    
    input("\n按回车键退出...")