import pandas as pd
import os
import re
from datetime import datetime, timedelta

folder = r"C:\Users\out-tanyuting\Downloads\test-0206\add"
output = r"C:\Users\out-tanyuting\Desktop\邮件日本时间0206-new-01.xlsx"

def extract_jst_time(content):
    """从邮件内容中提取日本时间"""
    # 方法1: 尝试查找类似 "2026-01-26 09:44:39" 的格式
    pattern1 = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})'
    match = re.search(pattern1, content)
    
    if match:
        return match.group(1)
    
    # 方法2: 尝试查找Date头（原始脚本的方法，但需要修正）
    date_pattern = r'Date:\s*(.+?)(?:\n|$)'
    date_match = re.search(date_pattern, content, re.IGNORECASE)
    
    if date_match:
        date_str = date_match.group(1).strip()
        
        # 尝试解析常见的邮件时间格式
        # 格式1: Tue, 20 Jan 2026 06:13:09 +0000
        # 格式2: 20 Jan 2026 06:13:09 +0900
        patterns = [
            r'(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+(\d{2}):(\d{2}):(\d{2})\s+([+-]\d{4})',
            r'([A-Za-z]{3}),\s+(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+(\d{2}):(\d{2}):(\d{2})\s+([+-]\d{4})',
            r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    # 月份映射
                    months = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,
                             'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
                    
                    if len(match.groups()) >= 6:
                        if pattern == patterns[0]:  # 格式1
                            day = int(match.group(1))
                            month = months.get(match.group(2), 1)
                            year = int(match.group(3))
                            hour = int(match.group(4))
                            minute = int(match.group(5))
                            second = int(match.group(6))
                            offset = match.group(7)
                        elif pattern == patterns[1]:  # 格式2
                            day = int(match.group(2))
                            month = months.get(match.group(3), 1)
                            year = int(match.group(4))
                            hour = int(match.group(5))
                            minute = int(match.group(6))
                            second = int(match.group(7))
                            offset = match.group(8)
                        
                        # 计算UTC偏移
                        offset_hours = int(offset[:3])
                        
                        # 创建时间对象
                        local_time = datetime(year, month, day, hour, minute, second)
                        
                        # 转换为UTC
                        utc_time = local_time - timedelta(hours=offset_hours)
                        
                        # 转换为日本时间 (UTC+9)
                        jst_time = utc_time + timedelta(hours=9)
                        
                        return jst_time.strftime("%Y-%m-%d %H:%M:%S")
                    
                except Exception as e:
                    print(f"解析时间时出错: {date_str}, 错误: {e}")
                    continue
    
    return "未找到时间信息"

# 处理所有文件
results = []
file_count = 0
error_files = []

for file in os.listdir(folder):
    if file.endswith('.eml'):
        file_count += 1
        path = os.path.join(folder, file)
        
        try:
            # 尝试不同的编码
            for encoding in ['utf-8', 'shift_jis', 'euc-jp', 'cp932', 'latin-1']:
                try:
                    with open(path, 'r', encoding=encoding, errors='ignore') as f:
                        content = f.read(5000)  # 读取更多内容以确保包含时间信息
                    
                    jst_time = extract_jst_time(content)
                    
                    # 如果找到时间就停止尝试其他编码
                    if jst_time != "未找到时间信息":
                        break
                except:
                    continue
            
            results.append([file, jst_time])
            
            if jst_time == "未找到时间信息":
                error_files.append(file)
                
            # 显示进度
            if file_count % 100 == 0:
                print(f"已处理 {file_count} 个文件...")
                
        except Exception as e:
            print(f"处理文件 {file} 时出错: {e}")
            results.append([file, f"错误: {str(e)}"])

# 保存到Excel
df = pd.DataFrame(results, columns=['文件名', '日本时间(JST)'])
df.to_excel(output, index=False)

print(f"\n完成！已处理 {len(results)} 个文件")
print(f"保存到: {output}")

# 显示统计信息
print(f"\n统计信息:")
print(f"- 成功处理: {len(results) - len(error_files)}")
print(f"- 未找到时间: {len(error_files)}")

if error_files:
    print("\n以下文件未找到时间信息:")
    for i, filename in enumerate(error_files[:10]):  # 只显示前10个
        print(f"  {i+1}. {filename}")
    if len(error_files) > 10:
        print(f"  ... 还有 {len(error_files) - 10} 个文件")

# 显示前几个结果
print("\n前10个结果:")
for i, (filename, time_str) in enumerate(results[:10]):
    print(f"{i+1:3}. {filename[:50]:50} → {time_str}")