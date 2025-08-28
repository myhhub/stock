import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from db_engine import CODE_MAP_CSV, AGG_DATA_DIR, HISTORY_DIR


def update_flag_status(all_files, failed_files, empty_files):
    """
    根据文件读取结果更新CODE_MAP_CSV中的flag列
    参数:
        all_files: 所有CSV文件列表
        failed_files: 读取失败的文件列表
        empty_files: 空文件列表
    """
    try:
        # 读取code_map.csv文件
        if not os.path.exists(CODE_MAP_CSV):
            print(f"警告: {CODE_MAP_CSV} 文件不存在，跳过flag更新")
            return
        
        df = pd.read_csv(CODE_MAP_CSV)
        
        # 如果没有flag列，则创建（布尔类型）
        if 'flag' not in df.columns:
            df['flag'] = False
            print("创建了新的flag列（布尔类型）")
        
        # 从文件名中提取股票代码（假设文件名格式为 "code.csv" 或 "market_code.csv"）
        success_files = set(all_files) - set(failed_files) - set(empty_files)
        
        def extract_code_from_filename(filename):
            """从文件名中提取股票代码，支持格式: sh-605090.csv"""
            # 去除.csv后缀
            name_without_ext = filename.replace('.csv', '')
            # 如果包含连字符，取最后一部分（格式为 market-code）
            if '-' in name_without_ext:
                return name_without_ext.split('-')[-1]
            # 兼容下划线格式
            elif '_' in name_without_ext:
                return name_without_ext.split('_')[-1]
            else:
                return name_without_ext
        
        # 提取成功文件的代码（统一转为字符串）
        success_codes = {str(extract_code_from_filename(f)) for f in success_files}
        failed_codes = {str(extract_code_from_filename(f)) for f in failed_files}
        empty_codes = {str(extract_code_from_filename(f)) for f in empty_files}
        
        print(f"成功文件代码数: {len(success_codes)}")
        print(f"失败文件代码数: {len(failed_codes)}")
        print(f"空文件代码数: {len(empty_codes)}")
        
        # 更新flag列（布尔类型）
        # 将code列转换为字符串以便比较
        df['code'] = df['code'].astype(str).str.zfill(6)  # 补零至6位确保格式统一
        
        # 成功的文件标记为True
        df.loc[df['code'].isin(success_codes), 'flag'] = True
        # 失败的文件标记为False
        df.loc[df['code'].isin(failed_codes), 'flag'] = False
        df.loc[df['code'].isin(empty_codes), 'flag'] = False
        
        # 保存更新后的文件
        df.to_csv(CODE_MAP_CSV, index=False)
        
        # 打印统计信息
        flag_counts = df['flag'].value_counts()
        print(f"\n=== Flag状态统计 ===")
        print(f"成功 (True): {flag_counts.get(True, 0)} 个股票")
        print(f"失败/空文件 (False): {flag_counts.get(False, 0)} 个股票")
        
        print(f"已更新 {CODE_MAP_CSV} 的flag列")
    except Exception as e:
        print(f"更新flag状态时发生错误: {str(e)}")

def read_file_in_directory(directory, max_workers=8):
    all_data = []
    failed_files = []
    empty_files = []
    
    # 获取所有CSV文件
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    print(f"找到 {len(csv_files)} 个CSV文件，开始并发读取...")
    
    # 线程锁，用于安全地更新共享列表
    lock = threading.Lock()
    
    def read_single_file(filename):
        filepath = os.path.join(directory, filename)
        try:
            df = pd.read_csv(filepath)
            if df.empty or len(df) == 0:
                with lock:
                    empty_files.append(filename)
                print(f"警告: 文件 {filename} 为空或无数据")
                return None
            else:
                print(f"成功读取文件: {filename}, 数据行数: {len(df)}")
                return df
        except Exception as e:
            with lock:
                failed_files.append(filename)
            print(f"错误: 无法读取文件 {filename}, 错误信息: {str(e)}")
            return None
    
    # 使用线程池并发读取文件
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_filename = {executor.submit(read_single_file, filename): filename 
                             for filename in csv_files}
        
        # 收集结果
        for future in as_completed(future_to_filename):
            filename = future_to_filename[future]
            try:
                result = future.result()
                if result is not None:
                    with lock:
                        all_data.append(result)
            except Exception as exc:
                with lock:
                    failed_files.append(filename)
                print(f"线程执行异常: 文件 {filename} 产生异常: {exc}")
    
    # 打印汇总信息
    print(f"\n=== 读取汇总 ===")
    print(f"成功读取的文件数: {len(all_data)}")
    print(f"空文件数: {len(empty_files)}")
    print(f"读取失败的文件数: {len(failed_files)}")
    
    if empty_files:
        print(f"空文件列表: {empty_files}")
    if failed_files:
        print(f"失败文件列表: {failed_files}")
    
    # 将成功文件的列表更新CODE_MAP_CSV的flag列
    update_flag_status(csv_files, failed_files, empty_files)

    if all_data:
        print("正在合并数据...")
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"合并后总数据行数: {len(combined_df)}")
        return combined_df, failed_files, empty_files
    else:
        return pd.DataFrame(), failed_files, empty_files

def read_history_data(max_workers=4):
    """
    并发读取历史数据目录下的所有CSV文件
    参数:
        max_workers: 最大并发线程数，默认为4
    返回: (合并的数据框, 失败文件列表, 空文件列表)
    """
    return read_file_in_directory(HISTORY_DIR, max_workers)

def save_data_by_five_year_periods(combined_df, output_dir=AGG_DATA_DIR):
    """
    将合并的数据按5年周期分组保存，从2000年开始
    参数:
        combined_df: 合并的数据框
        output_dir: 输出目录
    """
    if combined_df.empty:
        print("没有数据可以分组保存")
        return
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 假设数据中有日期列，尝试常见的日期列名
    date_columns = ['date', 'Date', 'DATE', 'trade_date', 'trading_date', 'time', 'timestamp']
    date_col = None
    
    for col in date_columns:
        if col in combined_df.columns:
            date_col = col
            break
    
    if date_col is None:
        print("警告: 未找到日期列，请检查数据格式")
        print(f"可用列名: {list(combined_df.columns)}")
        return
    
    try:
        # 转换日期列为datetime类型
        combined_df[date_col] = pd.to_datetime(combined_df[date_col])
        
        # 提取年份
        combined_df['year'] = combined_df[date_col].dt.year
        
        # 定义5年周期范围（从2000年开始）
        start_year = 2000
        current_year = pd.Timestamp.now().year
        
        periods = []
        year = start_year
        while year <= current_year:
            end_year = min(year + 4, current_year)  # 5年周期：2000-2004, 2005-2009, etc.
            periods.append((year, end_year))
            year += 5
        
        print(f"开始按5年周期分组保存数据，共 {len(periods)} 个周期")
        
        for start, end in periods:
            # 筛选该周期的数据
            period_data = combined_df[
                (combined_df['year'] >= start) & (combined_df['year'] <= end)
            ].copy()
            
            if not period_data.empty:
                # 删除临时的year列
                period_data = period_data.drop('year', axis=1)
                
                # 生成文件名
                filename = f"data_{start}_{end}.csv"
                filepath = os.path.join(output_dir, filename)
                
                # 保存数据
                period_data.to_csv(filepath, index=False)
                
                print(f"已保存 {start}-{end} 年数据: {len(period_data)} 行 → {filepath}")
            else:
                print(f"警告: {start}-{end} 年期间无数据")
        
        print(f"\n所有周期数据已保存到: {output_dir}")
        
    except Exception as e:
        print(f"分组保存数据时发生错误: {str(e)}")

if __name__ == "__main__":
    # 示例用法
    import time
    
    print(f"开始并发读取目录: {HISTORY_DIR}")
    start_time = time.time()
    
    # 可以调整并发线程数，根据你的系统性能
    combined_data, failed_files, empty_files = read_history_data(max_workers=8)
    
    end_time = time.time()
    print(f"\n总耗时: {end_time - start_time:.2f} 秒")
    
    if not combined_data.empty:
        print(f"\n最终合并数据信息:")
        print(f"数据形状: {combined_data.shape}")
        print(f"列名: {list(combined_data.columns)}")
        
        # 按5年周期保存数据
        print(f"\n开始按5年周期分组保存数据...")
        save_data_by_five_year_periods(combined_data)
    else:
        print("\n没有成功读取到任何数据")