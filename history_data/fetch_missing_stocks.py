#!/usr/bin/env python3
"""
缺失股票数据爬取脚本 (多线程批处理版本)

功能：
1. 读取missing_stock_data.csv中的缺失股票列表
2. 过滤掉北京交易所(BJ)的股票，只处理上海(SH)和深圳(SZ)交易所的股票
3. 使用多线程(默认5个)并发调用baostock获取历史数据
4. 每个线程独立工作，处理完20只股票后批量入库一次
5. 避免内存占用过大，提高数据入库的及时性
6. 按日期分组插入，避免ClickHouse分区过多的问题

特性：
- 多线程并发爬取，提高效率
- 批处理入库，每20只股票入库一次
- 线程独立工作，避免竞争
- 按日期分组插入，避免分区过多问题
- 实时入库，内存占用可控
- 线程安全的日志输出和统计
- 详细的进度显示和错误统计
"""

import os
import sys
import time
import traceback
import pandas as pd
import baostock as bs
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import argparse
from queue import Queue
from db_engine import create_clickhouse_client

# 线程安全的锁和统计变量
print_lock = threading.Lock()
stats_lock = threading.Lock()
global_stats = {
    'success_count': 0,
    'failed_count': 0,
    'total_records': 0,
    'failed_stocks': []
}

def thread_safe_print(message):
    """线程安全的打印函数"""
    with print_lock:
        print(message)

def update_global_stats(success_count=0, failed_count=0, total_records=0, failed_stocks=None):
    """线程安全的统计更新函数"""
    with stats_lock:
        global_stats['success_count'] += success_count
        global_stats['failed_count'] += failed_count
        global_stats['total_records'] += total_records
        if failed_stocks:
            global_stats['failed_stocks'].extend(failed_stocks)

# 市场代码映射
MARKET_MAPPING = {
    'SH': 'sh',
    'SZ': 'sz',
    'BJ': 'bj'
}

def get_market_code(stock_code, market):
    """根据股票代码和市场信息生成baostock需要的完整代码"""
    market_lower = market.lower()
    if market_lower in ['sh', 'sz']:
        # 确保股票代码是6位数字，左补零
        formatted_code = str(stock_code).zfill(6)
        return f"{market_lower}.{formatted_code}"
    else:
        # 北京交易所暂不支持
        return None

def fetch_single_stock_data(stock_code, market):
    """
    获取单只股票的历史数据并返回处理后的DataFrame
    
    Args:
        stock_code: 股票代码（如：600000）
        market: 市场代码（如：SH, SZ）
    
    Returns:
        tuple: (是否成功, DataFrame或None, 数据行数, 股票代码)
    """
    # 确保股票代码格式正确
    formatted_stock_code = str(stock_code).zfill(6)
    
    # 生成baostock格式的代码
    bao_code = get_market_code(formatted_stock_code, market)
    if not bao_code:
        return False, None, 0, formatted_stock_code
    
    # 每个线程需要独立登录baostock
    lg = bs.login()
    if lg.error_code != '0':
        return False, None, 0, formatted_stock_code
    
    try:
        # 查询历史数据
        rs = bs.query_history_k_data_plus(
            bao_code,
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date='2000-01-01', 
            end_date='2025-08-31',
            frequency="d", 
            adjustflag="3"
        )
        
        if rs.error_code != '0':
            return False, None, 0, formatted_stock_code
        
        # 收集数据
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            return False, None, 0, formatted_stock_code
        
        # 转换为DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 直接处理数据使其符合ClickHouse表结构
        processed_df = process_dataframe_for_clickhouse(df, market)
        
        if processed_df is not None:
            return True, processed_df, len(processed_df), formatted_stock_code
        else:
            return False, None, 0, formatted_stock_code
        
    except Exception as e:
        return False, None, 0, formatted_stock_code
    finally:
        # 登出baostock
        bs.logout()

def process_dataframe_for_clickhouse(df, market):
    """
    处理DataFrame，使其符合ClickHouse表结构要求
    
    Args:
        df: 原始DataFrame
        market: 市场代码
    
    Returns:
        pandas.DataFrame: 处理后的数据
    """
    try:
        if df.empty:
            return None
        
        # 数据清洗和转换
        # 1. 处理日期列
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # 2. 添加市场信息并清理代码
        df['market'] = market.lower()
        df['code'] = df['code'].str.replace(r'^(sh\.|sz\.|bj\.)', '', regex=True)
        
        # 3. 重命名列以保持一致性
        if 'pctChg' in df.columns:
            df.rename(columns={'pctChg': 'p_change'}, inplace=True)
        
        # 4. 处理数值列
        numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 
                          'adjustflag', 'turn', 'tradestatus', 'p_change', 'isST']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 5. 确保列顺序符合ClickHouse表结构
        expected_columns = [
            'date', 'code', 'market', 'open', 'high', 'low', 'close', 'preclose',
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'p_change', 'isST'
        ]
        
        # 添加缺失的列
        for col in expected_columns:
            if col not in df.columns:
                if col == 'market':
                    df[col] = market.lower()
                else:
                    df[col] = 0
        
        # 重新排序列
        df = df[expected_columns]
        
        return df
        
    except Exception as e:
        print(f"处理DataFrame时发生错误: {str(e)}")
        return None

def worker_thread(stock_queue, thread_id, batch_size=20):
    """
    工作线程函数，处理股票队列中的任务
    
    Args:
        stock_queue: 股票任务队列
        thread_id: 线程ID
        batch_size: 批处理大小，默认20个股票
    """
    thread_safe_print(f"线程 {thread_id} 开始工作")
    
    batch_dataframes = []
    batch_count = 0
    thread_success = 0
    thread_failed = 0
    thread_records = 0
    thread_failed_stocks = []
    
    while True:
        try:
            # 从队列获取任务，超时1秒
            try:
                stock_info = stock_queue.get(timeout=1)
            except:
                # 队列为空，退出线程
                break
            
            if stock_info is None:
                # 收到停止信号
                break
            
            stock_code = stock_info['stock_code']
            market = stock_info['market']
            index = stock_info['index']
            total = stock_info['total']
            
            # 确保股票代码格式正确
            formatted_stock_code = str(stock_code).zfill(6)
            
            thread_safe_print(f"线程{thread_id} [{index}/{total}] 处理股票: {formatted_stock_code}({market})")
            
            # 获取股票数据
            success, df, record_count, _ = fetch_single_stock_data(formatted_stock_code, market)
            
            if success and df is not None:
                batch_dataframes.append(df)
                batch_count += 1
                thread_success += 1
                thread_records += record_count
                thread_safe_print(f"线程{thread_id} [{index}/{total}] 成功获取 {formatted_stock_code}: {record_count} 条记录")
            else:
                thread_failed += 1
                thread_failed_stocks.append(f"{formatted_stock_code}({market})")
                thread_safe_print(f"线程{thread_id} [{index}/{total}] 获取 {formatted_stock_code} 失败")
            
            # 如果达到批处理大小，执行入库
            if batch_count >= batch_size:
                if batch_dataframes:
                    thread_safe_print(f"线程{thread_id} 开始批量入库 {batch_count} 只股票的数据...")
                    if insert_batch_to_clickhouse(batch_dataframes, thread_id):
                        thread_safe_print(f"线程{thread_id} 批量入库成功")
                    else:
                        thread_safe_print(f"线程{thread_id} 批量入库失败")
                
                # 重置批次
                batch_dataframes = []
                batch_count = 0
            
            # 标记任务完成
            stock_queue.task_done()
            
            # 稍作延迟，避免请求过快
            time.sleep(0.05)
            
        except Exception as e:
            thread_safe_print(f"线程{thread_id} 处理股票时发生错误: {str(e)}")
            thread_failed += 1
            stock_queue.task_done()
    
    # 处理剩余的数据
    if batch_dataframes:
        thread_safe_print(f"线程{thread_id} 处理剩余的 {len(batch_dataframes)} 只股票数据...")
        if insert_batch_to_clickhouse(batch_dataframes, thread_id):
            thread_safe_print(f"线程{thread_id} 最终批量入库成功")
        else:
            thread_safe_print(f"线程{thread_id} 最终批量入库失败")
    
    # 更新全局统计
    update_global_stats(thread_success, thread_failed, thread_records, thread_failed_stocks)
    
    thread_safe_print(f"线程{thread_id} 完成工作: 成功{thread_success}只, 失败{thread_failed}只, 记录{thread_records}条")

def insert_batch_to_clickhouse(df_list, thread_id):
    """
    批量插入数据到ClickHouse数据库，按日期分组以避免分区过多的问题
    
    Args:
        df_list: DataFrame列表
        thread_id: 线程ID
    
    Returns:
        bool: 是否成功
    """
    if not df_list:
        return True
    
    try:
        # 合并所有DataFrame
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # 连接ClickHouse
        client = create_clickhouse_client()
        if client is None:
            thread_safe_print(f"线程{thread_id} 无法连接到ClickHouse")
            return False
        
        try:
            # 按日期分组，每次插入不超过50个日期的数据
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            date_groups = combined_df.groupby(combined_df['date'].dt.date)
            
            # 将日期分组分批处理，每批最多50个日期
            date_list = list(date_groups.groups.keys())
            batch_size = 50  # 每批最多50个日期
            
            total_inserted = 0
            start_time = datetime.now()
            
            for i in range(0, len(date_list), batch_size):
                batch_dates = date_list[i:i + batch_size]
                
                # 获取这批日期的所有数据
                batch_data_list = []
                for date in batch_dates:
                    batch_data_list.append(date_groups.get_group(date))
                
                if batch_data_list:
                    batch_df = pd.concat(batch_data_list, ignore_index=True)
                    
                    # 确保date列格式正确
                    batch_df['date'] = batch_df['date'].dt.date
                    
                    # 插入这批数据
                    client.insert_df('cn_stock_history', batch_df)
                    total_inserted += len(batch_df)
                    
                    thread_safe_print(f"线程{thread_id} 插入了 {len(batch_dates)} 个日期的 {len(batch_df)} 条记录")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            thread_safe_print(f"线程{thread_id} 总共插入 {total_inserted} 条记录，耗时: {duration:.2f} 秒")
            return True
            
        finally:
            client.close()
            
    except Exception as e:
        thread_safe_print(f"线程{thread_id} 插入数据到ClickHouse时发生错误: {str(e)}")
        return False

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='缺失股票数据爬取工具 (多线程批处理版本)')
    parser.add_argument('--threads', '-t', type=int, default=5, 
                       help='并发线程数 (默认: 5)')
    parser.add_argument('--max-threads', type=int, default=10,
                       help='最大允许线程数 (默认: 10)')
    parser.add_argument('--batch-size', '-b', type=int, default=20,
                       help='每个线程批处理大小 (默认: 20)')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.threads < 1:
        print("错误: 线程数必须大于0")
        return
    if args.threads > args.max_threads:
        print(f"错误: 线程数不能超过 {args.max_threads}")
        return
    if args.batch_size < 1:
        print("错误: 批处理大小必须大于0")
        return
    
    print("=== 缺失股票数据爬取工具 (多线程批处理版本) ===")
    print(f"开始时间: {datetime.now()}")
    print(f"并发线程数: {args.threads}")
    print(f"批处理大小: {args.batch_size} 只股票/批次")
    
    # 1. 检查missing_stock_data.csv文件
    missing_file = os.path.join(os.path.dirname(__file__), 'missing_stock_data.csv')
    if not os.path.exists(missing_file):
        print(f"错误: 缺失股票数据文件不存在: {missing_file}")
        print("请先运行 deal_clickhouse_data.py 生成缺失股票列表")
        return
    
    # 2. 读取缺失股票数据
    print(f"读取缺失股票数据: {missing_file}")
    missing_df = pd.read_csv(missing_file)
    
    if missing_df.empty:
        print("没有缺失的股票数据")
        return
    
    # 3. 过滤掉北京交易所的股票
    non_bj_stocks = missing_df[missing_df['market'] != 'BJ'].copy()
    bj_stocks_count = len(missing_df) - len(non_bj_stocks)
    
    print(f"总缺失股票: {len(missing_df)} 只")
    print(f"北京交易所股票: {bj_stocks_count} 只 (跳过)")
    print(f"待处理股票: {len(non_bj_stocks)} 只 (上海+深圳)")
    
    if non_bj_stocks.empty:
        print("没有需要处理的股票（排除北京交易所后）")
        return
    
    # 4. 创建任务队列
    stock_queue = Queue()
    
    # 将股票信息添加到队列
    for index, row in non_bj_stocks.iterrows():
        stock_info = {
            'stock_code': row['clean_code'],
            'market': row['market'],
            'index': index + 1,
            'total': len(non_bj_stocks)
        }
        stock_queue.put(stock_info)
    
    print(f"已将 {stock_queue.qsize()} 只股票添加到任务队列")
    
    # 重置全局统计
    global global_stats
    global_stats = {
        'success_count': 0,
        'failed_count': 0,
        'total_records': 0,
        'failed_stocks': []
    }
    
    try:
        # 5. 启动线程池
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            # 提交工作线程
            futures = []
            for i in range(args.threads):
                future = executor.submit(worker_thread, stock_queue, i + 1, args.batch_size)
                futures.append(future)
            
            # 等待所有线程完成
            for future in futures:
                future.result()
        
        print(f"\n=== 数据获取完成 ===")
        print(f"成功: {global_stats['success_count']} 只股票")
        print(f"失败: {global_stats['failed_count']} 只股票") 
        print(f"总记录数: {global_stats['total_records']:,}")
        
        if global_stats['failed_stocks']:
            print(f"\n失败的股票列表:")
            for i, stock in enumerate(global_stats['failed_stocks'], 1):
                print(f"  {i}. {stock}")
        
        print("所有数据已在线程中实时入库完成")
        
    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"程序执行过程中发生错误: {str(e)}")
        traceback.print_exc()
    
    print(f"\n程序结束时间: {datetime.now()}")

if __name__ == "__main__":
    main()
