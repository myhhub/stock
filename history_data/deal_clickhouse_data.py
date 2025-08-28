import pandas as pd
import os
import traceback
import glob
import re
from datetime import datetime
from db_engine import create_clickhouse_client, create_stock_history_table_clickhouse, AGG_DATA_DIR

MARKET_PREFIXES = {"sh": "上交所", "sz": "深交所", "bj": "北交所"}

def extract_market_from_code(code):
    """从股票代码中提取市场信息"""
    if code.startswith('sh.'):
        return 'sh'
    elif code.startswith('sz.'):
        return 'sz'
    elif code.startswith('bj.'):
        return 'bj'
    else:
        return 'unknown'

def clean_stock_code(code):
    """清理股票代码，去掉市场前缀"""
    return re.sub(r'^(sh\.|sz\.|bj\.)', '', str(code))

def load_and_insert_data_clickhouse(file_path, client):
    """读取CSV文件并插入到ClickHouse表中"""
    try:
        filename = os.path.basename(file_path)
        print(f"\n处理文件: {filename}")
        
        # 读取CSV文件
        df = pd.read_csv(file_path)
        
        if df.empty:
            print(f"文件 {filename} 为空，跳过处理")
            return False
        
        print(f"读取到 {len(df)} 行数据")
        
        # 检查必需的列是否存在
        if 'date' not in df.columns or 'code' not in df.columns:
            print(f"警告: 文件 {filename} 中没有 'date' 列或者'code'列，跳过处理")
            return False
        
        # 数据清洗和转换
        # 1. 处理日期列
        try:
            df['date'] = pd.to_datetime(df['date']).dt.date
            print("已将date列转换为日期格式")
        except Exception as e:
            print(f"转换date列时出错: {str(e)}")
            return False
        
        # 2. 提取市场信息并清理代码
        df['market'] = df['code'].apply(extract_market_from_code)
        df['code'] = df['code'].apply(clean_stock_code)
        
        # 3. 删除不需要的列
        if 'time' in df.columns:
            df.drop(columns=['time'], inplace=True)
        
        # 4. 重命名列以保持一致性
        if 'pctChg' in df.columns:
            df.rename(columns={'pctChg': 'p_change'}, inplace=True)
            print("已将pctChg列重命名为p_change")
        
        # 5. 处理空值和数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 
                          'adjustflag', 'turn', 'tradestatus', 'p_change', 'isST']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 6. 确保列顺序符合ClickHouse表结构
        expected_columns = [
            'date', 'code', 'market', 'open', 'high', 'low', 'close', 'preclose',
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 'p_change', 'isST'
        ]
        
        # 添加缺失的列
        for col in expected_columns:
            if col not in df.columns:
                if col == 'market':
                    df[col] = 'unknown'
                else:
                    df[col] = 0
        
        # 重新排序列
        df = df[expected_columns]
        
        # 插入数据到ClickHouse
        print(f"开始插入数据到ClickHouse表...")
        start_time = datetime.now()
        
        # 使用clickhouse_connect的insert_df方法批量插入
        client.insert_df('cn_stock_history', df)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"成功插入 {len(df)} 行数据到ClickHouse")
        print(f"耗时: {duration:.2f} 秒")
        
        return True
        
    except Exception as e:
        print(f"处理文件 {file_path} 时发生错误: {str(e)}")
        traceback.print_exc()
        return False

def process_all_data_to_clickhouse():
    """处理所有聚合数据文件并写入ClickHouse"""
    # 创建ClickHouse连接
    client = create_clickhouse_client()
    if client is None:
        print("无法连接到ClickHouse，请检查配置和依赖")
        return
    
    try:
        # 创建表结构
        print("创建ClickHouse表结构...")
        if not create_stock_history_table_clickhouse(client):
            print("创建表失败，退出处理")
            return
        
        # 查找所有数据文件
        pattern = os.path.join(AGG_DATA_DIR, "data_*.csv")
        files = glob.glob(pattern)
        
        # 排除已分市场的文件，只处理原始文件
        original_files = [f for f in files if not any(market in os.path.basename(f) for market in ['_sh_', '_sz_', '_bj_'])]
        
        if not original_files:
            print(f"在目录 {AGG_DATA_DIR} 中未找到原始数据文件")
            return
        
        print(f"找到 {len(original_files)} 个原始数据文件")
        
        success_count = 0
        failed_count = 0
        
        # 按文件名排序
        original_files.sort()
        
        for file_path in original_files:
            filename = os.path.basename(file_path)
            print(f"\n{'='*50}")
            print(f"处理文件: {filename}")
            
            if load_and_insert_data_clickhouse(file_path, client):
                success_count += 1
            else:
                failed_count += 1
        
        print(f"\n{'='*50}")
        print(f"处理完成!")
        print(f"成功: {success_count} 个文件")
        print(f"失败: {failed_count} 个文件")
        
        # 检查数据统计
        check_clickhouse_data_stats(client)
        
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        traceback.print_exc()
    
    finally:
        # 关闭连接
        client.close()
        print("ClickHouse连接已关闭")

def check_clickhouse_data_stats(client):
    """检查ClickHouse中的数据统计"""
    try:
        print(f"\n检查ClickHouse数据统计...")
        
        # 总记录数
        total_count = client.query("SELECT count() FROM cn_stock_history").result_rows[0][0]
        print(f"总记录数: {total_count:,}")
        
        # 按市场统计
        market_stats = client.query("""
            SELECT market, count() as count, 
                   min(date) as min_date, 
                   max(date) as max_date,
                   count(DISTINCT code) as unique_stocks
            FROM cn_stock_history 
            GROUP BY market 
            ORDER BY market
        """)
        
        print(f"\n按市场统计:")
        print("-" * 70)
        print(f"{'市场':<10} {'记录数':<15} {'股票数':<10} {'起始日期':<12} {'结束日期':<12}")
        print("-" * 70)
        
        for row in market_stats.result_rows:
            market, count, min_date, max_date, unique_stocks = row
            market_name = MARKET_PREFIXES.get(market, market)
            print(f"{market_name:<10} {count:<15,} {unique_stocks:<10} {min_date:<12} {max_date:<12}")
        
        # 按年份统计
        yearly_stats = client.query("""
            SELECT toYear(date) as year, count() as count
            FROM cn_stock_history 
            GROUP BY year 
            ORDER BY year
        """)
        
        print(f"\n按年份统计:")
        print("-" * 30)
        print(f"{'年份':<10} {'记录数':<15}")
        print("-" * 30)
        
        for row in yearly_stats.result_rows:
            year, count = row
            print(f"{year:<10} {count:<15,}")
        
        # 表大小统计
        table_size = client.query("""
            SELECT 
                formatReadableSize(sum(bytes_on_disk)) as size_on_disk,
                formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
                round(sum(data_compressed_bytes) / sum(data_uncompressed_bytes) * 100, 2) as compression_ratio
            FROM system.parts 
            WHERE table = 'cn_stock_history' AND active = 1
        """)
        
        if table_size.result_rows:
            size_on_disk, uncompressed_size, compression_ratio = table_size.result_rows[0]
            print(f"\n存储统计:")
            print(f"磁盘大小: {size_on_disk}")
            print(f"未压缩大小: {uncompressed_size}")
            print(f"压缩率: {compression_ratio}%")
        
    except Exception as e:
        print(f"检查数据统计时发生错误: {str(e)}")

def query_stock_data_examples(client):
    """一些查询示例，展示ClickHouse的查询能力"""
    try:
        print(f"\n=== ClickHouse查询示例 ===")
        
        # 示例1: 查询特定股票的历史数据
        print("\n1. 查询平安银行(000001)最近10个交易日数据:")
        result = client.query("""
            SELECT date, close, volume, p_change
            FROM cn_stock_history 
            WHERE code = '000001' 
            ORDER BY date DESC 
            LIMIT 10
        """)
        
        for row in result.result_rows:
            date, close, volume, p_change = row
            print(f"  {date}: 收盘价={close}, 成交量={volume:,}, 涨跌幅={p_change}%")
        
        # 示例2: 市场每日交易统计
        print("\n2. 各市场2024年日均交易额:")
        result = client.query("""
            SELECT market, 
                   round(avg(daily_amount)/100000000, 2) as avg_daily_amount_yi
            FROM (
                SELECT market, date, sum(amount) as daily_amount
                FROM cn_stock_history 
                WHERE toYear(date) = 2024
                GROUP BY market, date
            )
            GROUP BY market
            ORDER BY avg_daily_amount_yi DESC
        """)
        
        for row in result.result_rows:
            market, avg_amount = row
            market_name = MARKET_PREFIXES.get(market, market)
            print(f"  {market_name}: {avg_amount} 亿元")
        
        # 示例3: 涨停股票统计
        print("\n3. 2024年各月涨停股票数量:")
        result = client.query("""
            SELECT toYYYYMM(date) as month, count() as limit_up_count
            FROM cn_stock_history 
            WHERE toYear(date) = 2024 AND p_change >= 9.8
            GROUP BY month
            ORDER BY month
        """)
        
        for row in result.result_rows:
            month, count = row
            print(f"  {month}: {count} 次涨停")
            
    except Exception as e:
        print(f"执行查询示例时发生错误: {str(e)}")

if __name__ == "__main__":
    print("=== ClickHouse股票数据处理工具 ===")
    print("这个工具将股票数据导入ClickHouse，采用单表设计 + 按月分区")
    print("相比MySQL分表方案，具有以下优势:")
    print("1. 无需手动分表，自动按月分区")
    print("2. 列式存储，查询性能更好") 
    print("3. 高压缩率，存储成本更低")
    print("4. 支持复杂分析查询")
    print()
    
    choice = input("是否开始处理数据? (y/n): ").strip().lower()
    
    if choice == 'y':
        # 检查数据目录是否存在
        if not os.path.exists(AGG_DATA_DIR):
            print(f"错误: 数据目录 {AGG_DATA_DIR} 不存在")
            exit(1)
        
        print("开始处理聚合数据并写入ClickHouse数据库...")
        process_all_data_to_clickhouse()
        
        # 执行查询示例
        client = create_clickhouse_client()
        if client:
            query_stock_data_examples(client)
            client.close()
    else:
        print("程序退出")
