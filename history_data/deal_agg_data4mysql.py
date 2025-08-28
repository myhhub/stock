import pandas as pd
import os
import traceback
import pymysql
from sqlalchemy import text
import glob
import re
from datetime import datetime

from db_engine import create_mysql_engine, DB_CONFIG, AGG_DATA_DIR

MARKET = ["sh", "sz", "bj"]


def create_table_if_not_exists(engine, table_name, sample_df):
    """
    根据数据框结构创建表（如果不存在）
    """
    try:
        with engine.connect() as conn:
            # 检查表是否存在
            check_table_sql = f"""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = '{DB_CONFIG['database']}' 
            AND table_name = '{table_name}'
            """
            
            result = conn.execute(text(check_table_sql)).fetchone()
            
            if result[0] == 0:
                print(f"表 {table_name} 不存在，正在创建...")
                
                # 根据DataFrame的数据类型生成CREATE TABLE语句
                column_definitions = []
                
                for col_name, dtype in sample_df.dtypes.items():
                    if col_name.lower() == 'date':
                        col_type = 'DATE'
                    elif col_name.lower() == 'code':
                        col_type = 'VARCHAR(20)'
                    elif 'int' in str(dtype):
                        col_type = 'BIGINT'
                    elif 'float' in str(dtype):
                        col_type = 'DECIMAL(20,6)'
                    elif 'datetime' in str(dtype) or 'date' in str(dtype):
                        col_type = 'DATE'
                    else:
                        col_type = 'VARCHAR(255)'
                    
                    column_definitions.append(f"`{col_name}` {col_type}")
                
                # 添加主键ID列
                columns_sql = "id BIGINT AUTO_INCREMENT PRIMARY KEY, " + ", ".join(column_definitions)
                
                create_table_sql = f"""
                CREATE TABLE `{table_name}` (
                    {columns_sql}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                
                conn.execute(text(create_table_sql))
                
                # 创建索引
                if 'date' in sample_df.columns:
                    index_date_sql = f"CREATE INDEX idx_{table_name}_date ON `{table_name}` (`date`)"
                    conn.execute(text(index_date_sql))
                    print(f"已创建date列索引: idx_{table_name}_date")
                
                if 'code' in sample_df.columns:
                    index_code_sql = f"CREATE INDEX idx_{table_name}_code ON `{table_name}` (`code`)"
                    conn.execute(text(index_code_sql))
                    print(f"已创建code列索引: idx_{table_name}_code")
                
                # 创建联合索引
                if 'date' in sample_df.columns and 'code' in sample_df.columns:
                    index_combined_sql = f"CREATE INDEX idx_{table_name}_date_code ON `{table_name}` (`date`, `code`)"
                    conn.execute(text(index_combined_sql))
                    print(f"已创建date+code联合索引: idx_{table_name}_date_code")
                
                conn.commit()
                print(f"成功创建表: {table_name}")
            else:
                print(f"表 {table_name} 已存在")
                
    except Exception as e:
        print(f"创建表 {table_name} 时发生错误: {str(e)}")

def extract_years_from_filename(filename):
    """从文件名中提取年份范围"""
    # 匹配格式: data_2000_2004.csv
    match = re.search(r'data_\w+_(\d{4})_(\d{4})\.csv', filename)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def load_and_insert_data(file_path, engine, market=""):
    """读取CSV文件并插入到对应的MySQL表中"""
    try:
        if market not in MARKET and market != "":
            print(f"无效的市场前缀: {market}")
            return False
        filename = os.path.basename(file_path)
        from_year, end_year = extract_years_from_filename(filename)
        
        if from_year is None or end_year is None:
            print(f"警告: 无法从文件名 {filename} 中提取年份，跳过处理")
            return False
        
        # 生成表名
        market_prefix = f"{market}_" if market else ""
        table_name = f"cn_stock_history_{market_prefix}{from_year}_{end_year}"
        
        print(f"\n处理文件: {filename} -> 目标表名: {table_name}")
        
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
        
        # 处理日期列 - 转换为datetime格式
        try:
            df['date'] = pd.to_datetime(df['date']).dt.date
            print("已将date列转换为日期格式")
        except Exception as e:
            print(f"转换date列时出错: {str(e)}")
            return False
        
        # 处理code列 - 去掉市场前缀(sh., sz., bj.)
        if df['code'].dtype == 'object':  # 确保是字符串类型
            df['code'] = df['code'].str.replace(r'^(sh\.|sz\.|bj\.)', '', regex=True)
            print("已去掉code列的市场前缀")
        
        # 处理其他可能的日期列
        for col in df.columns:
            if col != 'date' and ('time' in col.lower()):
                try:
                    df[col] = pd.to_datetime(df[col]).dt.date
                except:
                    pass
        # 删除time列
        if 'time' in df.columns:
            df.drop(columns=['time'], inplace=True)
        # pctChg列改为p_change兼容业务代码
        if 'pctChg' in df.columns:
            df.rename(columns={'pctChg': 'p_change'}, inplace=True)
            print("已将pctChg列重命名为p_change")
        # 创建表（如果不存在）
        create_table_if_not_exists(engine, table_name, df)
        
        # 插入数据
        print(f"开始插入数据到表 {table_name}...")
        start_time = datetime.now()
        
        # 使用to_sql方法批量插入数据
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append', 
            index=False,        
            method='multi',      # 使用批量插入
            chunksize=5000       # 每次插入5000行（优化后的批量大小）
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"成功插入 {len(df)} 行数据到表 {table_name}")
        print(f"耗时: {duration:.2f} 秒")
        
        return True
        
    except Exception as e:
        print(f"处理文件 {file_path} 时发生错误: {str(e)}")
        traceback.print_exc()
        return False

def process_all_agg_data(market=""):
    """
    处理所有聚合数据文件
    market参数可选，指定市场前缀（如'sh', 'sz', 'bj'），为空则处理所有市场
    """
    # 创建数据库连接
    engine = create_mysql_engine()
    if engine is None:
        return
    
    try:
        # 查找所有数据文件
        if market:
            pattern = os.path.join(AGG_DATA_DIR, f"data_{market}_*.csv")
        else:
            pattern = os.path.join(AGG_DATA_DIR, "data_*.csv")
        files = glob.glob(pattern)
        
        if not files:
            print(f"在目录 {AGG_DATA_DIR} 中未找到数据文件")
            return
        
        print(f"找到 {len(files)} 个数据文件")
        
        success_count = 0
        failed_count = 0
        
        # 按文件名排序
        files.sort()
        
        for file_path in files:
            filename = os.path.basename(file_path)
            print(f"\n{'='*50}")
            print(f"处理文件: {filename}")
            # TODO 先测试2025的
            if "2020" in filename:
                if load_and_insert_data(file_path, engine, market):
                    success_count += 1
                else:
                    failed_count += 1
        
        print(f"\n{'='*50}")
        print(f"处理完成!")
        print(f"成功: {success_count} 个文件")
        print(f"失败: {failed_count} 个文件")
        
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
    
    finally:
        # 关闭数据库连接
        engine.dispose()
        print("数据库连接已关闭")

def split_data_by_market():
    """根据股票代码将数据分市场存储"""
    try:
        # 查找所有数据文件
        pattern = os.path.join(AGG_DATA_DIR, "data_*.csv")
        files = glob.glob(pattern)
        if not files:
            print(f"在目录 {AGG_DATA_DIR} 中未找到数据文件")
            return
        # 检查文件名长度是否一致，不一致说明已经被分割过
        lengths = {len(os.path.basename(f)) for f in files}
        if len(lengths) > 1:
            print("检测到数据文件名长度不一致，可能已经被分割过，跳过分割步骤")
            return
 
        
        print(f"找到 {len(files)} 个数据文件")
        
        for file_path in files:
            filename = os.path.basename(file_path)

            print(f"\n处理文件: {filename}")
            
            # 提取年份范围
            from_year, end_year = extract_years_from_filename(filename)
            if from_year is None or end_year is None:
                print(f"警告: 无法从文件名 {filename} 中提取年份，跳过处理")
                continue
            
            year_range = f"{from_year}_{end_year}"
            
            # 读取CSV文件
            df = pd.read_csv(file_path)
            if df.empty:
                print(f"文件 {filename} 为空，跳过处理")
                continue
            
            print(f"读取到 {len(df)} 行数据")
            
            # 根据code列前缀分组
            market_data = {
                'sh': df[df['code'].str.startswith('sh.')].copy(),
                'sz': df[df['code'].str.startswith('sz.')].copy(),
                'bj': df[df['code'].str.startswith('bj.')].copy()
            }
            
            # 保存各市场数据
            for market_name, market_df in market_data.items():
                if not market_df.empty:
                    output_filename = f"data_{market_name}_{year_range}.csv"
                    output_path = os.path.join(AGG_DATA_DIR, output_filename)
                    
                    market_df.to_csv(output_path, index=False)
                    print(f"  保存 {market_name.upper()} 市场数据: {len(market_df)} 行 -> {output_filename}")
                else:
                    print(f"  {market_name.upper()} 市场无数据")
        
        print(f"\n数据分市场处理完成!")
        
    except Exception as e:
        print(f"分市场处理数据时发生错误: {str(e)}")

def check_database_status(engine):
    """检查数据库状态和表信息"""
    try:
        with engine.connect() as conn:
            # 查看所有相关表
            tables_sql = f"""
            SELECT table_name, table_rows, data_length, index_length
            FROM information_schema.tables 
            WHERE table_schema = '{DB_CONFIG['database']}' 
            AND table_name LIKE 'cn_stock_history_%'
            ORDER BY table_name
            """
            
            result = conn.execute(text(tables_sql)).fetchall()
            
            if result:
                print(f"\n数据库中的股票历史数据表:")
                print("-" * 80)
                print(f"{'表名':<30} {'行数':<15} {'数据大小(MB)':<15} {'索引大小(MB)':<15}")
                print("-" * 80)
                
                total_rows = 0
                total_size = 0
                
                for row in result:
                    table_name, rows, data_length, index_length = row
                    data_mb = (data_length or 0) / 1024 / 1024
                    index_mb = (index_length or 0) / 1024 / 1024
                    total_rows += rows or 0
                    total_size += data_mb + index_mb
                    
                    print(f"{table_name:<30} {rows or 0:<15} {data_mb:<15.2f} {index_mb:<15.2f}")
                
                print("-" * 80)
                print(f"总计: {len(result)} 个表, {total_rows} 行数据, {total_size:.2f} MB")
            else:
                print("未找到相关的股票历史数据表")
                
    except Exception as e:
        print(f"检查数据库状态时发生错误: {str(e)}")

if __name__ == "__main__":
    print("股票数据处理工具")
    print("1. 处理聚合数据并写入MySQL数据库")
    print("2. 按市场分割数据文件")
    
    choice = input("请选择操作 (1/2): ").strip()
    
    # 检查数据目录是否存在
    if not os.path.exists(AGG_DATA_DIR):
        print(f"错误: 数据目录 {AGG_DATA_DIR} 不存在")
        exit(1)
    
    if choice == "1":
        print("开始处理聚合数据并写入MySQL数据库...")
        # 处理所有数据
        process_all_agg_data()
        
        # 检查数据库状态
        print(f"\n检查数据库状态...")
        engine = create_mysql_engine()
        if engine:
            check_database_status(engine)
            engine.dispose()
    
    elif choice == "2":
        print("开始按市场分割数据文件...")
        split_data_by_market()
        market_dict = {"1": "sh", "2": "sz", "3": "bj", "4": "all"}
        print("""
        请选择市场前缀进行数据入库处理:
        1. sh
        2. sz
        3. bj
        4. all (处理所有市场)
              """)
        market_choice = input("请输入选择 (1/2/3/4),其他任意按键结束: ").strip()
        if market_choice in market_dict:
            selected_market = market_dict[market_choice]
            print(f"处理市场: {selected_market}")
            if selected_market == "all":
                process_all_agg_data("sh")
                process_all_agg_data("sz")
                process_all_agg_data("bj")
            else:
                process_all_agg_data(market=selected_market)
        else:
            print("无效的选择，程序退出")
        market = ["sh", "sz", "bj"]
        
        
    
    else:
        print("无效的选择，程序退出")
