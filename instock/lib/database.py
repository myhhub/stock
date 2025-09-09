#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import pymysql
import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import pandas as pd

__author__ = 'myh '
__date__ = '2023/3/10 '

db_host = "192.168.1.6"  # 数据库服务主机
db_user = "root"  # 数据库访问用户
db_password = "LZHlzh.rootOOT12#"  # 数据库访问密码
db_database = "instockdb"  # 数据库名称
db_port = 3306  # 数据库服务端口
db_charset = "utf8mb4"  # 数据库字符集

# 使用环境变量获得数据库,docker -e 传递
_db_host = os.environ.get('MYSQL_HOST')
if _db_host is not None:
    db_host = _db_host
_db_user = os.environ.get('MYSQL_USER')
if _db_user is not None:
    db_user = _db_user
_db_password = os.environ.get('MYSQL_PASSWORD')
if _db_password is not None:
    db_password = _db_password
_db_database = os.environ.get('MYSQL_DATABASE')
if _db_database is not None:
    db_database = _db_database
_db_port = int(os.environ.get('MYSQL_PORT'))
if _db_port is not None:
    db_port = int(_db_port)

MYSQL_CONN_URL = "mysql+pymysql://%s:%s@%s:%s/%s?charset=%s" % (
    db_user, db_password, db_host, db_port, db_database, db_charset)
logging.info(f"数据库链接信息：{ MYSQL_CONN_URL}")

MYSQL_CONN_DBAPI = {'host': db_host, 'user': db_user, 'password': db_password, 'database': db_database,
                    'charset': db_charset, 'port': db_port, 'autocommit': True}

MYSQL_CONN_TORNDB = {'host': f'{db_host}:{str(db_port)}', 'user': db_user, 'password': db_password,
                     'database': db_database, 'charset': db_charset, 'max_idle_time': 3600, 'connect_timeout': 1000}


# 通过数据库链接 engine
# 全局连接池实例
_engine_pool = None
_engine_pool_to_db = {}

# 通过数据库链接 engine（带连接池）
def engine():
    global _engine_pool
    if _engine_pool is None:
        _engine_pool = create_engine(
            MYSQL_CONN_URL,
            pool_size=100,           # 连接池大小
            max_overflow=50,        # 最大溢出连接数
            pool_pre_ping=True,     # 连接健康检查
            pool_recycle=60,      # 连接回收时间（秒）
            echo=False              # 不输出SQL日志
        )
    return _engine_pool


def engine_to_db(to_db):
    global _engine_pool_to_db
    if to_db not in _engine_pool_to_db:
        _engine_pool_to_db[to_db] = create_engine(
            MYSQL_CONN_URL.replace(f'/{db_database}?', f'/{to_db}?'),
            pool_size=100,           # 连接池大小
            max_overflow=50,        # 最大溢出连接数
            pool_pre_ping=True,     # 连接健康检查
            pool_recycle=60,      # 连接回收时间（秒）
            echo=False              # 不输出SQL日志
        )
    return _engine_pool_to_db[to_db]

# DB Api -数据库连接对象connection
def get_connection():
    try:
        return pymysql.connect(**MYSQL_CONN_DBAPI)
    except Exception as e:
        logging.error(f"database.conn_not_cursor处理异常：{MYSQL_CONN_DBAPI}{e}")
    return None


# 定义通用方法函数，插入数据库表，并创建数据库主键，保证重跑数据的时候索引唯一。
def insert_db_from_df(data, table_name, cols_type, write_index, primary_keys, indexs=None):
    # 插入默认的数据库。
    insert_other_db_from_df(None, data, table_name, cols_type, write_index, primary_keys, indexs)


# 增加一个插入到其他数据库的方法。
def insert_other_db_from_df(to_db, data, table_name, cols_type, write_index, primary_keys, indexs=None):
    # 定义engine
    if to_db is None:
        engine_mysql = engine()
    else:
        engine_mysql = engine_to_db(to_db)
    # 使用 http://docs.sqlalchemy.org/en/latest/core/reflection.html
    # 使用检查检查数据库表是否有主键。
    ipt = inspect(engine_mysql)
    col_name_list = data.columns.tolist()
    # 如果有索引，把索引增加到varchar上面。
    if write_index:
        # 插入到第一个位置：
        col_name_list.insert(0, data.index.name)
    try:
        if cols_type is None:
            data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                        index=write_index, )
        elif not cols_type:
            data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                        dtype={col_name: NVARCHAR(255) for col_name in col_name_list}, index=write_index, )
        else:
            data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                        dtype=cols_type, index=write_index, )
    except Exception as e:
        logging.error(f"database.insert_other_db_from_df处理异常：{table_name}表{e}")

    # 判断是否存在主键
    if not ipt.get_pk_constraint(table_name)['constrained_columns']:
        try:
            # 执行数据库插入数据。
            with get_connection() as conn:
                with conn.cursor() as db:
                    db.execute(f'ALTER TABLE `{table_name}` ADD PRIMARY KEY ({primary_keys});')
                    if indexs is not None:
                        for k in indexs:
                            db.execute(f'ALTER TABLE `{table_name}` ADD INDEX IN{k}({indexs[k]});')
        except Exception as e:
            logging.error(f"database.insert_other_db_from_df处理异常：{table_name}表{e}")


# 检查表是否存在
def checkTableIsExist(tableName):
    from instock.lib.database_factory import get_database
    db = get_database()
    sql = f"SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = '{tableName}'"
    res = db.query(sql)
    if res and int(res[0]['count']) > 0:
        return True
    return False


# 增删改数据
def executeSql(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as db:
            try:
                db.execute(sql, params)
            except Exception as e:
                logging.error(f"database.executeSql处理异常：{sql}{e}")


# 查询数据
def executeSqlFetch(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as db:
            try:
                db.execute(sql, params)
                return db.fetchall()
            except Exception as e:
                logging.error(f"database.executeSqlFetch处理异常：{sql}{e}")
    return None


# 计算数量
def executeSqlCount(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as db:
            try:
                db.execute(sql, params)
                result = db.fetchall()
                if len(result) == 1:
                    return int(result[0][0])
                else:
                    return 0
            except Exception as e:
                logging.error(f"database.select_count计算数量处理异常：{e}")
    return 0


# 使用pandas读取数据库并支持变量查询
def read_sql_to_df(sql, params=None, to_db=None):
    """
    使用pandas读取数据库并支持变量查询
    
    Args:
        sql: SQL查询语句，支持%s占位符
        params: 查询参数元组或字典
        to_db: 指定数据库，None使用默认数据库
    
    Returns:
        pandas.DataFrame: 查询结果
    """

    
    try:
        if to_db is None:
            engine_mysql = engine()
        else:
            engine_mysql = engine_to_db(to_db)
        
        # 使用pandas的read_sql方法，自动处理参数化查询
        df = pd.read_sql(sql, engine_mysql, params=params)
        return df
        
    except Exception as e:
        logging.error(f"database.read_sql_to_df处理异常：{sql}{e}")
        return pd.DataFrame()


# 使用pandas批量读取数据库表
def read_table_to_df(table_name, columns=None, where=None, params=None, to_db=None, limit=None):
    """
    使用pandas批量读取数据库表
    
    Args:
        table_name: 表名
        columns: 指定列名列表，None表示所有列
        where: WHERE条件语句（不含WHERE关键字）
        params: WHERE条件参数
        to_db: 指定数据库
        limit: 限制返回行数
    
    Returns:
        pandas.DataFrame: 查询结果
    """
    import pandas as pd
    
    try:
        if to_db is None:
            engine_mysql = engine()
        else:
            engine_mysql = engine_to_db(to_db)
        
        # 构建查询
        if columns:
            cols_str = ', '.join([f'`{col}`' for col in columns])
            sql = f"SELECT {cols_str} FROM `{table_name}`"
        else:
            sql = f"SELECT * FROM `{table_name}`"
        
        # 添加WHERE条件
        if where:
            sql += f" WHERE {where}"
        
        # 添加LIMIT
        if limit:
            sql += f" LIMIT {limit}"
        
        # 执行查询
        df = pd.read_sql(sql, engine_mysql, params=params)
        return df
        
    except Exception as e:
        logging.error(f"database.read_table_to_df处理异常：{table_name}表{e}")
        return pd.DataFrame()


# 查询范围，如果开始时间是在2020_2024的表之间
def get_history_table_data_by_start_date_from_now(date_start, date_end=None, base_name="cn_stock_history_sz"):
    """
    根据开始日期获取历史表名列表和查询条件
    
    Args:
        base_name: 基础表名
        date_start: 开始日期对象或字符串（格式：YYYY-MM-DD）
        date_end: 结束日期对象或字符串（格式：YYYY-MM-DD），None表示当前日期
    
    Returns:
        dict: 包含表名和查询条件的字典
        {
            'tables': [
                {
                    'table_name': 'cn_stock_history_2020_2024',
                    'date_condition': "date >= '2022-01-01' AND date <= '2024-12-31'"
                },
                {
                    'table_name': 'cn_stock_history_2025_2029', 
                    'date_condition': "date >= '2025-01-01' AND date <= '2025-08-26'"
                }
            ]
        }
    """
    if isinstance(date_start, str):
        date_start = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    if date_end is None:
        date_end = datetime.datetime.now()
    elif isinstance(date_end, str):
        date_end = datetime.datetime.strptime(date_end, "%Y-%m-%d")
    
    tables = []
    start_year = (date_start.year // 5) * 5
    end_year = (date_end.year // 5) * 5
    
    for year in range(start_year, end_year + 1, 5):
        table_name = f"{base_name}_{year}_{year + 4}"
        
        # 计算这个表的实际查询日期范围
        table_start_date = max(date_start, datetime.datetime(year, 1, 1))
        table_end_date = min(date_end, datetime.datetime(year + 4, 12, 31))
        
        # 如果表的日期范围和查询范围有交集
        if table_start_date <= table_end_date:
            # 对于 datetime 类型的 date 字段，使用 DATE() 函数进行日期比较
            date_condition = f"DATE(date) >= '{table_start_date.strftime('%Y-%m-%d')}' AND DATE(date) <= '{table_end_date.strftime('%Y-%m-%d')}'"
            
            tables.append({
                'table_name': table_name,
                'date_condition': date_condition,
                'start_date': table_start_date,
                'end_date': table_end_date
            })
    
    return {'tables': tables}

def query_history_data_by_date_range(date_start, date_end=None, base_name="cn_stock_history_sz", 
                                    columns=None, where_extra=None, params=None, to_db=None):
    """
    查询指定日期范围的历史数据，自动处理跨表查询和数据拼接
    
    Args:
        date_start: 开始日期对象或字符串（格式：YYYY-MM-DD）
        date_end: 结束日期对象或字符串（格式：YYYY-MM-DD），None表示当前日期
        base_name: 基础表名
        columns: 指定列名列表，None表示所有列
        where_extra: 额外的WHERE条件（不含日期条件），例如 "code = %s"
        params: WHERE条件参数
        to_db: 指定数据库
    
    Returns:
        pandas.DataFrame: 拼接后的查询结果
    """
    import pandas as pd
    
    # 获取需要查询的表信息
    table_info = get_history_table_data_by_start_date_from_now(date_start, date_end, base_name)
    
    all_data = []
    
    for table_data in table_info['tables']:
        table_name = table_data['table_name']
        date_condition = table_data['date_condition']
        
        # 检查表是否存在
        if not checkTableIsExist(table_name):
            logging.warning(f"表 {table_name} 不存在，跳过查询")
            continue
        
        try:
            # 构建查询语句
            if columns:
                cols_str = ', '.join([f'`{col}`' for col in columns])
                sql = f"SELECT {cols_str} FROM `{table_name}`"
            else:
                sql = f"SELECT * FROM `{table_name}`"
            
            # 构建WHERE条件
            where_conditions = [date_condition]
            if where_extra:
                where_conditions.append(where_extra)
            query_params = []
            if params:
                if isinstance(params, (list, tuple)):
                    query_params.extend(params)
                elif isinstance(params, dict):
                    query_params = params
                else:
                    query_params.append(params)            
            if where_conditions:
                sql += f" WHERE {' AND '.join(where_conditions)}"
            
            # 添加排序
            sql += " ORDER BY date ASC"
            
            logging.info(f"查询表 {table_name}，日期范围：{table_data['start_date'].strftime('%Y-%m-%d')} 到 {table_data['end_date'].strftime('%Y-%m-%d')}")
            
            # 执行查询
            df = read_sql_to_df(sql, tuple(query_params), to_db)
            
            if not df.empty:
                all_data.append(df)
                logging.info(f"从表 {table_name} 查询到 {len(df)} 条记录")
            
        except Exception as e:
            logging.error(f"查询表 {table_name} 时发生错误：{e}")
            continue
    
    # 拼接所有数据
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        # 按日期重新排序
        if 'date' in result_df.columns:
            try:
                # 确保 date 列是 datetime 类型
                result_df['date'] = pd.to_datetime(result_df['date'])
                result_df = result_df.sort_values('date').reset_index(drop=True)
            except Exception as e:
                logging.warning(f"日期排序失败，跳过排序：{e}")
                # 如果排序失败，至少保证返回数据
                pass
        
        logging.info(f"总共查询到 {len(result_df)} 条记录")
        return result_df
    else:
        logging.warning("没有查询到任何数据")
        return pd.DataFrame()


def save_batch_realtime_data_to_history(data, table_object):
    """
    批量将实时数据保存到ClickHouse历史数据库
    
    Args:
        data: pandas.DataFrame - 实时数据（必须包含date字段）
        table_object: dict - 表结构对象
    
    Returns:
        bool: 操作是否成功
    """
    import pandas as pd
    from instock.lib.database_factory import get_database
    
    if data is None or len(data) == 0:
        logging.warning("没有数据需要保存到历史数据库")
        return False
    
    # 检查数据是否包含date字段
    if 'date' not in data.columns:
        logging.error("数据中缺少date字段，无法保存到历史数据库")
        return False
    
    try:
        columns_mapping = table_object.get('columns_to_history_db', {})
        db_description = table_object.get('db_description', {})
        
        # 统一表名，不再分表
        table_name = "cn_stock_history"
        
        # 字段重命名和映射
        mapped_data = pd.DataFrame()
        for api_field, db_field in columns_mapping.items():
            if api_field in data.columns:
                mapped_data[db_field] = data[api_field]
        
        # 添加必要字段
        mapped_data['code'] = data['code']
        if 'date' not in mapped_data.columns:
            mapped_data['date'] = data['date']
        
        # 处理计算字段（批量）
        if 'amount' in db_description and 'amount' not in mapped_data.columns:
            if 'volume' in mapped_data.columns and 'close' in mapped_data.columns:
                mapped_data['amount'] = mapped_data['volume'] * mapped_data['close']
        
        if 'isST' in db_description:
            if 'name' in data.columns:
                mapped_data['isST'] = data['name'].str.startswith(('*ST', 'ST')).astype(int)
            else:
                mapped_data['isST'] = 0
        
        if 'tradestatus' in db_description:
            mapped_data['tradestatus'] = 1
            
        if 'adjustflag' in db_description:
            mapped_data['adjustflag'] = 3
        
        # 成交量单位转换
        if 'volume' in mapped_data.columns:
            mapped_data['volume'] = mapped_data['volume'] * 100
        
        # 批量保存到ClickHouse
        if not mapped_data.empty:
            db = get_database()
            success = db.insert_from_dataframe(mapped_data, table_name)
            if success:
                logging.info(f"成功批量保存 {len(mapped_data)} 条数据到ClickHouse表 {table_name}")
            else:
                logging.error(f"批量保存数据到ClickHouse表 {table_name} 失败")
            return success
        
        return True
        
    except Exception as e:
        logging.error(f"批量保存实时数据到ClickHouse历史数据库时发生异常：{e}")
        return False


def _upsert_history_data(data, table_name):
    """
    向历史表插入或更新数据，处理date和code的重复数据
    
    Args:
        data: pandas.DataFrame - 要插入的数据
        table_name: str - 历史表名
    
    Returns:
        bool: 操作是否成功
    """
    from sqlalchemy.types import DATE, VARCHAR, FLOAT, BIGINT
    
    try:
        # 检查表是否存在
        table_exists = checkTableIsExist(table_name)
        
        if not table_exists:
            # 如果表不存在，先创建表结构
            logging.info(f"历史表 {table_name} 不存在，准备创建")
            
            # 定义历史表的基本字段类型
            history_cols_type = {
                'date': DATE,
                'code': VARCHAR(6),
                'open': FLOAT,
                'high': FLOAT, 
                'low': FLOAT,
                'close': FLOAT,
                'preclose': FLOAT,
                'volume': BIGINT,
                'amount': BIGINT,
                'turnover': FLOAT,
                'p_change': FLOAT,
                'isST': BIGINT,
                'trade_status': BIGINT,
                'adjustflag': BIGINT
            }
            
            # 创建表（使用第一条记录来触发表创建）
            sample_data = data.head(1)
            sample_data.to_sql(name=table_name, con=engine(), if_exists='append', 
                             index=False, dtype=history_cols_type)
            
            # 创建主键和索引
            with get_connection() as conn:
                with conn.cursor() as db:
                    try:
                        # 添加主键
                        db.execute(f'ALTER TABLE `{table_name}` ADD PRIMARY KEY (`date`, `code`);')
                        # 添加索引
                        db.execute(f'ALTER TABLE `{table_name}` ADD INDEX IDX_DATE (`date`);')
                        db.execute(f'ALTER TABLE `{table_name}` ADD INDEX IDX_CODE (`code`);')
                        logging.info(f"成功为表 {table_name} 创建主键和索引")
                    except Exception as e:
                        logging.warning(f"创建主键和索引时发生异常（可能已存在）：{e}")
            
            # 移除已插入的示例数据
            delete_sql = f"DELETE FROM `{table_name}` WHERE date = %s AND code = %s"
            first_row = sample_data.iloc[0]
            executeSql(delete_sql, (first_row['date'], first_row['code']))
        
        # 执行批量UPSERT操作
        upsert_count = 0
        for _, row in data.iterrows():
            date_val = row['date']
            code_val = row['code']
            
            # 使用 INSERT ... ON DUPLICATE KEY UPDATE 实现UPSERT
            columns = list(data.columns)
            column_names = ', '.join([f"`{col}`" for col in columns])
            placeholders = ', '.join(['%s'] * len(columns))
            
            # 构建UPDATE部分（排除主键字段）
            update_parts = []
            for col in columns:
                if col not in ['date', 'code']:
                    update_parts.append(f"`{col}` = VALUES(`{col}`)")
            
            if update_parts:  # 如果有可更新的字段
                upsert_sql = f"""
                INSERT INTO `{table_name}` ({column_names}) 
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {', '.join(update_parts)}
                """
            else:  # 如果只有主键字段，使用IGNORE
                upsert_sql = f"""
                INSERT IGNORE INTO `{table_name}` ({column_names}) 
                VALUES ({placeholders})
                """
            
            executeSql(upsert_sql, tuple(row[col] for col in columns))
            upsert_count += 1
        
        logging.info(f"成功处理 {upsert_count} 条记录到历史表 {table_name}")
        return True
        
    except Exception as e:
        logging.error(f"向历史表 {table_name} 执行UPSERT操作时发生异常：{e}")
        return False
