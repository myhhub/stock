#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
from enum import Enum
from typing import Optional, Dict, Any, Union, List, Tuple
import pandas as pd
from datetime import datetime, time
try:
    import pymysql
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.types import NVARCHAR
except ImportError:
    logging.warning("MySQL dependencies not installed")
    pymysql = None

__author__ = 'myh '
__date__ = '2023/3/10 '

class DatabaseType(Enum):
    """数据库类型枚举"""
    MYSQL = "mysql"
    CLICKHOUSE = "clickhouse"

# 数据库配置类
class DatabaseConfig:
    """数据库配置管理"""
    
    def __init__(self):
        # 从环境变量获取数据库类型，默认使用MySQL
        self.db_type = DatabaseType(os.environ.get('DB_TYPE', 'clickhouse').lower())
        
        # MySQL配置
        self.mysql_config = {
            'host': os.environ.get('MYSQL_HOST', '192.168.1.6'),
            'port': int(os.environ.get('MYSQL_PORT', '3306')),
            'user': os.environ.get('MYSQL_USER', 'root'),
            'password': os.environ.get('MYSQL_PASSWORD', 'LZHlzh.rootOOT12#'),
            'database': os.environ.get('MYSQL_DATABASE', 'instockdb'),
            'charset': os.environ.get('MYSQL_CHARSET', 'utf8mb4')
        }
        
        # ClickHouse配置
        self.clickhouse_config = {
            'host': os.environ.get('CLICKHOUSE_HOST', '192.168.1.6'),
            'port': int(os.environ.get('CLICKHOUSE_PORT', '8123')),
            'tcp': os.environ.get('CLICKHOUSE_TCP', '9000'),
            'username': os.environ.get('CLICKHOUSE_USER', 'root'),
            'password': os.environ.get('CLICKHOUSE_PASSWORD', '123456'),
            'database': os.environ.get('CLICKHOUSE_DATABASE', 'instockdb')
        }
        
        logging.info(f"数据库配置初始化完成，当前使用: {self.db_type.value}")
    
    def get_current_config(self) -> Dict[str, Any]:
        """获取当前数据库配置"""
        if self.db_type == DatabaseType.MYSQL:
            return self.mysql_config
        elif self.db_type == DatabaseType.CLICKHOUSE:
            return self.clickhouse_config
        else:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")
    
    def switch_database(self, db_type: Union[str, DatabaseType]):
        """切换数据库类型"""
        if isinstance(db_type, str):
            db_type = DatabaseType(db_type.lower())
        
        self.db_type = db_type
        logging.info(f"数据库类型已切换至: {self.db_type.value}")

# 全局配置实例
db_config = DatabaseConfig()

# 数据库抽象接口
class DatabaseInterface:
    """数据库操作抽象接口"""
    
    def query(self, sql: str, *params) -> List[Dict]:
        """执行查询并返回结果"""
        raise NotImplementedError
    
    def query_to_dataframe(self, sql: str, params: Optional[Union[Tuple, Dict]] = None) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        raise NotImplementedError
    
    def execute(self, sql: str, *params) -> bool:
        """执行SQL语句"""
        raise NotImplementedError
    
    def _convert_dataframe_to_records(self, df: pd.DataFrame) -> List[Dict]:
        """将DataFrame转换为记录列表，处理datetime对象"""
        if df.empty:
            return []
        
        # 创建DataFrame副本以避免修改原始数据
        df_copy = df.copy()
        
        # 处理datetime列，将其转换为字符串格式
        for col in df_copy.columns:
            if df_copy[col].dtype.name.startswith('datetime'):
                # 检查是否包含时间信息
                sample_non_null = df_copy[col].dropna()
                if not sample_non_null.empty:
                    sample_value = sample_non_null.iloc[0]
                    if hasattr(sample_value, 'time') and sample_value.time() != time(0):
                        # 包含时间信息，使用完整的datetime格式
                        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # 只有日期信息，使用日期格式
                        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
            elif hasattr(df_copy[col].dtype, 'type') and issubclass(df_copy[col].dtype.type, pd.Timestamp):
                # 处理Timestamp类型
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return df_copy.to_dict('records')
    
    def close(self):
        """关闭连接"""
        raise NotImplementedError
    
    def insert_from_dataframe(self, df: pd.DataFrame, table_name: str, cols_type=None, write_index=False, primary_keys=None):
        """从DataFrame插入数据"""
        raise NotImplementedError

# MySQL实现
class MySQLDatabase(DatabaseInterface):
    """MySQL数据库实现"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or db_config.mysql_config
        self._connection = None
        self._torndb_connection = None
        self._init_connections()
    
    def _init_connections(self):
        """初始化数据库连接"""
        try:
            import instock.lib.torndb as torndb
            import pymysql
            
            # TornDB连接（用于查询）
            torndb_config = {
                'host': f"{self.config['host']}:{self.config['port']}",
                'user': self.config['user'],
                'password': self.config['password'],
                'database': self.config['database'],
                'charset': self.config['charset'],
                'max_idle_time': 3600,
                'connect_timeout': 1000
            }
            self._torndb_connection = torndb.Connection(**torndb_config)
            
            # PyMySQL连接（用于执行）
            pymysql_config = {
                'host': self.config['host'],
                'port': self.config['port'],
                'user': self.config['user'],
                'password': self.config['password'],
                'database': self.config['database'],
                'charset': self.config['charset'],
                'autocommit': True
            }
            self._connection = pymysql.connect(**pymysql_config)
            
            logging.info(f"MySQL连接初始化成功: {self.config['host']}:{self.config['port']}/{self.config['database']}")
            
        except Exception as e:
            logging.error(f"MySQL连接初始化失败: {e}")
            raise
    
    def query(self, sql: str, *params) -> List[Dict]:
        """执行查询并返回结果"""
        try:
            if params:
                return self._torndb_connection.query(sql, *params)
            else:
                return self._torndb_connection.query(sql)
        except Exception as e:
            logging.error(f"MySQL查询错误: {e}, SQL: {sql}")
            raise
    
    def query_to_dataframe(self, sql: str, params: Optional[Union[Tuple, Dict]] = None) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        try:
            from sqlalchemy import create_engine
            
            # 构建连接URL
            url = f"mysql+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}?charset={self.config['charset']}"
            engine = create_engine(url)
            
            return pd.read_sql(sql, engine, params=params)
            
        except Exception as e:
            logging.error(f"MySQL DataFrame查询错误: {e}, SQL: {sql}")
            return pd.DataFrame()
    
    def execute(self, sql: str, *params) -> bool:
        """执行SQL语句"""
        try:
            if not self._connection.open:
                self._connection = pymysql.connect(**{
                    'host': self.config['host'],
                    'port': self.config['port'],
                    'user': self.config['user'],
                    'password': self.config['password'],
                    'database': self.config['database'],
                    'charset': self.config['charset'],
                    'autocommit': True
                })
            
            with self._connection.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
            return True
            
        except Exception as e:
            logging.error(f"MySQL执行错误: {e}, SQL: {sql}")
            return False
    
    def close(self):
        """关闭连接"""
        if self._torndb_connection:
            self._torndb_connection.close()
        if self._connection and self._connection.open:
            self._connection.close()
    
    def insert_from_dataframe(self, df: pd.DataFrame, table_name: str, cols_type=None, write_index=False, primary_keys=None):
        """从DataFrame插入数据到MySQL"""
        try:
            # 构建SQLAlchemy引擎
            url = f"mysql+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}?charset={self.config['charset']}"
            engine = create_engine(url)
            
            col_name_list = df.columns.tolist()
            if write_index:
                col_name_list.insert(0, df.index.name)
            
            if cols_type is None:
                df.to_sql(name=table_name, con=engine, if_exists='append', index=write_index)
            elif not cols_type:
                df.to_sql(name=table_name, con=engine, if_exists='append',
                         dtype={col_name: NVARCHAR(255) for col_name in col_name_list}, index=write_index)
            else:
                df.to_sql(name=table_name, con=engine, if_exists='append',
                         dtype=cols_type, index=write_index)
            
            logging.info(f"成功插入 {len(df)} 条记录到 MySQL 表 {table_name}")
            return True
            
        except Exception as e:
            logging.error(f"MySQL DataFrame插入错误: {e}, 表: {table_name}")
            return False

# ClickHouse实现
class ClickHouseDatabase(DatabaseInterface):
    """ClickHouse数据库实现"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or db_config.clickhouse_config
        self._client = None
        self._init_connection()
    
    def _init_connection(self):
        """初始化ClickHouse连接"""
        try:
            from instock.lib.clickhouse_client import ClickHouseClient
            self._client = ClickHouseClient(self.config)
            logging.info(f"ClickHouse连接初始化成功: {self.config['host']}:{self.config['port']}/{self.config['database']}")
            
        except Exception as e:
            logging.error(f"ClickHouse连接初始化失败: {e}")
            raise
    
    def _execute_chdb_query(self, sql: str, params: Optional[Union[Tuple, Dict]] = None, return_type: str = "DataFrame") -> Union[pd.DataFrame, List[Dict]]:
        """
        使用chdb执行查询的通用方法
        
        Args:
            sql: SQL查询语句
            params: 查询参数
            return_type: 返回类型，"DataFrame" 或 "records"
            
        Returns:
            根据return_type返回DataFrame或字典列表
        """
        try:
            import chdb
            
            # 构建远程查询SQL，如果SQL中没有remote函数则包装一下
            if 'remote(' not in sql.lower():
                sql = self._wrap_sql_with_remote(sql)
            
            # 处理参数替换
            if params:
                if isinstance(params, tuple):
                    # 处理位置参数
                    for param in params:
                        if isinstance(param, str):
                            sql = sql.replace('%s', f"'{param}'", 1)
                        else:
                            sql = sql.replace('%s', str(param), 1)
                elif isinstance(params, dict):
                    # 处理命名参数
                    for key, value in params.items():
                        placeholder = f'%({key})s'
                        if isinstance(value, str):
                            sql = sql.replace(placeholder, f"'{value}'")
                        else:
                            sql = sql.replace(placeholder, str(value))
            
            # 执行查询
            result_df = chdb.query(sql, "DataFrame")
            # 如果存在date列且为uint16类型，转换为日期格式
            if 'date' in result_df.columns and pd.api.types.is_integer_dtype(result_df['date']):
                try:
                    result_df["date"] = pd.to_datetime(
                        result_df["date"],  # 待转换的 uint16 列
                        origin="1970-01-01",  # 基准日期（ClickHouse Date 类型的起始点）
                        unit="D"  # 单位：天数（对应 ClickHouse 的存储逻辑）
                    ).dt.date  # 可选：若只需日期（无时间），加 .dt.date 转为 Python date 类型
                except Exception as e:
                    logging.warning(f"日期转换失败: {e}")
            if return_type == "DataFrame":
                return result_df if not result_df.empty else pd.DataFrame()
            elif return_type == "records":
                if result_df.empty:
                    return []
                # 使用通用的DataFrame到记录转换方法
                return self._convert_dataframe_to_records(result_df)
            else:
                raise ValueError(f"不支持的返回类型: {return_type}")
                
        except Exception as e:
            logging.error(f"ClickHouse查询错误: {e}, SQL: {sql}")
            if return_type == "DataFrame":
                return pd.DataFrame()
            else:
                raise

    def query(self, sql: str, *params) -> List[Dict]:
        """执行查询并返回结果"""
        return self._execute_chdb_query(sql, params, "records")
    
    def query_to_dataframe(self, sql: str, params: Optional[Union[Tuple, Dict]] = None) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        return self._execute_chdb_query(sql, params, "DataFrame")
    
    def _wrap_sql_with_remote(self, sql: str) -> str:
        """将普通SQL包装为remote查询格式"""
        try:
            # 提取表名（简单匹配，假设SQL格式标准）
            import re
            
            # 匹配 FROM table_name 或 FROM `table_name`
            from_match = re.search(r'FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
            if not from_match:
                # 如果找不到FROM，直接返回原SQL
                return sql
            
            table_name = from_match.group(1)
            
            # 构建remote函数调用
            remote_call = f"remote('{self.config['host']}:{self.config.get('tcp', 9000)}', '{self.config['database']}', '{table_name}', '{self.config['username']}', '{self.config['password']}')"
            
            # 替换原表名为remote调用
            wrapped_sql = re.sub(r'FROM\s+`?' + table_name + r'`?', f'FROM {remote_call}', sql, flags=re.IGNORECASE)
            
            return wrapped_sql
            
        except Exception as e:
            logging.warning(f"包装SQL为remote格式失败: {e}，使用原SQL")
            return sql
    
    def execute(self, sql: str, *params) -> bool:
        """执行SQL语句"""
        try:
            # 对于INSERT、CREATE等操作，仍然使用原来的客户端
            if any(keyword in sql.upper() for keyword in ['INSERT', 'CREATE', 'ALTER', 'DROP', 'UPDATE', 'DELETE']):
                # 使用原来的客户端执行写操作
                parameters = {}
                if params:
                    for i, param in enumerate(params):
                        parameters[f'param_{i}'] = param
                    # 替换SQL中的%s为命名参数
                    for i in range(len(params)):
                        sql = sql.replace('%s', f'%(param_{i})s', 1)
                
                result = self._client.execute_query(sql, parameters)
                return result is not None
            else:
                # 对于SELECT等查询操作，使用chdb
                import chdb
                
                # 构建远程查询SQL
                if 'remote(' not in sql.lower():
                    sql = self._wrap_sql_with_remote(sql)
                
                # 处理参数替换
                if params:
                    for param in params:
                        if isinstance(param, str):
                            sql = sql.replace('%s', f"'{param}'", 1)
                        else:
                            sql = sql.replace('%s', str(param), 1)
                
                # 执行查询
                result = chdb.query(sql, "DataFrame")
                return True  # chdb查询成功即返回True
            
        except Exception as e:
            logging.error(f"ClickHouse执行错误: {e}, SQL: {sql}")
            return False
    
    def close(self):
        """关闭连接"""
        if self._client:
            self._client.close()
    
    def insert_from_dataframe(self, df: pd.DataFrame, table_name: str, cols_type=None, write_index=False, primary_keys=None):
        """从DataFrame插入数据到ClickHouse"""
        try:
            if write_index:
                # ClickHouse通常不需要索引，但如果需要可以将索引作为一列
                df = df.reset_index()
            
            # 创建DataFrame副本以避免修改原始数据
            df_copy = df.copy()
            
            # 获取表定义
            table_definition = self._get_table_definition(table_name)
            
            # 检查表是否存在，如果不存在则创建
            # 对于ClickHouse，我们忽略传入的cols_type，自己推断类型并应用Nullable规则
            if not self._table_exists(table_name):
                self._create_table_from_dataframe(table_name, df_copy, None)  # 传递None忽略cols_type
            
            # 转换数据类型以适配ClickHouse，同样忽略cols_type
            df_copy = self._convert_dataframe_for_clickhouse(df_copy, None)  # 传递None忽略cols_type
            
            # 使用ClickHouse客户端的DataFrame插入方法，传递表定义
            result = self._client.insert_dataframe(table_name, df_copy, table_definition)
            
            logging.info(f"成功插入 {len(df_copy)} 条记录到 ClickHouse 表 {table_name}")
            return result
            
        except Exception as e:
            logging.error(f"ClickHouse DataFrame插入错误: {e}, 表: {table_name}")
            return False
    
    def _convert_dataframe_for_clickhouse(self, df: pd.DataFrame, cols_type=None):
        """转换DataFrame数据类型以适配ClickHouse，保持原始格式"""
        try:
            df_converted = df.copy()
            
            # 遍历所有列，进行必要的类型转换
            for col in df_converted.columns:
                col_data = df_converted[col]
                
                # 处理日期相关的列
                if 'date' in col.lower() and col_data.dtype == 'object':
                    try:
                        # 尝试转换日期字符串为pandas datetime，然后转为date
                        df_converted[col] = pd.to_datetime(col_data, errors='coerce').dt.date
                    except Exception as e:
                        logging.warning(f"日期转换失败 {col}: {e}")
                        # 保持原格式
                        pass
                
                # 处理datetime类型
                elif col_data.dtype.name.startswith('datetime'):
                    try:
                        # 保持datetime格式，ClickHouse可以处理
                        df_converted[col] = pd.to_datetime(col_data)
                    except:
                        pass
                
                # 处理数值类型 - 保持pandas的原始推断
                elif pd.api.types.is_numeric_dtype(col_data):
                    # 如果已经是数值类型，保持不变
                    pass
                
                # 处理字符串类型
                elif col_data.dtype == 'object':
                    # 对于object类型，尝试转换为数值，如果失败则保持字符串
                    try:
                        # 检查是否所有值都能转换为数值
                        numeric_col = pd.to_numeric(col_data, errors='coerce')
                        if not numeric_col.isna().all():
                            df_converted[col] = numeric_col
                    except:
                        # 保持为字符串
                        pass
            df_converted['code'] = df_converted['code'].astype(str).str.zfill(6)
            
            return df_converted
            
        except Exception as e:
            logging.error(f"DataFrame类型转换失败: {e}")
            # 如果转换失败，返回原始DataFrame
            return df
    
    def _table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            result = self.query(f"EXISTS TABLE {table_name}")
            return result and len(result) > 0 and result[0].get('result', 0) == 1
        except:
            return False
    
    def _create_table_from_dataframe(self, table_name: str, df: pd.DataFrame, cols_type=None):
        """根据DataFrame和列类型创建ClickHouse表"""
        try:
            # 构建列定义
            column_defs = []
            
            for col in df.columns:
                if cols_type and col in cols_type:
                    # 使用指定的类型，需要转换为ClickHouse类型
                    clickhouse_type = self._convert_sqlalchemy_to_clickhouse_type(cols_type[col])
                else:
                    # 根据DataFrame数据类型推断ClickHouse类型
                    clickhouse_type = self._infer_clickhouse_type(df[col])
                
                # 判断是否允许NULL值：除了date和code，其他字段都允许NULL
                # 无论是从cols_type还是推断得到的类型，都要应用这个规则
                if col.lower() in ['date', 'code']:
                    # 关键字段不允许NULL
                    column_defs.append(f"`{col}` {clickhouse_type}")
                else:
                    # 其他字段允许NULL
                    column_defs.append(f"`{col}` Nullable({clickhouse_type})")
            
            # 确定主键，用于ORDER BY
            if 'date' in df.columns and 'code' in df.columns:
                order_by = "ORDER BY (date, code)"
            elif 'date' in df.columns:
                order_by = "ORDER BY date"
            elif 'code' in df.columns:
                order_by = "ORDER BY code"
            else:
                # 使用第一列作为排序键
                first_col = df.columns[0]
                order_by = f"ORDER BY {first_col}"
            
            # 创建表SQL
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(column_defs)}
                ) ENGINE = MergeTree()
                {order_by}
                SETTINGS index_granularity = 8192
            """
            print(create_sql)
            logging.info(f"准备创建ClickHouse表 {table_name}，SQL: {create_sql}")
            
            success = self.execute(create_sql)
            if success:
                logging.info(f"ClickHouse表 {table_name} 创建成功")
            else:
                logging.error(f"ClickHouse表 {table_name} 创建失败")
                
        except Exception as e:
            logging.error(f"创建ClickHouse表 {table_name} 异常: {e}")
    
    def _convert_sqlalchemy_to_clickhouse_type(self, sqlalchemy_type):
        """将SQLAlchemy类型转换为ClickHouse类型"""
        type_str = str(sqlalchemy_type).upper()
        
        if 'VARCHAR' in type_str or 'TEXT' in type_str:
            return 'String'
        elif 'FLOAT' in type_str or 'DOUBLE' in type_str:
            return 'Float64'
        elif 'BIGINT' in type_str:
            return 'Int64'  # 使用有符号整数支持负数
        elif 'INT' in type_str:
            return 'Int32'  # 使用有符号整数支持负数
        elif 'DATE' in type_str:
            return 'Date'
        elif 'DATETIME' in type_str or 'TIMESTAMP' in type_str:
            return 'DateTime'
        elif 'BIT' in type_str:
            return 'UInt8'
        else:
            return 'String'  # 默认使用String类型
    
    def _infer_clickhouse_type(self, series):
        """根据pandas Series推断ClickHouse类型，优先保持原始格式兼容性"""
        dtype = str(series.dtype)
        series_name = series.name.lower() if series.name else ''
        
        # 首先检查列名中的日期指示器
        if 'date' in series_name and dtype == 'object':
            return 'Date'
        elif 'time' in series_name or 'datetime' in series_name:
            return 'DateTime'
        
        # 根据pandas数据类型推断
        if dtype == 'object':
            # 对于object类型，尝试判断是字符串还是数值
            try:
                # 尝试转换为数值
                numeric_series = pd.to_numeric(series.dropna(), errors='coerce')
                if not numeric_series.isna().all():
                    # 可以转换为数值，检查是否有小数
                    if (numeric_series % 1 == 0).all():
                        # 都是整数，检查是否有负数
                        if (numeric_series < 0).any():
                            return 'Int64'
                        else:
                            return 'Int64'  # 统一使用Int64避免负数问题
                    else:
                        return 'Float64'
                else:
                    return 'String'
            except:
                return 'String'
        
        elif dtype.startswith('int'):
            return 'Int64'  # 统一使用有符号整数
        elif dtype.startswith('float'):
            return 'Float64'
        elif dtype == 'bool':
            return 'UInt8'
        elif dtype.startswith('datetime'):
            return 'DateTime'
        else:
            return 'String'  # 默认字符串类型
    
    def _get_table_definition(self, table_name: str):
        """从tablestructure获取表定义"""
        try:
            import instock.core.tablestructure as tbs
            
            # 映射表名到tablestructure中的定义
            table_mapping = {
                'cn_stock_spot': tbs.TABLE_CN_STOCK_SPOT,
                'cn_etf_spot': tbs.TABLE_CN_ETF_SPOT,
                'cn_stock_fund_flow': tbs.TABLE_CN_STOCK_FUND_FLOW,
                'cn_stock_fund_flow_industry': tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY,
                'cn_stock_fund_flow_concept': tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT,
                'cn_stock_bonus': tbs.TABLE_CN_STOCK_BONUS,
                'cn_stock_top': tbs.TABLE_CN_STOCK_TOP,
                'cn_stock_blocktrade': tbs.TABLE_CN_STOCK_BLOCKTRADE,
                'cn_stock_foreign_key': tbs.TABLE_CN_STOCK_FOREIGN_KEY,
                'cn_stock_backtest_data': tbs.TABLE_CN_STOCK_BACKTEST_DATA,
                'cn_stock_indicators': tbs.TABLE_CN_STOCK_INDICATORS,
                'cn_stock_indicators_buy': tbs.TABLE_CN_STOCK_INDICATORS_BUY,
                'cn_stock_indicators_sell': tbs.TABLE_CN_STOCK_INDICATORS_SELL,
                'cn_stock_pattern': tbs.TABLE_CN_STOCK_KLINE_PATTERN,
                'cn_stock_selection': tbs.TABLE_CN_STOCK_SELECTION,
                'cn_stock_attention': tbs.TABLE_CN_STOCK_ATTENTION,
                'cn_stock_spot_buy': tbs.TABLE_CN_STOCK_SPOT_BUY,
                'cn_stock_chip_race_open': tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN,
                'cn_stock_chip_race_end': tbs.TABLE_CN_STOCK_CHIP_RACE_END,
                'cn_stock_limitup_reason': tbs.TABLE_CN_STOCK_LIMITUP_REASON
            }
            
            # 动态添加策略表定义
            for strategy_table in tbs.TABLE_CN_STOCK_STRATEGIES:
                table_mapping[strategy_table['name']] = strategy_table
            
            return table_mapping.get(table_name)
            
        except Exception as e:
            logging.warning(f"无法获取表 {table_name} 的定义: {e}")
            return None

# 数据库工厂类
class DatabaseFactory:
    """数据库工厂，根据配置创建相应的数据库实例"""
    
    @staticmethod
    def create_database(db_type: Optional[Union[str, DatabaseType]] = None) -> DatabaseInterface:
        """创建数据库实例"""
        if db_type is None:
            db_type = db_config.db_type
        elif isinstance(db_type, str):
            db_type = DatabaseType(db_type.lower())
        
        if db_type == DatabaseType.MYSQL:
            return MySQLDatabase()
        elif db_type == DatabaseType.CLICKHOUSE:
            return ClickHouseDatabase()
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")

# 全局数据库实例（单例模式）
_database_instance = None

def get_database() -> DatabaseInterface:
    """获取数据库实例（单例）"""
    global _database_instance
    
    if _database_instance is None:
        _database_instance = DatabaseFactory.create_database()
    
    return _database_instance

def switch_database_type(db_type: Union[str, DatabaseType]):
    """切换数据库类型（重新创建实例）"""
    global _database_instance
    
    # 关闭现有连接
    if _database_instance:
        _database_instance.close()
    
    # 更新配置
    db_config.switch_database(db_type)
    
    # 重新创建实例
    _database_instance = DatabaseFactory.create_database()
    
    logging.info(f"数据库实例已切换至: {db_type}")

# 兼容性函数（保持向后兼容）
def get_connection():
    """获取数据库连接（兼容性函数）"""
    db = get_database()
    if isinstance(db, MySQLDatabase):
        return db._connection
    else:
        return db._client

# 便利函数
def execute_sql(sql: str, params: Tuple = ()) -> bool:
    """执行SQL语句"""
    return get_database().execute(sql, *params)


def execute_sql_fetch(sql: str, params: Tuple = ()) -> List[Dict]:
    """执行查询并返回结果"""
    return get_database().query(sql, *params)

def execute_sql_count(sql: str, params: Tuple = ()) -> int:
    """执行查询并返回计数"""
    result = get_database().query(sql, *params)
    if result and len(result) == 1:
        return int(list(result[0].values())[0])
    return 0

def read_sql_to_df(sql: str, params: Optional[Union[Tuple, Dict]] = None) -> pd.DataFrame:
    """执行查询并返回DataFrame"""
    return get_database().query_to_dataframe(sql, params)

def insert_db_from_df(data: pd.DataFrame, table_name: str, cols_type=None, write_index=False, primary_keys=None, indexs=None):
    """从DataFrame插入数据（兼容性函数）"""
    return get_database().insert_from_dataframe(data, table_name, cols_type, write_index, primary_keys)
