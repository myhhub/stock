#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
from enum import Enum
from typing import Optional, Dict, Any, Union, List, Tuple
import pandas as pd

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
    
    def close(self):
        """关闭连接"""
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
            # 检查连接
            self._torndb_connection.query("SELECT 1")
        except:
            self._torndb_connection.reconnect()
        
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
    
    def query(self, sql: str, *params) -> List[Dict]:
        """执行查询并返回结果"""
        try:
            # 将位置参数转换为命名参数
            parameters = {}
            if params:
                for i, param in enumerate(params):
                    parameters[f'param_{i}'] = param
                # 替换SQL中的%s为命名参数
                for i in range(len(params)):
                    sql = sql.replace('%s', f'%(param_{i})s', 1)
            
            result = self._client.execute_query(sql, parameters)
            if result:
                # 转换为字典列表格式
                columns = result.column_names
                rows = result.result_rows
                return [dict(zip(columns, row)) for row in rows]
            return []
            
        except Exception as e:
            logging.error(f"ClickHouse查询错误: {e}, SQL: {sql}")
            raise
    
    def query_to_dataframe(self, sql: str, params: Optional[Union[Tuple, Dict]] = None) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        try:
            # 转换参数格式
            if isinstance(params, tuple):
                parameters = {}
                for i, param in enumerate(params):
                    parameters[f'param_{i}'] = param
                # 替换SQL中的%s为命名参数
                for i in range(len(params)):
                    sql = sql.replace('%s', f'%(param_{i})s', 1)
            else:
                parameters = params
            
            return self._client.query_to_dataframe(sql, parameters) or pd.DataFrame()
            
        except Exception as e:
            logging.error(f"ClickHouse DataFrame查询错误: {e}, SQL: {sql}")
            return pd.DataFrame()
    
    def execute(self, sql: str, *params) -> bool:
        """执行SQL语句"""
        try:
            # ClickHouse主要用于查询，大部分INSERT通过专门的方法处理
            parameters = {}
            if params:
                for i, param in enumerate(params):
                    parameters[f'param_{i}'] = param
                # 替换SQL中的%s为命名参数
                for i in range(len(params)):
                    sql = sql.replace('%s', f'%(param_{i})s', 1)
            
            result = self._client.execute_query(sql, parameters)
            return result is not None
            
        except Exception as e:
            logging.error(f"ClickHouse执行错误: {e}, SQL: {sql}")
            return False
    
    def close(self):
        """关闭连接"""
        if self._client:
            self._client.close()

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
