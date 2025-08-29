#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, date
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ClickHouse配置
CLICKHOUSE_CONFIG = {
    'host': os.environ.get('CLICKHOUSE_HOST', '192.168.1.6'),
    'port': int(os.environ.get('CLICKHOUSE_PORT', '8123')),
    'username': os.environ.get('CLICKHOUSE_USER', 'root'),
    'password': os.environ.get('CLICKHOUSE_PASSWORD', '123456'),
    'database': os.environ.get('CLICKHOUSE_DB', 'instockdb')
}


class ClickHouseClient:
    """
    ClickHouse数据库客户端，提供数据查询和pandas转换功能
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化ClickHouse客户端
        
        Args:
            config: ClickHouse配置字典，如果为None则使用默认配置
        """
        self.config = config or CLICKHOUSE_CONFIG
        self.client = None
        self._connect()
    
    def _connect(self) -> bool:
        """建立ClickHouse连接"""
        try:
            import clickhouse_connect
            
            self.client = clickhouse_connect.get_client(
                host=self.config['host'],
                port=self.config['port'],
                username=self.config['username'],
                password=self.config['password'],
                database=self.config['database']
            )
            
            # 测试连接
            result = self.client.query("SELECT 1")
            logger.info(f"成功连接到ClickHouse: {self.config['host']}:{self.config['port']}/{self.config['database']}")
            return True
            
        except ImportError:
            logger.error("需要安装clickhouse-connect: pip install clickhouse-connect")
            return False
        except Exception as e:
            logger.error(f"连接ClickHouse失败: {str(e)}")
            return False
    
    def execute_query(self, sql: str, parameters: Optional[Dict] = None) -> Optional[Any]:
        """
        执行SQL查询
        
        Args:
            sql: SQL查询语句
            parameters: 查询参数
            
        Returns:
            查询结果
        """
        if not self.client:
            logger.error("ClickHouse客户端未连接")
            return None
            
        try:
            if parameters:
                result = self.client.query(sql, parameters=parameters)
            else:
                result = self.client.query(sql)
            return result
        except Exception as e:
            logger.error(f"执行查询失败: {str(e)}")
            logger.error(f"SQL: {sql}")
            return None
    
    def query_to_dataframe(self, sql: str, parameters: Optional[Dict] = None) -> Optional[pd.DataFrame]:
        """
        执行查询并返回pandas DataFrame
        
        Args:
            sql: SQL查询语句
            parameters: 查询参数
            
        Returns:
            pandas DataFrame或None
        """
        result = self.execute_query(sql, parameters)
        if result is None:
            return None
            
        try:
            # 获取列名和数据
            columns = result.column_names
            data = result.result_rows
            
            # 创建DataFrame
            df = pd.DataFrame(data, columns=columns)
            logger.info(f"查询成功，返回 {len(df)} 行数据")
            return df
            
        except Exception as e:
            logger.error(f"转换为DataFrame失败: {str(e)}")
            return None
    
    def get_stock_data(self, 
                      code: Optional[str] = None,
                      start_date: Optional[Union[str, date, datetime]] = None,
                      end_date: Optional[Union[str, date, datetime]] = None,
                      limit: Optional[int] = None,
                      order_by: str = "date DESC") -> Optional[pd.DataFrame]:
        """
        获取股票历史数据
        
        Args:
            code: 股票代码，如果为None则查询所有股票
            start_date: 开始日期
            end_date: 结束日期
            limit: 限制返回行数
            order_by: 排序方式，默认按日期降序
            
        Returns:
            包含股票数据的DataFrame
        """
        # 构建基础SQL
        sql = "SELECT * FROM cn_stock_history WHERE 1=1"
        parameters = {}
        
        # 添加股票代码条件
        if code:
            sql += " AND code = %(code)s"
            parameters['code'] = code
        
        # 添加日期范围条件
        if start_date:
            sql += " AND date >= %(start_date)s"
            parameters['start_date'] = self._format_date(start_date)
        
        if end_date:
            sql += " AND date <= %(end_date)s"
            parameters['end_date'] = self._format_date(end_date)
        
        # 添加排序
        sql += f" ORDER BY {order_by}"
        
        # 添加限制
        if limit:
            sql += f" LIMIT {limit}"
        
        logger.info(f"查询股票数据: code={code}, 日期范围={start_date}~{end_date}")
        return self.query_to_dataframe(sql, parameters)
    
    def get_stock_list(self, market: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        获取股票列表
        
        Args:
            market: 市场类型（如'sh', 'sz'），如果为None则查询所有市场
            
        Returns:
            包含股票基础信息的DataFrame
        """
        sql = "SELECT * FROM cn_stock_basic_info"
        parameters = {}
        
        if market:
            sql += " WHERE market = %(market)s"
            parameters['market'] = market
        
        sql += " ORDER BY code"
        
        return self.query_to_dataframe(sql, parameters)
    
    def get_market_stats(self, 
                        start_date: Optional[Union[str, date, datetime]] = None,
                        end_date: Optional[Union[str, date, datetime]] = None) -> Optional[pd.DataFrame]:
        """
        获取市场统计数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            包含市场统计的DataFrame
        """
        sql = "SELECT * FROM cn_market_daily_stats WHERE 1=1"
        parameters = {}
        
        if start_date:
            sql += " AND date >= %(start_date)s"
            parameters['start_date'] = self._format_date(start_date)
        
        if end_date:
            sql += " AND date <= %(end_date)s"
            parameters['end_date'] = self._format_date(end_date)
        
        sql += " ORDER BY date DESC, market"
        
        return self.query_to_dataframe(sql, parameters)
    
    def get_stock_latest_data(self, codes: Optional[List[str]] = None, limit: int = 20) -> Optional[pd.DataFrame]:
        """
        获取股票最新数据
        
        Args:
            codes: 股票代码列表，如果为None则查询所有股票
            limit: 每只股票返回的最新数据条数
            
        Returns:
            包含最新股票数据的DataFrame
        """
        if codes:
            code_list = "','".join(codes)
            where_clause = f"code IN ('{code_list}')"
        else:
            where_clause = "1=1"
        
        sql = f"""
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) as rn
            FROM cn_stock_history
            WHERE {where_clause}
        ) ranked
        WHERE rn <= {limit}
        ORDER BY code, date DESC
        """
        
        return self.query_to_dataframe(sql)
    
    def search_stocks(self, keyword: str) -> Optional[pd.DataFrame]:
        """
        搜索股票（按代码）
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            匹配的股票DataFrame
        """
        sql = """
        SELECT DISTINCT code, market, last_trading_date, total_trading_days
        FROM cn_stock_basic_info
        WHERE code LIKE %(keyword)s
        ORDER BY code
        LIMIT 100
        """
        
        parameters = {'keyword': f'%{keyword}%'}
        return self.query_to_dataframe(sql, parameters)
    
    def get_trading_calendar(self, 
                           start_date: Optional[Union[str, date, datetime]] = None,
                           end_date: Optional[Union[str, date, datetime]] = None) -> Optional[pd.DataFrame]:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日期DataFrame
        """
        sql = "SELECT DISTINCT date FROM cn_stock_history WHERE 1=1"
        parameters = {}
        
        if start_date:
            sql += " AND date >= %(start_date)s"
            parameters['start_date'] = self._format_date(start_date)
        
        if end_date:
            sql += " AND date <= %(end_date)s"
            parameters['end_date'] = self._format_date(end_date)
        
        sql += " ORDER BY date"
        
        return self.query_to_dataframe(sql, parameters)
    
    def _format_date(self, date_input: Union[str, date, datetime]) -> str:
        """格式化日期"""
        if isinstance(date_input, str):
            return date_input
        elif isinstance(date_input, (date, datetime)):
            return date_input.strftime('%Y-%m-%d')
        else:
            return str(date_input)
    
    def close(self):
        """关闭连接"""
        if self.client:
            self.client.close()
            logger.info("ClickHouse连接已关闭")
    
    def __enter__(self):
        """支持with语句"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持with语句"""
        self.close()


# 便利函数
def create_clickhouse_client(config: Optional[Dict[str, Any]] = None) -> ClickHouseClient:
    """
    创建ClickHouse客户端实例
    
    Args:
        config: 配置字典
        
    Returns:
        ClickHouseClient实例
    """
    return ClickHouseClient(config)


def quick_query(sql: str, parameters: Optional[Dict] = None, config: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
    """
    快速查询并返回DataFrame
    
    Args:
        sql: SQL查询语句
        parameters: 查询参数
        config: ClickHouse配置
        
    Returns:
        pandas DataFrame
    """
    with create_clickhouse_client(config) as client:
        return client.query_to_dataframe(sql, parameters)


def get_stock_data_quick(code: str, 
                        start_date: Optional[str] = None, 
                        end_date: Optional[str] = None,
                        limit: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    快速获取股票数据
    
    Args:
        code: 股票代码
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        limit: 限制返回行数
        
    Returns:
        股票数据DataFrame
    """
    with create_clickhouse_client() as client:
        return client.get_stock_data(code, start_date, end_date, limit)


if __name__ == "__main__":
    # 测试代码
    print("测试ClickHouse客户端...")
    
    # 基本连接测试
    with create_clickhouse_client() as client:
        # 测试查询
        result = client.query_to_dataframe("SELECT 1 as test")
        if result is not None:
            print("✅ 连接测试成功")
            
            # 测试获取股票列表
            stocks = client.get_stock_list()
            if stocks is not None and not stocks.empty:
                print(f"✅ 股票列表查询成功，共 {len(stocks)} 只股票")
                print(stocks.head())
            
            # 测试获取单只股票数据
            stock_data = client.get_stock_data('000001', limit=10)
            if stock_data is not None and not stock_data.empty:
                print(f"✅ 股票数据查询成功，平安银行(000001) 最近10条数据:")
                print(stock_data.head())
            # 测试获取单只股票数据的时间范围
            stock_data = client.get_stock_data('000001', start_date='2023-01-01', end_date='2023-12-31')
            if stock_data is not None and not stock_data.empty:
                print(f"✅ 股票数据查询成功，平安银行(000001) 2023年数据:")
                print(stock_data.head())
            
        else:
            print("❌ 连接测试失败")
