#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import traceback
import pandas as pd
import numpy as np
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
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame, table_definition: dict = None) -> bool:
        """
        将DataFrame插入到ClickHouse表中
        
        Args:
            table_name: 目标表名
            df: 要插入的DataFrame
            table_definition: 表结构定义（来自tablestructure）
            
        Returns:
            bool: 插入是否成功
        """
        if df.empty:
            logger.warning(f"DataFrame为空，跳过插入到表 {table_name}")
            return True
            
        try:
            # 确保DataFrame类型兼容ClickHouse，传递表定义
            df_clean = self._prepare_dataframe_for_insert(df, table_definition)
            
            # 添加调试信息
            logger.info(f"准备插入数据到表 {table_name}")
            logger.info(f"DataFrame形状: {df.shape}")
            logger.info(f"DataFrame列: {df.columns.tolist()}")
            
            # 直接使用df_clean插入
            result = self.client.insert_df(table_name, df_clean)
            logger.info(f"成功插入 {len(df_clean)} 行数据到表 {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"插入DataFrame到ClickHouse失败: {str(e)}")
            logger.error(f"表名: {table_name}, DataFrame形状: {df.shape}")
            logger.error(f"DataFrame列: {list(df.columns)}")
            logger.error(f"DataFrame类型: {df.dtypes.to_dict()}")
            logger.error(f"DataFrame前几行:\n{df.head()}")
            logger.error(f"详细错误: {traceback.format_exc()}")
            return False
    
    def _prepare_dataframe_for_insert(self, df: pd.DataFrame, table_definition: dict = None) -> pd.DataFrame:
        """准备DataFrame以便插入ClickHouse"""        
        # 创建DataFrame副本
        df_clean = df.copy()
        
        # 处理每一列，确保数据类型兼容ClickHouse
        for col in df_clean.columns:
            series = df_clean[col]
            logger.info(f"处理列 {col}, 数据类型: {series.dtype}")
            
            # 根据列名和数据类型进行转换
            if col.lower() == 'date' or (col.lower().endswith('_date') and series.dtype == 'object'):
                try:
                    # 检查表定义中该字段的类型
                    field_type = self._get_field_type_from_definition(col, table_definition)
                    
                    # 如果是DATE字段，或者字段名是'date'且数据是字符串，转换为datetime字符串
                    if (field_type and ('Date' in str(field_type) or 'DATE' in str(field_type))) or (col.lower() == 'date' and series.dtype == 'object'):
                        logger.info(f"日期列 {col} 转换为datetime字符串格式")
                        df_clean[col] = self._convert_series_to_datetime(series)
                    else:
                        # 对于其他包含date的字段，如果原本是字符串，保持字符串
                        logger.info(f"日期相关列 {col} 保持原始格式")
                        df_clean[col] = series.where(pd.notna(series) & (series != ''), None)
                        
                except Exception as e:
                    logger.error(f"日期转换失败 {col}: {e}")
                    df_clean[col] = [None] * len(series)
                    
            elif series.dtype.name.startswith('int') or series.dtype.name.startswith('uint'):
                df_clean[col] = series.where(pd.notna(series), None).astype('Int64')
                
            elif series.dtype.name.startswith('float'):
                df_clean[col] = series.where(pd.notna(series), None)
                
            elif series.dtype == 'bool':
                df_clean[col] = series.astype('Int64')
                
            elif series.dtype == 'object':
                df_clean[col] = series.where(pd.notna(series) & (series != ''), None)
                
            else:
                df_clean[col] = series.astype(str).where(pd.notna(series), None)
        
        return df_clean
    
    def _get_field_type_from_definition(self, col: str, table_definition: dict = None):
        """从表定义中获取字段类型"""
        if not table_definition:
            return None
            
        columns = table_definition.get('columns', {})
        if isinstance(columns, dict) and col in columns:
            return columns[col].get('type')
        
        return None
    
    def _is_string_field(self, col: str, table_definition: dict = None) -> bool:
        """检查字段是否为String类型"""
        if not table_definition:
            return False
            
        columns = table_definition.get('columns', [])
        for column_def in columns:
            if isinstance(column_def, dict) and column_def.get('name') == col:
                field_type = column_def.get('type', '')
                return 'String' in field_type
        return False
    
    def _is_null_value(self, val) -> bool:
        """检查值是否为空值"""
        return (pd.isna(val) or 
                val in ['NaT', 'nat', 'None', '', 'nan', 'NaN'] or
                (isinstance(val, str) and val.strip() == ''))
    
    def _is_valid_date_range(self, dt) -> bool:
        """检查日期是否在合理范围内，避免timestamp()错误"""
        try:
            if pd.isna(dt):
                return False
            
            # 检查年份范围 (1900-2100年)
            year = dt.year if hasattr(dt, 'year') else dt.dt.year
            if year < 1900 or year > 2100:
                return False
            
            # 尝试调用timestamp()方法检查是否会抛出异常
            _ = dt.timestamp()
            return True
        except (ValueError, OSError, OverflowError) as e:
            # 捕获"Out of bounds nanosecond timestamp"等错误
            return False
        except Exception:
            return False
    
    def _convert_single_value_to_date_string(self, val) -> str:
        """将单个值转换为日期字符串"""
        if self._is_null_value(val):
            return None
            
        # datetime对象直接格式化
        if hasattr(val, 'strftime'):
            return val.strftime('%Y-%m-%d')
            
        # numpy datetime64 和 pandas Timestamp
        if isinstance(val, (np.datetime64, pd.Timestamp)):
            try:
                ts = pd.to_datetime(val)
                return None if pd.isna(ts) else ts.strftime('%Y-%m-%d')
            except:
                return None
                
        # 整数类型处理
        if isinstance(val, (int, np.integer)):
            if 1900 <= val <= 2100:  # 年份
                return str(val)
            elif val > 1e9:  # 秒级时间戳
                try:
                    return pd.to_datetime(val, unit='s').strftime('%Y-%m-%d')
                except:
                    return None
            elif val > 100000:  # 天数序列号
                try:
                    dt = pd.to_datetime('1900-01-01') + pd.Timedelta(days=val)
                    return dt.strftime('%Y-%m-%d')
                except:
                    return None
            return None
            
        # 浮点数类型处理
        if isinstance(val, (float, np.floating)):
            if np.isnan(val):
                return None
            try:
                if val > 1e9:  # 秒级时间戳
                    return pd.to_datetime(val, unit='s').strftime('%Y-%m-%d')
                elif val > 100000:  # 天数序列号
                    dt = pd.to_datetime('1900-01-01') + pd.Timedelta(days=val)
                    return dt.strftime('%Y-%m-%d')
                return None
            except:
                return None
                
        # 字符串或其他类型
        try:
            str_val = str(val).strip()
            if str_val in ['', 'NaT', 'None', 'nan', 'NaN']:
                return None
            # 尝试解析为日期
            dt = pd.to_datetime(str_val)
            return None if pd.isna(dt) else dt.strftime('%Y-%m-%d')
        except:
            try:
                # 如果无法解析为日期，保持原字符串
                return str_val if str_val else None
            except:
                return None
    
    def _convert_series_to_date_string(self, series: pd.Series) -> list:
        """将Series转换为日期字符串列表"""
        return [self._convert_single_value_to_date_string(val) for val in series]
    
    def _convert_series_to_datetime(self, series: pd.Series) -> list:
        """将Series转换为datetime.date对象列表，适配ClickHouse Date类型"""
        result = []
        for val in series:
            if self._is_null_value(val):
                result.append(None)
                continue
                
            try:
                # 尝试直接转换
                if isinstance(val, str):
                    # 处理常见的日期字符串格式
                    if val in ['', 'NaT', 'nat', 'None', 'null', 'NULL']:
                        result.append(None)
                        continue
                    
                    # 尝试解析日期字符串，返回 datetime 对象
                    dt = pd.to_datetime(val, errors='coerce')
                    if pd.isna(dt):
                        result.append(None)
                    else:
                        # 检查日期范围是否合理 (1900-2100年)
                        if self._is_valid_date_range(dt):
                            result.append(dt)  # 返回 datetime 对象，支持 timestamp()
                        else:
                            logger.warning(f"日期超出有效范围，跳过: {dt}")
                            result.append(None)
                elif hasattr(val, 'date'):
                    # 已经是datetime对象，检查范围后直接使用
                    if self._is_valid_date_range(val):
                        result.append(val)
                    else:
                        logger.warning(f"日期超出有效范围，跳过: {val}")
                        result.append(None)
                elif isinstance(val, (int, float)):
                    # 数值类型，可能是时间戳
                    if val > 1e9:  # 秒级时间戳
                        dt = pd.to_datetime(val, unit='s', errors='coerce')
                    else:
                        dt = pd.to_datetime(val, errors='coerce')
                    
                    if pd.isna(dt):
                        result.append(None)
                    elif self._is_valid_date_range(dt):
                        result.append(dt)
                    else:
                        logger.warning(f"日期超出有效范围，跳过: {dt}")
                        result.append(None)
                else:
                    # 其他类型尝试直接转换
                    dt = pd.to_datetime(val, errors='coerce')
                    if pd.isna(dt):
                        result.append(None)
                    elif self._is_valid_date_range(dt):
                        result.append(dt)
                    else:
                        logger.warning(f"日期超出有效范围，跳过: {dt}")
                        result.append(None)
            except Exception as e:
                logger.warning(f"日期转换失败: {val} -> {e}")
                result.append(None)
                
        return result
    
    def batch_insert_dataframe(self, table_name: str, df: pd.DataFrame, batch_size: int = 10000) -> bool:
        """
        批量插入DataFrame到ClickHouse表中
        
        Args:
            table_name: 目标表名
            df: 要插入的DataFrame
            batch_size: 每批插入的记录数
            
        Returns:
            bool: 插入是否成功
        """
        if df.empty:
            logger.warning(f"DataFrame为空，跳过插入到表 {table_name}")
            return True
            
        try:
            total_rows = len(df)
            successful_batches = 0
            
            # 分批插入
            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]
                
                if self.insert_dataframe(table_name, batch_df):
                    successful_batches += 1
                    logger.info(f"批次 {successful_batches}: 插入 {len(batch_df)} 条记录")
                else:
                    logger.error(f"批次插入失败: 第 {i} 到 {i + len(batch_df)} 行")
                    return False
            
            logger.info(f"批量插入完成: 总计 {total_rows} 条记录，{successful_batches} 个批次")
            return True
            
        except Exception as e:
            logger.error(f"批量插入DataFrame失败: {str(e)}")
            return False
    
    def get_stock_data(self, 
                      code: Optional[str] = None,
                      start_date: Optional[Union[str, date, datetime]] = None,
                      end_date: Optional[Union[str, date, datetime]] = None,
                      columns: Optional[List[str]] = None,
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
        sql = f"SELECT {','.join(columns) if columns else '*'} FROM cn_stock_history WHERE 1=1"
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
        print(sql)
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
