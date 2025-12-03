#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import datetime
import concurrent.futures
import instock.core.stockfetch as stf
import instock.core.tablestructure as tbs
import instock.lib.trade_time as trd
from instock.lib.singleton_type import singleton_type

# 获取logger实例
logger = logging.getLogger(__name__)

__author__ = 'myh '
__date__ = '2023/3/10 '


# 读取当天股票数据
class stock_data(metaclass=singleton_type):
    def __init__(self, date):
        logger.info(f"开始初始化stock_data单例，日期：{date}")
        try:
            logger.info(f"调用stf.fetch_stocks()获取股票数据，日期：{date}")
            self.data = stf.fetch_stocks(date)
            
            if self.data is not None and not self.data.empty:
                logger.info(f"成功获取股票数据，日期：{date}，共 {len(self.data)} 条记录")
            else:
                logger.warning(f"获取到的股票数据为空，日期：{date}")
                
        except Exception as e:
            logger.error(f"singleton.stock_data处理异常，日期：{date}：{e}")
            import traceback
            logger.error(f"异常堆栈信息: {traceback.format_exc()}")
            self.data = None

    def get_data(self):
        logger.info("调用get_data()方法获取股票数据")
        return self.data


# 读取股票历史数据
class stock_hist_data(metaclass=singleton_type):
    def __init__(self, date=None, stocks=None, workers=16):
        logger.info("开始初始化stock_hist_data单例")
        try:
            if stocks is None:
                logger.info("stocks参数为空，开始获取当天股票数据")
                
                # 获取当天股票数据
                stock_data_instance = stock_data(date)
                stock_data_result = stock_data_instance.get_data()
                
                if stock_data_result is None or stock_data_result.empty:
                    logger.warning("当天股票数据为空，尝试从数据库获取股票列表")
                    # 从数据库获取股票列表
                    try:
                        import instock.lib.database as mdb
                        import pandas as pd
                        
                        # 从股票表中获取最新的股票数据
                        sql = f"SELECT * FROM `{tbs.TABLE_CN_STOCK_SPOT['name']}` WHERE `date` = (SELECT MAX(`date`) FROM `{tbs.TABLE_CN_STOCK_SPOT['name']}`)"
                        logger.info(f"执行SQL查询：{sql}")
                        stock_data_result = pd.read_sql(sql=sql, con=mdb.engine())
                        
                        if stock_data_result is None or stock_data_result.empty:
                            logger.warning("从数据库获取的股票数据也为空，无法获取股票列表")
                            self.data = None
                            return
                            
                        logger.info(f"成功从数据库获取股票数据，共 {len(stock_data_result)} 条记录")
                    except Exception as e:
                        logger.error(f"从数据库获取股票数据失败：{e}")
                        import traceback
                        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
                        self.data = None
                        return
                    
                logger.info("开始提取股票列表")
                _subset = stock_data_result[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
                stocks = [tuple(x) for x in _subset.values]
                logger.info(f"成功提取股票列表，共 {len(stocks)} 只股票")
                
            if stocks is None:
                logger.warning("股票列表为空")
                self.data = None
                return
                
            logger.info(f"开始获取股票历史数据，共 {len(stocks)} 只股票")
            # 使用传入的date参数或从股票列表中提取日期
            if date is None and stocks:
                date = stocks[0][0]
            # 将datetime.date对象转换为字符串类型的日期
            if isinstance(date, datetime.date):
                date = date.strftime("%Y-%m-%d")
            date_start, is_cache = trd.get_trade_hist_interval(date)  # 提高运行效率，只运行一次
            logger.info(f"历史数据开始日期：{date_start}，是否使用缓存：{is_cache}")
            
            _data = {}
            # max_workers是None还是没有给出，将默认为机器cup个数*5
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_stock = {executor.submit(stf.fetch_stock_hist, stock, date_start, is_cache): stock for stock
                                   in stocks}
                
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        __data = future.result()
                        if __data is not None:
                            _data[stock] = __data
                            logger.info(f"成功获取股票历史数据：{stock[1]}")
                        else:
                            logger.warning(f"股票历史数据为空：{stock[1]}")
                    except Exception as e:
                        logger.error(f"singleton.stock_hist_data处理异常：{stock[1]}代码{e}")
                        import traceback
                        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
            
            if not _data:
                logger.warning("没有获取到任何股票历史数据")
                self.data = None
            else:
                logger.info(f"成功获取股票历史数据，共 {len(_data)} 只股票")
                self.data = _data
                
        except Exception as e:
            logger.error(f"singleton.stock_hist_data处理异常：{e}")
            import traceback
            logger.error(f"异常堆栈信息: {traceback.format_exc()}")
            self.data = None

    def get_data(self):
        logger.info("调用get_data()方法获取股票历史数据")
        return self.data
