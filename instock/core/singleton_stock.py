#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import logging
import concurrent.futures
import instock.core.stockfetch as stf
import instock.core.tablestructure as tbs
import instock.lib.trade_time as trd
from instock.lib.singleton_type import singleton_type

__author__ = 'myh '
__date__ = '2023/3/10 '


# 读取当天股票数据
class stock_data(metaclass=singleton_type):
    def __init__(self, date):
        try:
            self.data = stf.fetch_stocks(date)
        except Exception as e:
            logging.error(f"singleton.stock_data处理异常：{e}")

    def get_data(self):
        return self.data


# 读取股票历史数据
class stock_hist_data(metaclass=singleton_type):
    def __init__(self, date=None, stocks=None, workers=16):
        if stocks is None:
            stocks = stf.get_stock_code_name()  # 不再传递date参数，使用最大date
            # _subset = stock_data(date).get_data()[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
            # stocks = [tuple(x) for x in _subset.values()]
        if stocks is None:
            self.data = None
            return
        date_start, is_cache = trd.get_trade_hist_interval(stocks[0][0])  # 提高运行效率，只运行一次
        _data = {}
        stock_codes = [stock[1] for stock in stocks]
        try:
            # DEBUG BELOW
            now = time.time()
            print(f"Read db:{now}")
            db_data = stf.get_stock_hist_from_db(date_start)
            print(f"End seconds:{time.time()-now}")
            self.data = self.batch_fetch_stock_hist_optimized(stock_codes, date_start, db_data, stocks)
           
            # max_workers是None还是没有给出，将默认为机器cup个数*5
            #print(f"stock_hist_data: 开始多线程获取股票历史数据，线程数{workers}，股票数{len(stocks)}")
            #with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            #    future_to_stock = {executor.submit(stf.fetch_stock_hist, stock, date_start, is_cache, db_data): stock for stock
            #                       in stocks}
            #    for future in concurrent.futures.as_completed(future_to_stock):
            #        stock = future_to_stock[future]
            #        try:
            #            __data = future.result()
            #            if __data is not None:
            #                _data[stock] = __data
            #        except Exception as e:
            #            logging.error(f"singleton.stock_hist_data处理异常：{stock[1]}代码{e}")
        except Exception as e:
            logging.exception(f"singleton.stock_hist_data处理异常：{e}")
    def batch_fetch_stock_hist_optimized(self, stock_codes, date_start, cached_data, stocks):
        """更高效的批量处理版本"""
        if cached_data is None or cached_data.empty:
            return {}
        stock_mapping = {stock[1]: stock for stock in stocks}
        result = {}
        print(f"Batch processing {len(stock_codes)} stocks")
        # 按code分组，每组就是一个股票的完整数据
        grouped = cached_data.groupby('code')
    
        for full_code, group_data in grouped:
            # 提取股票代码后缀进行匹配
            code_suffix = full_code.split('.')[-1] if '.' in full_code else full_code
            if code_suffix in stock_mapping:
                result[stock_mapping[code_suffix]] = group_data
        print(f"Batch processing completed, found {len(result)} matching stocks")

        return result

    def get_data(self):
        return self.data
