#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from threading import RLock
import concurrent.futures
import instock.core.stockfetch as stf
import instock.core.tablestructure as tbs

__author__ = 'myh '
__date__ = '2023/3/10 '


class SingletonType(type):
    single_lock = RLock()

    def __call__(cls, *args, **kwargs):  # 创建cls的对象时候调用
        with SingletonType.single_lock:
            if not hasattr(cls, "_instance"):
                cls._instance = super(SingletonType, cls).__call__(*args, **kwargs)  # 创建cls的对象

        return cls._instance


# 读取股票交易日历数据
class stock_trade_date(metaclass=SingletonType):
    def __init__(self):
        try:
            self.data = stf.fetch_stocks_trade_date()
        except Exception as e:
            logging.debug("{}处理异常：{}".format('singleton.stock_trade_date', e))

    def get_data(self):
        return self.data


# 读取当天股票数据
class stock_data(metaclass=SingletonType):
    def __init__(self, date):
        try:
            self.data = stf.fetch_stocks(date)
        except Exception as e:
            logging.debug("{}处理异常：{}".format('singleton.stock_data', e))

    def get_data(self):
        return self.data


# 读取股票历史数据
class stock_hist_data(metaclass=SingletonType):
    def __init__(self, date=None, stocks=None, workers=16):
        if stocks is None:
            _subset = stock_data(date).get_data()[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
            stocks = [tuple(x) for x in _subset.values]
        if stocks is None:
            self.data = None
            return
        _data = {}
        try:
            # max_workers是None还是没有给出，将默认为机器cup个数*5
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_stock = {executor.submit(stf.fetch_stock_hist, stock): stock for stock in stocks}
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        __data = future.result()
                        if __data is not None:
                            _data[stock] = __data
                    except Exception as e:
                        logging.debug(
                            "{}处理异常：{}代码{}".format('singleton.stock_hist_data', stock[1], e))
        except Exception as e:
            logging.debug("{}处理异常：{}".format('singleton.stock_hist_data', e))
        if not _data:
            self.data = None
        else:
            self.data = _data

    def get_data(self):
        return self.data
