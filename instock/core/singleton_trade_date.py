#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from threading import RLock
import instock.core.stockfetch as stf


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
            logging.error(f"singleton.stock_trade_date处理异常：{e}")

    def get_data(self):
        return self.data
