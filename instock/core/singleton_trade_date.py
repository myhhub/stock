#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import instock.core.stockfetch as stf
from instock.lib.singleton_type import singleton_type

__author__ = 'myh '
__date__ = '2023/3/10 '


# 读取股票交易日历数据
class stock_trade_date(metaclass=singleton_type):
    def __init__(self):
        try:
            logging.info("初始化股票交易日历单例")
            self.data = stf.fetch_stocks_trade_date()
            if self.data is None:
                logging.warning("股票交易日历单例数据为空")
            else:
                logging.info(f"股票交易日历单例初始化成功，共{len(self.data)}个交易日")
        except Exception as e:
            logging.error(f"singleton.stock_trade_date处理异常：{e}")

    def get_data(self):
        return self.data
