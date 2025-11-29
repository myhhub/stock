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
            self.data = stf.fetch_stocks_trade_date()
            # 如果获取失败，使用默认的最近一年交易日历作为 fallback
            if self.data is None:
                logging.warning("singleton.stock_trade_date: 无法获取交易日历数据，使用默认值")
                # 生成最近一年的交易日历（简单模拟，实际应该更复杂）
                import datetime
                from instock.lib.trade_time import is_trade_date
                
                self.data = set()
                start_date = datetime.date.today() - datetime.timedelta(days=365)
                current_date = start_date
                
                while current_date <= datetime.date.today():
                    # 简单判断：周一到周五是交易日
                    if current_date.weekday() < 5:
                        self.data.add(current_date)
                    current_date += datetime.timedelta(days=1)
        except Exception as e:
            logging.error(f"singleton.stock_trade_date处理异常：{e}")
            self.data = set()

    def get_data(self):
        return self.data
