#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import instock.core.stockfetch as stf
from instock.lib.singleton_type import singleton_type

# 获取logger实例
logger = logging.getLogger(__name__)

__author__ = 'myh '
__date__ = '2023/3/10 '


# 读取股票交易日历数据
class stock_trade_date(metaclass=singleton_type):
    def __init__(self):
        logger.info("开始初始化stock_trade_date单例")
        try:
            logger.info("开始获取股票交易日历数据")
            self.data = stf.fetch_stocks_trade_date()
            
            if self.data is not None:
                logger.info(f"成功获取股票交易日历数据，共 {len(self.data)} 个交易日")
            else:
                logger.warning("获取到的股票交易日历数据为空")
                
        except Exception as e:
            logger.error(f"singleton.stock_trade_date处理异常：{e}")
            import traceback
            logger.error(f"异常堆栈信息: {traceback.format_exc()}")
            self.data = None

    def get_data(self):
        logger.info("调用get_data()方法获取交易日历数据")
        return self.data
