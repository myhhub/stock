#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import sys
# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
need_data = os.path.join(cpath_current, 'config', 'trade_client.json')
log_filepath = os.path.join(cpath_current, 'log', 'stock_trade.log')
from instock.trade.robot.engine.main_engine import MainEngine
from instock.trade.robot.infrastructure.default_handler import DefaultLogHandler

__author__ = 'myh '
__date__ = '2023/4/10 '


def main():
    broker = 'gf_client'
    log_handler = DefaultLogHandler(name='交易服务', log_type='file', filepath=log_filepath)
    m = MainEngine(broker, need_data, log_handler)
    m.is_watch_strategy = True  # 策略文件出现改动时,自动重载,不建议在生产环境下使用
    m.load_strategy()
    m.start()


# main函数入口
if __name__ == '__main__':
    main()
