#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import time
import datetime
import concurrent.futures
import logging
import os.path
import sys

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(format='%(asctime)s %(message)s', filename=os.path.join(log_path, 'stock_execute_job.log'))
logging.getLogger().setLevel(logging.INFO)
import init_job as bj
import basic_data_daily_job as hdj
import basic_data_other_daily_job as hdtj
import basic_data_after_close_daily_job as acdj
import indicators_data_daily_job as gdj
import strategy_data_daily_job as sdj
import backtest_data_daily_job as bdj
import klinepattern_data_daily_job as kdj
import selection_data_daily_job as sddj

__author__ = 'myh '
__date__ = '2023/3/10 '


def main():
    start = time.time()
    _start = datetime.datetime.now()
    logging.info("######## 任务执行时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))
    # 第1步创建数据库
    bj.main()
    # 第2.1步创建股票基础数据表
    hdj.main()
    # 第2.2步创建综合股票数据表
    sddj.main()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # # 第3.1步创建股票其它基础数据表
        executor.submit(hdtj.main)
        # # 第3.2步创建股票指标数据表
        executor.submit(gdj.main)
        # # # # 第4步创建股票k线形态表
        executor.submit(kdj.main)
        # # # # 第5步创建股票策略数据表
        executor.submit(sdj.main)

    # # # # 第6步创建股票回测
    bdj.main()

    # # # # 第7步创建股票闭盘后才有的数据
    acdj.main()

    logging.info("######## 完成任务, 使用时间: %s 秒 #######" % (time.time() - start))


# main函数入口
if __name__ == '__main__':
    main()
