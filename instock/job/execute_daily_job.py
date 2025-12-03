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
# 配置日志，同时输出到控制台和文件
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 格式器
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# 文件处理器
file_handler = logging.FileHandler(os.path.join(log_path, 'stock_execute_job.log'))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 控制台处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
import instock.job.init_job as bj
import instock.job.basic_data_daily_job as hdj
import instock.job.basic_data_other_daily_job as hdtj
import instock.job.basic_data_after_close_daily_job as acdj
import instock.job.indicators_data_daily_job as gdj
import instock.job.strategy_data_daily_job as sdj
import instock.job.backtest_data_daily_job as bdj
import instock.job.klinepattern_data_daily_job as kdj
import instock.job.selection_data_daily_job as sddj

__author__ = 'myh '
__date__ = '2023/3/10 '


def main():
    start = time.time()
    _start = datetime.datetime.now()
    logging.info("######## 任务执行时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))
    
    # 第1步创建数据库
    logging.info("######## 开始执行第1步：创建数据库 #######")
    bj.main()
    logging.info("######## 第1步执行完成 #######")
    
    # 第2.1步创建股票基础数据表
    logging.info("######## 开始执行第2.1步：创建股票基础数据表 #######")
    hdj.main()
    logging.info("######## 第2.1步执行完成 #######")
    
    # 第2.2步创建综合股票数据表
    logging.info("######## 开始执行第2.2步：创建综合股票数据表 #######")
    sddj.main()
    logging.info("######## 第2.2步执行完成 #######")
    
    # 第3、4、5步：并行执行股票其它基础数据表、指标数据表、k线形态表、策略数据表
    logging.info("######## 开始并行执行第3、4、5步：股票其它基础数据表、指标数据表、k线形态表、策略数据表 #######")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 第3.1步创建股票其它基础数据表
        logging.info("######## 开始执行第3.1步：创建股票其它基础数据表 #######")
        future_hdtj = executor.submit(hdtj.main)
        
        # 第3.2步创建股票指标数据表
        logging.info("######## 开始执行第3.2步：创建股票指标数据表 #######")
        future_gdj = executor.submit(gdj.main)
        
        # 第4步创建股票k线形态表
        logging.info("######## 开始执行第4步：创建股票k线形态表 #######")
        future_kdj = executor.submit(kdj.main)
        
        # 第5步创建股票策略数据表
        logging.info("######## 开始执行第5步：创建股票策略数据表 #######")
        future_sdj = executor.submit(sdj.main)
        
        # 等待所有并行任务完成
        concurrent.futures.wait([future_hdtj, future_gdj, future_kdj, future_sdj])
        
        # 检查并行任务是否有异常
        for future in [future_hdtj, future_gdj, future_kdj, future_sdj]:
            if future.exception():
                logging.error(f"并行任务执行异常：{future.exception()}")
    
    logging.info("######## 第3、4、5步并行执行完成 #######")
    
    # 第6步创建股票回测
    logging.info("######## 开始执行第6步：创建股票回测 #######")
    bdj.main()
    logging.info("######## 第6步执行完成 #######")
    
    # 第7步创建股票闭盘后才有的数据
    logging.info("######## 开始执行第7步：创建股票闭盘后才有的数据 #######")
    acdj.main()
    logging.info("######## 第7步执行完成 #######")
    
    logging.info("######## 所有任务执行完成, 使用时间: %s 秒 #######" % (time.time() - start))


# main函数入口
if __name__ == '__main__':
    main()
