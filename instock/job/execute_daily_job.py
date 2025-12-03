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

# 控制台日志处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# 文件日志处理器
file_handler = logging.FileHandler(os.path.join(log_path, 'stock_execute_job.log'), encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')
file_handler.setFormatter(file_formatter)

# 添加日志处理器到logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)
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
    logger.info("######## 任务执行开始时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))
    
    try:
        # 第1步创建数据库
        logger.info("开始执行第1步: 创建数据库")
        bj.main()
        logger.info("第1步执行完成")
        
        # 第2.1步创建股票基础数据表
        logger.info("开始执行第2.1步: 创建股票基础数据表")
        hdj.main()
        logger.info("第2.1步执行完成")
        
        # 第2.2步创建综合股票数据表
        logger.info("开始执行第2.2步: 创建综合股票数据表")
        sddj.main()
        logger.info("第2.2步执行完成")
        
        # 使用线程池执行并行任务
        logger.info("开始执行并行任务")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            # 第3.1步创建股票其它基础数据表
            future1 = executor.submit(hdtj.main)
            logger.info("提交任务: 第3.1步创建股票其它基础数据表")
            
            # 第3.2步创建股票指标数据表
            future2 = executor.submit(gdj.main)
            logger.info("提交任务: 第3.2步创建股票指标数据表")
            
            # 第4步创建股票k线形态表
            future3 = executor.submit(kdj.main)
            logger.info("提交任务: 第4步创建股票k线形态表")
            
            # 第5步创建股票策略数据表
            future4 = executor.submit(sdj.main)
            logger.info("提交任务: 第5步创建股票策略数据表")
            
            # 等待所有任务完成
            logger.info("等待所有并行任务完成")
            futures = [future1, future2, future3, future4]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # 获取任务结果，以便捕获异常
                except Exception as e:
                    logger.error(f"并行任务执行失败: {e}")
        
        logger.info("所有并行任务执行完成")
        
        # 第6步创建股票回测
        logger.info("开始执行第6步: 创建股票回测")
        bdj.main()
        logger.info("第6步执行完成")
        
        # 第7步创建股票闭盘后才有的数据
        logger.info("开始执行第7步: 创建股票闭盘后才有的数据")
        acdj.main()
        logger.info("第7步执行完成")
        
        end_time = time.time() - start
        logger.info("######## 所有任务执行完成, 总用时: %.2f 秒 #######" % end_time)
        
    except Exception as e:
        logger.error(f"主任务执行过程中发生异常: {e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")


# main函数入口
if __name__ == '__main__':
    main()
