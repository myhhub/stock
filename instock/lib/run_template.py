#!/usr/local/bin/python
# -*- coding: utf-8 -*-


import logging
import datetime
import concurrent.futures
import sys
import time
import instock.lib.trade_time as trd

# 获取logger实例
logger = logging.getLogger(__name__)

__author__ = 'myh '
__date__ = '2023/3/10 '


# 通用函数，获得日期参数，支持批量作业。
def run_with_args(run_fun, *args):
    logger.info(f"开始执行run_with_args，函数: {run_fun.__name__}, 参数: {args}")
    logger.info(f"命令行参数数量: {len(sys.argv)}")
    
    try:
        if len(sys.argv) == 3:
            # 区间作业 python xxx.py 2023-03-01 2023-03-21
            logger.info("执行区间作业模式")
            tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
            start_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
            tmp_year, tmp_month, tmp_day = sys.argv[2].split("-")
            end_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
            logger.info(f"区间作业开始日期: {start_date}, 结束日期: {end_date}")
            
            run_date = start_date
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    logger.info("创建线程池执行区间作业")
                    while run_date <= end_date:
                        logger.info(f"检查日期: {run_date}")
                        if trd.is_trade_date(run_date):
                            logger.info(f"{run_date} 是交易日，提交任务")
                            executor.submit(run_fun, run_date, *args)
                            time.sleep(2)
                        else:
                            logger.info(f"{run_date} 不是交易日，跳过")
                        run_date += datetime.timedelta(days=1)
                logger.info("区间作业执行完成")
            except Exception as e:
                logger.error(f"区间作业执行异常：{e}")
                import traceback
                logger.error(f"异常堆栈信息: {traceback.format_exc()}")
                raise
        
        elif len(sys.argv) == 2:
            # N个时间作业 python xxx.py 2023-03-01,2023-03-02
            logger.info("执行多个日期作业模式")
            dates = sys.argv[1].split(',')
            logger.info(f"要处理的日期列表: {dates}")
            
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    logger.info("创建线程池执行多个日期作业")
                    for date_str in dates:
                        tmp_year, tmp_month, tmp_day = date_str.split("-")
                        run_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
                        logger.info(f"检查日期: {run_date}")
                        if trd.is_trade_date(run_date):
                            logger.info(f"{run_date} 是交易日，提交任务")
                            executor.submit(run_fun, run_date, *args)
                            time.sleep(2)
                        else:
                            logger.info(f"{run_date} 不是交易日，跳过")
                logger.info("多个日期作业执行完成")
            except Exception as e:
                logger.error(f"多个日期作业执行异常：{e}")
                import traceback
                logger.error(f"异常堆栈信息: {traceback.format_exc()}")
                raise
        
        else:
            # 当前时间作业 python xxx.py
            logger.info("执行当前时间作业模式")
            
            try:
                logger.info("获取最后一个交易日")
                run_date, run_date_nph = trd.get_trade_date_last()
                logger.info(f"获取到的交易日: run_date={run_date}, run_date_nph={run_date_nph}")
                
                # 根据函数名选择合适的日期
                if run_fun.__name__.startswith('save_nph'):
                    logger.info(f"函数名以'save_nph'开头，使用日期: {run_date_nph}")
                    run_fun(run_date_nph, *args)
                elif run_fun.__name__.startswith('save_after_close'):
                    logger.info(f"函数名以'save_after_close'开头，使用日期: {run_date}")
                    run_fun(run_date, *args)
                else:
                    logger.info(f"使用默认日期: {run_date_nph}")
                    run_fun(run_date_nph, *args)
                
                logger.info("当前时间作业执行完成")
            except Exception as e:
                logger.error(f"当前时间作业执行异常：{e}")
                import traceback
                logger.error(f"异常堆栈信息: {traceback.format_exc()}")
                raise
        
    except Exception as e:
        logger.error(f"run_template.run_with_args处理异常：函数={run_fun.__name__}, 参数={sys.argv}, 异常={e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
        raise
