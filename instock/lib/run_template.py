#!/usr/local/bin/python
# -*- coding: utf-8 -*-


import logging
import datetime
import concurrent.futures
import sys
import time
import instock.lib.trade_time as trd

__author__ = 'myh '
__date__ = '2023/3/10 '


# 通用函数，获得日期参数，支持批量作业。
def run_with_args(run_fun, *args):
    try:
        logging.info(f"开始执行任务：{run_fun.__name__}")
        if len(sys.argv) == 3:
            # 区间作业 python xxx.py 2023-03-01 2023-03-21
            tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
            start_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
            tmp_year, tmp_month, tmp_day = sys.argv[2].split("-")
            end_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
            run_date = start_date
            logging.info(f"区间作业：从{start_date}到{end_date}")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                while run_date <= end_date:
                    if trd.is_trade_date(run_date):
                        logging.info(f"执行任务：{run_fun.__name__}，日期：{run_date}")
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)
                    run_date += datetime.timedelta(days=1)
        elif len(sys.argv) == 2:
            # N个时间作业 python xxx.py 2023-03-01,2023-03-02
            dates = sys.argv[1].split(',')
            logging.info(f"多个日期作业：{dates}")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for date in dates:
                    tmp_year, tmp_month, tmp_day = date.split("-")
                    run_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
                    if trd.is_trade_date(run_date):
                        logging.info(f"执行任务：{run_fun.__name__}，日期：{run_date}")
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)
        else:
            # 当前时间作业 python xxx.py
            logging.info("当前时间作业")
            run_date, run_date_nph = trd.get_trade_date_last()
            if run_fun.__name__.startswith('save_nph'):
                logging.info(f"执行盘前任务：{run_fun.__name__}，日期：{run_date_nph}")
                run_fun(run_date_nph, False)
            elif run_fun.__name__.startswith('save_after_close'):
                logging.info(f"执行盘后任务：{run_fun.__name__}，日期：{run_date}")
                run_fun(run_date, *args)
            else:
                logging.info(f"执行任务：{run_fun.__name__}，日期：{run_date_nph}")
                run_fun(run_date_nph, *args)
        logging.info(f"任务执行完成：{run_fun.__name__}")
    except Exception as e:
        logging.error(f"run_template.run_with_args处理异常：{run_fun.__name__}{sys.argv}{e}")
