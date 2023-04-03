#!/usr/local/bin/python
# -*- coding: utf-8 -*-


import logging
import datetime
import concurrent.futures
import sys
import time

from instock.core.singleton import stock_trade_date

__author__ = 'myh '
__date__ = '2023/3/10 '


def is_stock_trade_date(date):
    if date in stock_trade_date().get_data():
        return True
    else:
        return False


def get_stock_trade_date(date=None, hour=15):
    current_time = datetime.datetime.now()
    if date is None:
        tmp_date = current_time.date()
    else:
        tmp_date = date

    if tmp_date == current_time.date():
        hour_int = int(current_time.strftime("%H"))
        # 每天15点前获取昨天的数据
        if hour_int < hour:
            current_time = (current_time + datetime.timedelta(days=-1))
        tmp_date = current_time.date()

    while tmp_date not in stock_trade_date().get_data():
        tmp_date += datetime.timedelta(days=-1)

    return tmp_date


def get_current_date():
    return datetime.datetime.now().date()


def is_get_data(date, hour=15):
    current_time = datetime.datetime.now()
    if date != current_time.date():
        return False

    hour_int = int(current_time.strftime("%H"))
    if hour_int < hour:
        return False

    return True


# 通用函数，获得日期参数，支持批量作业。
def run_with_args(run_fun, *args):
    if len(sys.argv) == 3:
        # 区间作业 python xxx.py 2023-03-01 2023-03-21
        tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
        start_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
        tmp_year, tmp_month, tmp_day = sys.argv[2].split("-")
        end_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
        run_date = start_date
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                while run_date <= end_date:
                    if is_stock_trade_date(run_date):
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)
                    run_date += datetime.timedelta(days=1)
        except Exception as e:
            logging.debug("{}处理异常：{}{}{}".format('run_template.run_with_args', run_fun, sys.argv, e))
    elif len(sys.argv) == 2:
        # N个时间作业 python xxx.py 2023-03-01,2023-03-02
        dates = sys.argv[1].split(',')
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for date in dates:
                    tmp_year, tmp_month, tmp_day = date.split("-")
                    run_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
                    if is_stock_trade_date(run_date):
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)
        except Exception as e:
            logging.debug("{}处理异常：{}{}{}".format('run_template.run_with_args', run_fun, sys.argv, e))
    else:
        # 当前时间作业 python xxx.py
        try:
            run_date = get_stock_trade_date()
            run_fun(run_date, *args)  # 使用当前时间
        except Exception as e:
            logging.debug("{}处理异常：{}{}{}".format('run_template.run_with_args', run_fun, sys.argv, e))
