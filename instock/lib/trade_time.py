#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import logging
from instock.core.singleton_trade_date import stock_trade_date

__author__ = 'myh '
__date__ = '2023/4/10 '


def is_trade_date(date=None):
    try:
        logging.info(f"检查日期{date}是否为交易日")
        trade_date = stock_trade_date().get_data()
        if trade_date is None:
            logging.warning("股票交易日历数据为空，无法检查日期是否为交易日")
            return False
        if date in trade_date:
            logging.info(f"日期{date}是交易日")
            return True
        else:
            logging.info(f"日期{date}不是交易日")
            return False
    except Exception as e:
        logging.error(f"trade_time.is_trade_date处理异常：{e}")
        return False


def get_previous_trade_date(date):
    trade_date = stock_trade_date().get_data()
    if trade_date is None:
        return date
    tmp_date = date
    while True:
        tmp_date += datetime.timedelta(days=-1)
        if tmp_date in trade_date:
            break
    return tmp_date


def get_next_trade_date(date):
    trade_date = stock_trade_date().get_data()
    if trade_date is None:
        return date
    tmp_date = date
    while True:
        tmp_date += datetime.timedelta(days=1)
        if tmp_date in trade_date:
            break
    return tmp_date


OPEN_TIME = (
    (datetime.time(9, 15, 0), datetime.time(11, 30, 0)),
    (datetime.time(13, 0, 0), datetime.time(15, 0, 0)),
)


def is_tradetime(now_time):
    now = now_time.time()
    for begin, end in OPEN_TIME:
        if begin <= now < end:
            return True
    else:
        return False


PAUSE_TIME = (
    (datetime.time(11, 30, 0), datetime.time(12, 59, 30)),
)


def is_pause(now_time):
    now = now_time.time()
    for b, e in PAUSE_TIME:
        if b <= now < e:
            return True


CONTINUE_TIME = (
    (datetime.time(12, 59, 30), datetime.time(13, 0, 0)),
)


def is_continue(now_time):
    now = now_time.time()
    for b, e in CONTINUE_TIME:
        if b <= now < e:
            return True
    return False


CLOSE_TIME = (
    datetime.time(15, 0, 0),
)


def is_closing(now_time, start=datetime.time(14, 54, 30)):
    now = now_time.time()
    for close in CLOSE_TIME:
        if start <= now < close:
            return True
    return False


def is_close(now_time):
    now = now_time.time()
    for close in CLOSE_TIME:
        if now >= close:
            return True
    return False


def is_open(now_time):
    now = now_time.time()
    if now >= datetime.time(9, 30, 0):
        return True
    return False


def get_trade_hist_interval(date):
    tmp_year, tmp_month, tmp_day = date.split("-")
    date_end = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
    date_start = (date_end + datetime.timedelta(days=-(365 * 3))).strftime("%Y%m%d")

    now_time = datetime.datetime.now()
    now_date = now_time.date()
    is_trade_date_open_close_between = False
    if date_end.date() == now_date:
        if is_trade_date(now_date):
            if is_open(now_time) and not is_close(now_time):
                is_trade_date_open_close_between = True

    return date_start, not is_trade_date_open_close_between


def get_trade_date_last():
    try:
        logging.info("获取最后一个交易日")
        now_time = datetime.datetime.now()
        run_date = now_time.date()
        run_date_nph = run_date
        logging.info(f"当前日期：{run_date}")
        if is_trade_date(run_date):
            logging.info("当前日期是交易日")
            if not is_close(now_time):
                logging.info("当前时间未收盘，获取上一个交易日")
                run_date = get_previous_trade_date(run_date)
                if not is_open(now_time):
                    logging.info("当前时间未开盘，更新run_date_nph为上一个交易日")
                    run_date_nph = run_date
        else:
            logging.info("当前日期不是交易日，获取上一个交易日")
            run_date = get_previous_trade_date(run_date)
            run_date_nph = run_date
        logging.info(f"最后一个交易日：{run_date}")
        logging.info(f"最后一个交易日（盘后）：{run_date_nph}")
        return run_date, run_date_nph
    except Exception as e:
        logging.error(f"trade_time.get_trade_date_last处理异常：{e}")
        return None, None


def get_quarterly_report_date():
    now_time = datetime.datetime.now()
    year = now_time.year
    month = now_time.month
    if 1 <= month <= 3:
        month_day = '1231'
    elif 4 <= month <= 6:
        month_day = '0331'
    elif 7 <= month <= 9:
        month_day = '0630'
    else:
        month_day = '0930'
    return f"{year}{month_day}"


def get_bonus_report_date():
    now_time = datetime.datetime.now()
    year = now_time.year
    month = now_time.month
    if 2 <= month <= 6:
        year -= 1
        month_day = '1231'
    elif 8 <= month <= 12:
        month_day = '0630'
    elif month == 7:
        if now_time.day > 25:
            month_day = '0630'
        else:
            year -= 1
            month_day = '1231'
    else:
        year -= 1
        if now_time.day > 25:
            month_day = '1231'
        else:
            month_day = '0630'
    return f"{year}{month_day}"
