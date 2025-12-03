#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import datetime
from instock.core.singleton_trade_date import stock_trade_date

# 获取logger实例
logger = logging.getLogger(__name__)

__author__ = 'myh '
__date__ = '2023/4/10 '


def is_trade_date(date=None):
    logger.info(f"开始检查是否为交易日，日期: {date}")
    
    try:
        logger.info("获取交易日历数据")
        trade_date = stock_trade_date().get_data()
        
        if trade_date is None:
            logger.warning("交易日历数据为空")
            return False
        
        logger.info(f"交易日历包含 {len(trade_date)} 个交易日")
        
        if date in trade_date:
            logger.info(f"{date} 是交易日")
            return True
        else:
            logger.info(f"{date} 不是交易日")
            return False
            
    except Exception as e:
        logger.error(f"is_trade_date处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
        return False


def get_previous_trade_date(date):
    logger.info(f"开始获取前一个交易日，当前日期: {date}")
    
    try:
        logger.info("获取交易日历数据")
        trade_date = stock_trade_date().get_data()
        
        if trade_date is None:
            logger.warning("交易日历数据为空，返回原日期")
            return date
        
        tmp_date = date
        logger.info(f"从 {tmp_date} 开始查找前一个交易日")
        
        while True:
            tmp_date += datetime.timedelta(days=-1)
            logger.info(f"检查日期: {tmp_date}")
            
            if tmp_date in trade_date:
                logger.info(f"找到前一个交易日: {tmp_date}")
                return tmp_date
            
            # 防止无限循环，最多查找30天
            if (date - tmp_date).days > 30:
                logger.warning("查找30天后仍未找到前一个交易日，返回原日期")
                return date
                
    except Exception as e:
        logger.error(f"get_previous_trade_date处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
        return date


def get_next_trade_date(date):
    logger.info(f"开始获取下一个交易日，当前日期: {date}")
    
    try:
        logger.info("获取交易日历数据")
        trade_date = stock_trade_date().get_data()
        
        if trade_date is None:
            logger.warning("交易日历数据为空，返回原日期")
            return date
        
        tmp_date = date
        logger.info(f"从 {tmp_date} 开始查找下一个交易日")
        
        while True:
            tmp_date += datetime.timedelta(days=1)
            logger.info(f"检查日期: {tmp_date}")
            
            if tmp_date in trade_date:
                logger.info(f"找到下一个交易日: {tmp_date}")
                return tmp_date
            
            # 防止无限循环，最多查找30天
            if (tmp_date - date).days > 30:
                logger.warning("查找30天后仍未找到下一个交易日，返回原日期")
                return date
                
    except Exception as e:
        logger.error(f"get_next_trade_date处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
        return date


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
    logger.info("开始获取最后一个交易日")
    
    try:
        now_time = datetime.datetime.now()
        logger.info(f"当前时间: {now_time}")
        
        run_date = now_time.date()
        run_date_nph = run_date
        logger.info(f"初始日期: run_date={run_date}, run_date_nph={run_date_nph}")
        
        if is_trade_date(run_date):
            logger.info(f"{run_date} 是交易日")
            
            if not is_close(now_time):
                logger.info("当前时间未收盘")
                run_date = get_previous_trade_date(run_date)
                logger.info(f"将run_date设置为前一个交易日: {run_date}")
                
                if not is_open(now_time):
                    logger.info("当前时间未开盘")
                    run_date_nph = run_date
                    logger.info(f"将run_date_nph设置为: {run_date_nph}")
        else:
            logger.info(f"{run_date} 不是交易日")
            run_date = get_previous_trade_date(run_date)
            run_date_nph = run_date
            logger.info(f"将run_date和run_date_nph设置为前一个交易日: {run_date}")
        
        logger.info(f"最终返回: run_date={run_date}, run_date_nph={run_date_nph}")
        return run_date, run_date_nph
        
    except Exception as e:
        logger.error(f"get_trade_date_last处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
        # 发生异常时返回当前日期
        now_time = datetime.datetime.now()
        return now_time.date(), now_time.date()


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
