#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from instock.core.singleton import stock_trade_date

__author__ = 'myh '
__date__ = '2023/4/10 '


def is_trade_date(date=None):
    if date in stock_trade_date().get_data():
        return True
    else:
        return False


def get_previous_trade_date(date):
    tmp_date = date
    while True:
        tmp_date += datetime.timedelta(days=-1)
        if tmp_date in stock_trade_date().get_data():
            break
    return tmp_date


def get_next_trade_date(date):
    tmp_date = date
    while True:
        tmp_date += datetime.timedelta(days=1)
        if tmp_date in stock_trade_date().get_data():
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
