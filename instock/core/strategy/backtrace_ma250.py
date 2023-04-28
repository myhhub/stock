#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import talib as tl
from datetime import datetime, timedelta

__author__ = 'myh '
__date__ = '2023/3/10 '


# 回踩年线
# 1.时间段：前段=最近60交易日最高收盘价之前交易日(长度>0)，后段=最高价当日及后面的交易日
# 2.前段由年线(250日)以下向上突破
# 3.后段必须在年线以上运行，且后段最低价日与最高价日相差必须在10-50日间
# 4.回踩伴随缩量：最高价日交易量/后段最低价日交易量>2,后段最低价/最高价<0.8
def check(code_name, data, date=None, threshold=60):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")

    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if len(data.index) < 250:
        return False

    data.loc[:, 'ma250'] = tl.MA(data['close'].values, timeperiod=250)
    data['ma250'].values[np.isnan(data['ma250'].values)] = 0.0

    data = data.tail(n=threshold)

    # 区间最低点
    lowest_row = [1000000, 0, '']
    # 区间最高点
    highest_row = [0, 0, '']
    # 近期低点
    recent_lowest_row = [1000000, 0, '']

    # 计算区间最高、最低价格
    for _close, _volume, _date in zip(data['close'].values, data['volume'].values, data['date'].values):
        if _close > highest_row[0]:
            highest_row[0] = _close
            highest_row[1] = _volume
            highest_row[2] = _date
        elif _close < lowest_row[0]:
            lowest_row[0] = _close
            lowest_row[1] = _volume
            lowest_row[2] = _date

    if lowest_row[1] == 0 or highest_row[1] == 0:
        return False

    data_front = data.loc[(data['date'] < highest_row[2])]
    data_end = data.loc[(data['date'] >= highest_row[2])]

    if data_front.empty:
        return False
    # 前半段由年线以下向上突破
    if not (data_front.iloc[0]['close'] < data_front.iloc[0]['ma250'] and
            data_front.iloc[-1]['close'] > data_front.iloc[-1]['ma250']):
        return False

    if not data_end.empty:
        # 后半段必须在年线以上运行（回踩年线）
        for _close, _volume, _date, _ma250 in zip(data_end['close'].values, data_end['volume'].values, data_end['date'].values, data_end['ma250'].values):
            if _close < _ma250:
                return False
            if _close < recent_lowest_row[0]:
                recent_lowest_row[0] = _close
                recent_lowest_row[1] = _volume
                recent_lowest_row[2] = _date

    date_diff = datetime.date(datetime.strptime(recent_lowest_row[2], '%Y-%m-%d')) - \
                datetime.date(datetime.strptime(highest_row[2], '%Y-%m-%d'))

    if not (timedelta(days=10) <= date_diff <= timedelta(days=50)):
        return False
    # 回踩伴随缩量
    vol_ratio = highest_row[1] / recent_lowest_row[1]
    back_ratio = recent_lowest_row[0] / highest_row[0]

    if not (vol_ratio > 2 and back_ratio < 0.8):
        return False

    return True
