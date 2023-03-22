#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import talib as tl
import logging

__author__ = 'myh '
__date__ = '2023/3/10 '


# 低ATR成长策略
def check_low_increase(code_name, data, date=None, ma_short=30, ma_long=250, threshold=10):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if len(data.index) < ma_long:
        # logging.debug("{0}:样本小于{1}天...\n".format(code_name, ma_long))
        return False

    data.loc[:, 'ma_short'] = pd.Series(tl.MA(data['close'].values, ma_short), index=data.index.values)
    data.loc[:, 'ma_long'] = pd.Series(tl.MA(data['close'].values, ma_long), index=data.index.values)

    data = data.tail(n=threshold)
    inc_days = 0
    dec_days = 0
    if len(data.index) < threshold:
        # logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
        return False

    # 区间最低点
    lowest_row = data.iloc[-1]
    # 区间最高点
    highest_row = data.iloc[-1]

    days_count = len(data)
    total_change = 0.0
    for index, row in data.iterrows():
        if 'p_change' in row:
            p_change = float(row['p_change'])
            if abs(p_change) > 0:
                total_change += abs(p_change)
            # if p_change < -7:
            #     return False
            # if row['ma_short'] < row['ma_long']:
            #     return False

            if p_change > 0:
                inc_days = inc_days + 1
            if p_change < 0:
                dec_days = dec_days + 1

            if row['close'] > highest_row['close']:
                highest_row = row
            if row['close'] < lowest_row['close']:
                lowest_row = row

    atr = total_change / days_count
    if atr > 10:
        return False

    ratio = (highest_row['close'] - lowest_row['close']) / lowest_row['close']

    if ratio > 1.1:
        # stock = code_name[1]
        # debug_info = "股票：（{1}） 最低:{2}, 最高:{3}, 涨跌比率:{4} 上涨天数:{5}，下跌天数:{6}"
        # logging.debug(debug_info.format(stock, lowest_row['date'], highest_row['date'], ratio, inc_days, dec_days))
        return True

    return False
