#!/usr/local/bin/python
# -*- coding: utf-8 -*-


import talib as tl
import pandas as pd
import logging
from datetime import datetime, timedelta

__author__ = 'myh '
__date__ = '2023/3/10 '


# 使用示例：result = backtrace_ma250.check(code_name, data, end_date=end_date)
# 如：当end_date='2019-02-01'，输出选股结果如下：
# [('601616', '广电电气'), ('002243', '通产丽星'), ('000070', '特发信息'), ('300632', '光莆股份'), ('601700', '风范股份'), ('002017', '东信和平'), ('600775', '南京熊猫'), ('300265', '通光线缆'), ('600677', '航天通信'), ('600776', '东方通信')]
# 当然，该函数中的参数可能存在过拟合的问题


# 回踩年线策略
def check(code_name, data, date=None, threshold=60):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")

    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if len(data.index) < 250:
        # logging.debug("{0}:样本小于250天...\n".format(code_name))
        return

    data.loc[:, 'ma250'] = pd.Series(tl.MA(data['close'].values, 250), index=data.index.values)

    begin_date = data.iloc[0]['date']
    if end_date is not None:
        if end_date < begin_date:  # 该股票在end_date时还未上市
            # logging.debug("{}在{}时还未上市".format(code_name, end_date))
            return False

    data = data.tail(n=threshold)

    # last_close = data.iloc[-1]['close']

    # 区间最低点
    lowest_row = data.iloc[-1]
    # 区间最高点
    highest_row = data.iloc[-1]

    # 近期低点
    recent_lowest_row = data.iloc[-1]

    # 计算区间最高、最低价格
    for index, row in data.iterrows():
        if row['close'] > highest_row['close']:
            highest_row = row
        elif row['close'] < lowest_row['close']:
            lowest_row = row

    if lowest_row['volume'] == 0 or highest_row['volume'] == 0:
        return False

    data_front = data.loc[(data['date'] < highest_row['date'])]
    data_end = data.loc[(data['date'] >= highest_row['date'])]

    if data_front.empty:
        return False
    # 前半段由年线以下向上突破
    if not (data_front.iloc[0]['close'] < data_front.iloc[0]['ma250'] and
            data_front.iloc[-1]['close'] > data_front.iloc[-1]['ma250']):
        return False

    if not data_end.empty:
        # 后半段必须在年线以上运行（回踩年线）
        for index, row in data_end.iterrows():
            if row['close'] < row['ma250']:
                return False
            if row['close'] < recent_lowest_row['close']:
                recent_lowest_row = row

    date_diff = datetime.date(datetime.strptime(recent_lowest_row['date'], '%Y-%m-%d')) - \
                datetime.date(datetime.strptime(highest_row['date'], '%Y-%m-%d'))

    if not (timedelta(days=10) <= date_diff <= timedelta(days=50)):
        return False
    # 回踩伴随缩量
    vol_ratio = highest_row['volume'] / recent_lowest_row['volume']
    back_ratio = recent_lowest_row['close'] / highest_row['close']

    if not (vol_ratio > 2 and back_ratio < 0.8):
        return False

    return True
