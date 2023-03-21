#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import talib as tl
import pandas as pd
import logging
from strategy import enter

__author__ = 'myh '
__date__ = '2023/3/10 '


# 平台突破策略
def check(code_name, data, date=None, threshold=60):
    origin_data = data
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if len(data.index) < threshold:
        # logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
        return

    data.loc[:, 'ma60'] = pd.Series(tl.MA(data['close'].values, 60), index=data.index.values)

    data = data.tail(n=threshold)

    breakthrough_row = None

    for index, row in data.iterrows():
        if row['open'] < row['ma60'] <= row['close']:
            if enter.check_volume(code_name, origin_data, date=date, threshold=threshold):
                breakthrough_row = row

    if breakthrough_row is None:
        return False

    data_front = data.loc[(data['date'] < breakthrough_row['date'])]
    data_end = data.loc[(data['date'] >= breakthrough_row['date'])]

    for index, row in data_front.iterrows():
        if not (-0.05 < (row['ma60'] - row['close']) / row['ma60'] < 0.2):
            return False

    return True
