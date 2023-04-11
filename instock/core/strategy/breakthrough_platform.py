#!/usr/local/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime
import numpy as np
import talib as tl
from instock.core.strategy import enter

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
        data = data.loc[mask]
    if len(data.index) < threshold:
        return False

    data.loc[:, 'ma60'] = tl.MA(data['close'].values, timeperiod=60)
    data['ma60'].values[np.isnan(data['ma60'].values)] = 0.0

    data = data.tail(n=threshold)

    breakthrough_row = None
    for _close, _open, _date, _ma60 in zip(data['close'].values, data['open'].values, data['date'].values, data['ma60'].values):
        if _open < _ma60 <= _close:
            if enter.check_volume(code_name, origin_data, date=datetime.date(datetime.strptime(_date, '%Y-%m-%d')), threshold=threshold):
                breakthrough_row = _date
                break

    if breakthrough_row is None:
        return False

    data_front = data.loc[(data['date'] < breakthrough_row)]
    for _close, _ma60 in zip(data_front['close'].values, data_front['ma60'].values):
        if not (-0.05 < ((_ma60 - _close) / _ma60) < 0.2):
            return False

    return True
