#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import talib as tl
import pandas as pd
import logging

__author__ = 'myh '
__date__ = '2023/3/10 '


# 持续上涨（MA30向上）
def check(code_name, data, date=None, threshold=30):
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

    data.loc[:, 'ma30'] = pd.Series(tl.MA(data['close'].values, 30), index=data.index.values)
    if len(data.index) == 0:
        return
    data = data.tail(n=threshold)

    step1 = round(threshold / 3)
    step2 = round(threshold * 2 / 3)

    if data.iloc[0]['ma30'] < data.iloc[step1]['ma30'] < \
            data.iloc[step2]['ma30'] < data.iloc[-1]['ma30'] and data.iloc[-1]['ma30'] > 1.2 * data.iloc[0]['ma30']:
        return True
    else:
        return False
