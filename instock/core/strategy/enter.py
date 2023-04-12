#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import talib as tl


__author__ = 'myh '
__date__ = '2023/3/10 '


# 量比大于2
# 例如：
#   2017-09-26 2019-02-11 京东方A
#   2019-03-22 浙江龙盛
#   2019-02-13 汇顶科技
#   2019-01-29 新城控股
#   2017-11-16 保利地产
def check_volume(code_name, data, date=None, threshold=60):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if len(data.index) < threshold:
        return False

    p_change = data.iloc[-1]['p_change']
    if p_change < 2 or data.iloc[-1]['close'] < data.iloc[-1]['open']:
        return False

    data.loc[:, 'vol_ma5'] = tl.MA(data['volume'].values, timeperiod=5)
    data['vol_ma5'].values[np.isnan(data['vol_ma5'].values)] = 0.0

    data = data.tail(n=threshold + 1)
    if len(data) < threshold + 1:
        return False

    # 最后一天收盘价
    last_close = data.iloc[-1]['close']
    # 最后一天成交量
    last_vol = data.iloc[-1]['volume']

    amount = last_close * last_vol

    # 成交额不低于2亿
    if amount < 200000000:
        return False

    data = data.head(n=threshold)

    mean_vol = data.iloc[-1]['vol_ma5']

    vol_ratio = last_vol / mean_vol
    if vol_ratio >= 2:
        return True
    else:
        return False
