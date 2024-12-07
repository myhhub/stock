#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import talib as tl

__author__ = 'myh '
__date__ = '2023/3/10 '

# 量价突破策略
# 1.最近七天有一天成交量大于当天-7天成交量均值的150%
# 2.当天的收盘价低于最近七天的最高价的15%
# 3.当天的成交量低于最高日的成交量
# 4.当天的最低价格高于昨天的最低价
def check(code_name, data, date=None, threshold=60):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if len(data.index) < threshold:
        return False

    # 参数设置
    volume_threshold = 1.5
    price_drop_threshold = 0.15
    lookback_period = 7

    # 计算需要的指标
    data['volume_ma'] = tl.MA(data['volume'].values, timeperiod=lookback_period)
    data['volume_ma'].values[np.isnan(data['volume_ma'].values)] = 0.0
    data['high_price'] = data['high'].rolling(window=lookback_period).max()
    data['max_volume'] = data['volume'].rolling(window=lookback_period).max()

    # 获取最近的数据
    recent_data = data.tail(lookback_period)
    latest_data = recent_data.iloc[-1]

    # 条件1: 最近七天有一天成交量大于当天-7天成交量均值的150%
    volume_condition = (recent_data['volume'] > recent_data['volume_ma'] * volume_threshold).any()

    # 条件2: 当天的收盘价低于最近七天的最高价的15%
    price_condition = latest_data['close'] < latest_data['high_price'] * (1 - price_drop_threshold)

    # 条件3: 当天的成交量低于最高日的成交量
    current_volume_condition = latest_data['volume'] < latest_data['max_volume']

    # 条件4: 当天的最低价格高于昨天的最低价
    low_price_condition = latest_data['low'] > data['low'].iloc[-2]

    # 所有条件都满足时返回True
    return (volume_condition and price_condition and
            current_volume_condition and low_price_condition)

