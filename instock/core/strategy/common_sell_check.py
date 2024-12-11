#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import talib as tl

__author__ = 'myh '
__date__ = '2023/3/10 '

# 止损止盈
params = {
    'stop_loss': 0.06,
    'take_profit': 0.30,
    'volume_threshold': 1.5,
    'price_drop_threshold': 0.15,
    'lookback_period': 7
}

# 通用卖出策略
def check(code_name, data, date=None, cost=None, threshold=60):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if cost is None:
        return False
    if len(data.index) < threshold:
        return True
    # 检查是否应该卖出
    if check_sell_signal(data, cost=cost, stop_loss=params['stop_loss'], take_profit=params['take_profit']):
        return True  # 如果应该卖出，则不考虑买入

    return False

def check_sell_signal(data, cost, stop_loss, take_profit):
    latest_data = data.iloc[-1]
    previous_data = data.iloc[-2]

    if cost:
        # 检查止损
        if latest_data['close'] <= cost * (1 - stop_loss):
            return True

        # 检查止盈
        if latest_data['close'] >= cost * (1 + take_profit):
            if latest_data['low'] < previous_data['low'] and latest_data['high'] < previous_data['high']:
                return True

    # 检查最高和最低点是否低于昨日
    if (latest_data['low'] < previous_data['low'] < data.iloc[-3]['low'] and
        latest_data['high'] < previous_data['high'] < data.iloc[-3]['high']):
        return True

    # 检查成交量和价格条件
    if (latest_data['volume'] == max(data['volume'].tail(params['lookback_period'])) and
        latest_data['close'] < previous_data['low']):
        return True

    return False

