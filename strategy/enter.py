#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import talib as tl
import pandas as pd
import logging

__author__ = 'myh '
__date__ = '2023/3/10 '


# TODO 真实波动幅度（ATR）放大
# 最后一个交易日收市价从下向上突破指定区间内最高价
def check_breakthrough(code_name, data, date=None, threshold=30):
    max_price = 0
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
    data = data.tail(n=threshold + 1)
    if len(data) < threshold + 1:
        logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
        return False

    # 最后一天收市价
    last_close = float(data.iloc[-1]['close'])
    last_open = float(data.iloc[-1]['open'])

    data = data.head(n=threshold)
    second_last_close = data.iloc[-1]['close']

    for index, row in data.iterrows():
        if row['close'] > max_price:
            max_price = float(row['close'])

    if last_close > max_price > second_last_close and max_price > last_open \
            and last_close / last_open > 1.06:
        return True
    else:
        return False


# 收盘价高于N日均线
def check_ma(code_name, data, date=None, ma_days=250):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]

    if data is None or len(data) < ma_days:
        logging.debug("{0}:样本小于{1}天...\n".format(code_name, ma_days))
        return False

    ma_tag = 'ma' + str(ma_days)
    data.loc[:, ma_tag] = pd.Series(tl.MA(data['close'].values, ma_days), index=data.index.values)

    last_close = data.iloc[-1]['close']
    last_ma = data.iloc[-1][ma_tag]
    if last_close > last_ma:
        return True
    else:
        return False


# 上市日小于60天
def check_new(code_name, data, date=None, threshold=60):
    size = len(data.index)
    if size < threshold:
        return True
    else:
        return False


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
        # logging.debug("{0}:样本小于250天...\n".format(code_name))
        return False

    data.loc[:, 'vol_ma5'] = pd.Series(tl.MA(data['volume'].values, 5), index=data.index.values)

    p_change = data.iloc[-1]['p_change']
    if p_change < 2 \
            or data.iloc[-1]['close'] < data.iloc[-1]['open']:
        return False

    data = data.tail(n=threshold + 1)
    if len(data) < threshold + 1:
        # logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
        return False

    # 最后一天收盘价
    last_close = data.iloc[-1]['close']
    # 最后一天成交量
    last_vol = data.iloc[-1]['volume']

    amount = last_close * last_vol * 100

    # 成交额不低于2亿
    if amount < 200000000:
        return False

    data = data.head(n=threshold)

    mean_vol = data.iloc[-1]['vol_ma5']

    vol_ratio = last_vol / mean_vol
    if vol_ratio >= 2:
        # msg = "*{0}\n量比：{1:.2f}\t涨幅：{2}%\n".format(code_name, vol_ratio, p_change)
        # logging.debug(msg)
        return True
    else:
        return False


# 量比大于3.0
def check_continuous_volume(code_name, data, date=None, threshold=60, window_size=3):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()

    data.loc[:, 'vol_ma5'] = pd.Series(tl.MA(data['volume'].values, 5), index=data.index.values)

    data = data.tail(n=threshold + window_size)
    if len(data) < threshold + window_size:
        # logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold + window_size))
        return False

    # 最后一天收盘价
    last_close = data.iloc[-1]['close']
    # 最后一天成交量
    last_vol = data.iloc[-1]['volume']

    data_front = data.head(n=threshold)
    data_end = data.tail(n=window_size)

    mean_vol = data_front.iloc[-1]['vol_ma5']

    for index, row in data_end.iterrows():
        if float(row['volume']) / mean_vol < 3.0:
            return False

    # msg = "*{0} 量比：{1:.2f}\n\t收盘价：{2}\n".format(code_name, last_vol / mean_vol, last_close)
    # logging.debug(msg)
    return True
