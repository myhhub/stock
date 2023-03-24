#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

__author__ = 'myh '
__date__ = '2023/3/24 '


def get_pattern_recognitions(data, stock_column, end_date=None, threshold=60):
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    data = data.tail(n=threshold)

    if len(data.index) <= 1:
        return None

    for k, v in stock_column.items():
        data.loc[:, k] = v['func'](data['open'].values, data['high'].values, data['low'].values, data['close'].values)

    if data is None or len(data.index) == 0:
        return None

    return data


def get_pattern_recognition_tail(code_name, data, stock_column, date=None, threshold=6):
    try:
        # 增加空判断，如果是空返回 0 数据。
        if date is None:
            end_date = code_name[0]
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]
        # 设置返回数组。

        # 增加空判断，如果是空返回 0 数据。
        if len(data.index) == 0:
            return None

        stockStat = get_pattern_recognitions(data, stock_column, end_date=end_date, threshold=6)

        if stockStat is None:
            return None

        stockStat = stockStat.tail(1)

        isHas = False
        for k in stock_column.keys():
            if stockStat.iloc[0][k] != 0:
                isHas = True
                break

        if isHas:
            stockStat.loc[:, 'code'] = code
            return stockStat.iloc[0, -(len(stock_column) + 1):]

    except Exception as e:
        logging.debug("{}处理异常：{}代码{}".format('pattern_recognitions.get_pattern_recognition_tail', code, e))

    return None
