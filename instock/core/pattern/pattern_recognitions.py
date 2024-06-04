#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

__author__ = 'myh '
__date__ = '2023/3/24 '


def get_pattern_recognitions(data, stock_column, end_date=None, threshold=120, calc_threshold=None):
    isCopy = False
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
        isCopy = True
    if calc_threshold is not None:
        data = data.tail(n=calc_threshold)
        isCopy = True
    if isCopy:
        data = data.copy()

    for k in stock_column:
        try:
            data.loc[:, k] = stock_column[k]['func'](data['open'].values, data['high'].values, data['low'].values, data['close'].values)
        except Exception as e:
            pass

    if data is None or len(data.index) == 0:
        return None

    if threshold is not None:
        data = data.tail(n=threshold).copy()

    return data


def get_pattern_recognition(code_name, data, stock_column, date=None, calc_threshold=12):
    try:
        # 增加空判断，如果是空返回 0 数据。
        if date is None:
            end_date = code_name[0]
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]
        # 设置返回数组。
        # 增加空判断，如果是空返回 0 数据。
        if len(data.index) <= 1:
            return None

        stockStat = get_pattern_recognitions(data, stock_column, end_date=end_date, threshold=1,
                                             calc_threshold=calc_threshold)

        if stockStat is None:
            return None

        isHas = False
        for k in stock_column:
            if stockStat.iloc[0][k] != 0:
                isHas = True
                break

        if isHas:
            stockStat.loc[:, 'code'] = code
            return stockStat.iloc[0, -(len(stock_column) + 1):]

    except Exception as e:
        logging.error(f"pattern_recognitions.get_pattern_recognition处理异常：{code}代码{e}")

    return None
