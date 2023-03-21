#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import stockstats
import numpy as np

__author__ = 'myh '
__date__ = '2023/3/10 '


def get_indicators(data, end_date=None, threshold=60):
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
    data = data.tail(n=threshold)
    data.loc[:, "volume"] = data["volume"] * 100  # 成交量单位从手变成股。
    data_stat = stockstats.StockDataFrame.retype(data)

    if data_stat is None or len(data_stat.index) == 0:
        return None

    return data_stat


def get_indicator_tail(code_name, data, stock_column, date=None, threshold=60):
    try:
        if date is None:
            end_date = code_name[0]
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]
        # 设置返回数组。
        stock_data_list = [end_date, code]
        # 增加空判断，如果是空返回 0 数据。
        if len(data.index) == 0:
            for i in range(len(stock_column) - 2):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        stockStat = get_indicators(data, end_date, threshold)
        # 增加空判断，如果是空返回 0 数据。
        if stockStat is None:
            for i in range(len(stock_column) - 2):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        # 初始化统计类
        for i in range(len(stock_column) - 2):
            # 将数据的最后一个返回。
            tmp_val = stockStat[stock_column[i + 2]].tail(1).values[0]
            # 解决值中存在INF NaN问题。
            if np.isinf(tmp_val) or np.isnan(tmp_val):
                stock_data_list.append(0)
            else:
                stock_data_list.append(tmp_val)
    except Exception as e:
        logging.debug("{}处理异常：{}代码{}".format('stockstats_data.get_indicator_tail指标计算', code, e))

    return pd.Series(stock_data_list, index=stock_column)
