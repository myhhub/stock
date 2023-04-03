#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import logging
from instock.core.strategy import turtle_trade

__author__ = 'myh '
__date__ = '2023/3/10 '


# “停机坪”策略
def check(code_name, data, date=None, threshold=15):
    origin_data = data
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
    if len(data.index) < threshold:
        # logging.debug("{0}:样本小于{1}天...\n".format(code_name, threshold))
        return

    data = data.tail(n=threshold)

    flag = False

    # 找出涨停日
    for index, row in data.iterrows():
        try:
            if float(row['p_change']) > 9.5:
                if turtle_trade.check_enter(code_name, origin_data, date=date, threshold=threshold):
                    if check_internal(code_name, data, row):
                        flag = True
        except KeyError as error:
            logging.debug("{}处理异常：{}".format(code_name, error))

    return flag


def check_internal(code_name, data, limitup_row):
    limitup_price = limitup_row['close']
    limitup_end = data.loc[(data['date'] > limitup_row['date'])]
    limitup_end = limitup_end.head(n=3)
    if len(limitup_end.index) < 3:
        return False

    consolidation_day1 = limitup_end.iloc[0]
    consolidation_day23 = limitup_end = limitup_end.tail(n=2)

    if not (consolidation_day1['close'] > limitup_price and consolidation_day1['open'] > limitup_price and
            0.97 < consolidation_day1['close'] / consolidation_day1['open'] < 1.03):
        return False

    threshold_price = limitup_end.iloc[-1]['close']

    for index, row in consolidation_day23.iterrows():
        try:
            if not (0.97 < (row['close'] / row['open']) < 1.03 and -5 < row['p_change'] < 5
                    and row['close'] > limitup_price and row['open'] > limitup_price):
                return False
        except KeyError as error:
            logging.debug("{}处理异常：{}".format(code_name, error))

    # logging.debug("股票{0} 涨停日期：{1}".format(code_name, limitup_row['date']))

    return True
