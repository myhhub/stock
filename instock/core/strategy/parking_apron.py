#!/usr/local/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime
from instock.core.strategy import turtle_trade

__author__ = 'myh '
__date__ = '2023/3/10 '


# 停机坪
# 1.最近15日有涨幅大于9.5%，且必须是放量上涨
# 2.紧接的下个交易日必须高开，收盘价必须上涨，且与开盘价不能大于等于相差3%
# 3.接下2、3个交易日必须高开，收盘价必须上涨，且与开盘价不能大于等于相差3%，且每天涨跌幅在5%间
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
        return False

    data = data.tail(n=threshold)

    limitup_row = [1000000, '']
    # 找出涨停日
    for _close, _p_change, _date in zip(data['close'].values, data['p_change'].values, data['date'].values):
        if _p_change > 9.5:
            if turtle_trade.check_enter(code_name, origin_data, date=datetime.date(datetime.strptime(_date, '%Y-%m-%d')), threshold=threshold):
                limitup_row[0] = _close
                limitup_row[1] = _date
                if check_internal(data, limitup_row):
                    return True
    return False

def check_internal(data, limitup_row):
    limitup_price = limitup_row[0]
    limitup_end = data.loc[(data['date'] > limitup_row[1])]
    limitup_end = limitup_end.head(n=3)
    if len(limitup_end.index) < 3:
        return False

    consolidation_day1 = limitup_end.iloc[0]
    consolidation_day23 = limitup_end.tail(n=2)

    if not (consolidation_day1['close'] > limitup_price and consolidation_day1['open'] > limitup_price and
            0.97 < consolidation_day1['close'] / consolidation_day1['open'] < 1.03):
        return False

    for _close, _p_change, _open in zip(consolidation_day23['close'].values, consolidation_day23['p_change'].values, consolidation_day23['open'].values):
        if not (0.97 < (_close / _open) < 1.03 and -5 < _p_change < 5
                and _close > limitup_price and _open > limitup_price):
            return False

    return True
