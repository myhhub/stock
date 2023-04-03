#!/usr/local/bin/python
# -*- coding: utf-8 -*-


__author__ = 'myh '
__date__ = '2023/3/10 '

# 总市值
BALANCE = 200000


# 最后一个交易日收市价为指定区间内最高价
def check_enter(code_name, data, date=None, threshold=60):
    max_price = 0
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

    for index, row in data.iterrows():
        if row['close'] > max_price:
            max_price = float(row['close'])

    last_close = data.iloc[-1]['close']

    if last_close >= max_price:
        return True

    return False
