#!/usr/local/bin/python
# -*- coding: utf-8 -*-


__author__ = 'myh '
__date__ = '2023/3/10 '


# 无大幅回撤
# 1.当日收盘价比60日前的收盘价的涨幅小于0.6
# 2.最近60日，不能有单日跌幅超7%、高开低走7%、两日累计跌幅10%、两日高开低走累计10%
def check(code_name, data, date=None, threshold=60):
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

    ratio_increase = (data.iloc[-1]['close'] - data.iloc[0]['close']) / data.iloc[0]['close']
    if ratio_increase < 0.6:
        return False

    # 允许有一次“洗盘”
    previous_p_change = 100.0
    previous_open = -1000000.0
    for _p_change, _close, _open in zip(data['p_change'].values, data['close'].values, data['open'].values):
        # 单日跌幅超7%；高开低走7%；两日累计跌幅10%；两日高开低走累计10%
        if _p_change < -7 or (_close - _open) / _open * 100 < -7 \
                or previous_p_change + _p_change < -10 \
                or (_close - previous_open)/previous_open * 100 < -10:
            return False
        previous_p_change = _p_change
        previous_open = _open
    return True
