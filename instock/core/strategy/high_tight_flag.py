#!/usr/local/bin/python
# -*- coding: utf-8 -*-


__author__ = 'myh '
__date__ = '2023/3/10 '


# 高而窄的旗形
# 1.必须至少上市交易60日
# 2.当日收盘价/之前24~10日的最低价>=1.9
# 3.之前24~10日必须连续两天涨幅大于等于9.5%
def check_high_tight(code_name, data, date=None, threshold=60, istop=False):
    # 龙虎榜上必须有机构
    if not istop:
        return False
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

    data = data.tail(n=24)
    data = data.head(n=14)
    low = data['low'].values.min()
    ratio_increase = data.iloc[-1]['high'] / low
    if ratio_increase < 1.9:
        return False

    # 连续两天涨幅大于等于10%
    previous_p_change = 0.0
    for _p_change in data['p_change'].values:
        # 单日跌幅超7%；高开低走7%；两日累计跌幅10%；两日高开低走累计10%
        if _p_change >= 9.5:
            if previous_p_change >= 9.5:
                return True
            else:
                previous_p_change = _p_change
        else:
            previous_p_change = 0.0

    return False
