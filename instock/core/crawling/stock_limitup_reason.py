#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2025/2/26 12:18
Desc: 同花顺涨停原因
http://zx.10jqka.com.cn/event/api/getharden/date/2025-02-21/orderby/date/orderway/desc/charset/GBK/
"""

import pandas as pd
import requests
import re
import numpy as np
from instock.core.singleton_proxy import proxys

__author__ = 'myh '
__date__ = '2025/5/9 '

def stock_limitup_reason(date: str = "2025-02-27") -> pd.DataFrame:
    """
    同花顺涨停原因
    http://zx.10jqka.com.cn/event/api/getharden/date/2025-02-27/orderby/date/orderway/desc/charset/GBK/
    :return: 涨停原因
    :rtype: pandas.DataFrame
    """
    url = f"http://zx.10jqka.com.cn/event/api/getharden/date/{date}/orderby/date/orderway/desc/charset/GBK/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 Thx"
    }
    r = requests.get(url, proxies = proxys().get_proxies(), headers=headers)
    data_json = r.json()

    data = data_json["data"]
    if not data:
        return pd.DataFrame()

    temp_df = pd.DataFrame(data)
    if len(temp_df.columns)<7:
        temp_df.columns = [
            "ID",
            "名称",
            "代码",
            "原因",
            "日期",
            "_",
        ]
        temp_df["最新价"] = np.nan
        temp_df["涨跌额"] = np.nan
        temp_df["涨跌幅"] = np.nan
        temp_df["换手率"] = np.nan
        temp_df["成交额"] = np.nan
        temp_df["成交量"] = np.nan
        temp_df["DDE"] = np.nan
    else:
        temp_df.columns = [
            "ID",
            "名称",
            "代码",
            "原因",
            "日期",
            "最新价",
            "涨跌额",
            "涨跌幅",
            "换手率",
            "成交额",
            "成交量",
            "DDE",
            "_",
        ]

    temp_df["详因"] = temp_df.apply(stock_limitup_detail, axis=1)
    temp_df["换手率"] = round(temp_df["换手率"], 2)
    temp_df = temp_df[
        [
            "日期",
            "代码",
            "名称",
            "原因",
            "详因",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "换手率",
            "成交量",
            "成交额",
            "DDE",
        ]
    ]

    return temp_df


def stock_limitup_detail(row):
    """
    同花顺涨停详因
    http://zx.10jqka.com.cn/event/harden/stockreason/id/70870005
    :return: 涨停详因
    :rtype: pandas.DataFrame
    """
    url = f"http://zx.10jqka.com.cn/event/harden/stockreason/id/{row['ID']}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
    }
    r = requests.get(url, proxies = proxys().get_proxies(), headers=headers)
    data_text = r.text

    # match_title = re.search(r"var title = '(.*?)';", data_text)
    # _title = ""
    # if match_title:
    #     _title = match_title.group(1)

    pattern_data = re.search(r"var data = '(.*?)';", data_text)
    _data = ""
    if pattern_data:
        _data = pattern_data.group(1).replace("&lt;spanclass=&quot;hl&quot;&gt;", "").replace("&lt;/span&gt;", "").replace("&amp;quot;", "\"")
    return _data

    # reason = f"{_title}\r\n{_data}"

    # return reason

if __name__ == "__main__":
    stock_limitup_reason_df = stock_limitup_reason()
    print(stock_limitup_reason_df)
