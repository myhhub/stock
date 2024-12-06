# -*- coding:utf-8 -*-
# !/usr/bin/env python

import math
import pandas as pd
import requests
import instock.core.tablestructure as tbs

__author__ = 'myh '
__date__ = '2023/5/9 '


def stock_selection() -> pd.DataFrame:
    """
    东方财富网-个股-选股器
    https://data.eastmoney.com/xuangu/
    :return: 选股器
    :rtype: pandas.DataFrame
    """
    cols = tbs.TABLE_CN_STOCK_SELECTION['columns']
    page_size = 1000
    page_current = 1
    sty = ""  # 初始值 "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE"
    for k in cols:
        sty = f"{sty},{cols[k]['map']}"
    url = "https://data.eastmoney.com/dataapi/xuangu/list"
    params = {
        "sty": sty[1:],
        "filter": "(MARKET+in+(\"上交所主板\",\"深交所主板\",\"深交所创业板\"))(NEW_PRICE>0)",
        "p": page_current,
        "ps": page_size,
        "source": "SELECT_SECURITIES",
        "client": "WEB"
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    data = data_json["result"]["data"]
    if not data:
        return pd.DataFrame()

    data_count = data_json["result"]["count"]
    page_count = math.ceil(data_count/page_size)
    while page_count > 1:
        page_current = page_current + 1
        params["p"] = page_current
        r = requests.get(url, params=params)
        data_json = r.json()
        _data = data_json["result"]["data"]
        data.extend(_data)
        page_count =page_count - 1

    temp_df = pd.DataFrame(data)

    mask = ~temp_df['CONCEPT'].isna()
    temp_df.loc[mask, 'CONCEPT'] = temp_df.loc[mask, 'CONCEPT'].apply(lambda x: ', '.join(x))
    mask = ~temp_df['STYLE'].isna()
    temp_df.loc[mask, 'STYLE'] = temp_df.loc[mask, 'STYLE'].apply(lambda x: ', '.join(x))

    for k in cols:
        t = tbs.get_field_type_name(cols[k]["type"])
        if t == 'numeric':
            temp_df[cols[k]["map"]] = pd.to_numeric(temp_df[cols[k]["map"]], errors="coerce")
        elif t == 'datetime':
            temp_df[cols[k]["map"]] = pd.to_datetime(temp_df[cols[k]["map"]], errors="coerce").dt.date

    return temp_df


def stock_selection_params():
    """
    东方财富网-个股-选股器-选股指标
    https://data.eastmoney.com/xuangu/
    :return: 选股器-选股指标
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/wstock/selection/api/data/get"
    params = {
        "type": "RPTA_PCNEW_WHOLE",
        "sty": "ALL",
        "p": 1,
        "ps": 50000,
        "source": "SELECT_SECURITIES",
        "client": "WEB"
    }

    r = requests.get(url, params=params)
    data_json = r.json()
    zxzb = data_json["zxzb"]  # 指标
    print(zxzb)


if __name__ == "__main__":
    stock_selection_df = stock_selection()
    print(stock_selection)
