# -*- coding:utf-8 -*-
# !/usr/bin/env python

import pandas as pd
import requests
import instock.core.tablestructure as tbs

__author__ = 'myh '
__date__ = '2023/5/7 '



def stock_cpbd_em(symbol: str = "688041") -> pd.DataFrame:
    """
    东方财富网-个股-操盘必读
    https://emweb.securities.eastmoney.com/PC_HSF10/OperationsRequired/Index?type=web&code=SH688041#
    :param symbol: 带市场标识的股票代码
    :type symbol: str
    :return: 操盘必读
    :rtype: pandas.DataFrame
    """
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/OperationsRequired/PageAjax"
    if symbol.startswith("6"):
        symbol = f"SH{symbol}"
    else:
        symbol = f"SZ{symbol}"
    params = {"code": symbol}

    r = requests.get(url, params=params)
    data_json = r.json()
    zxzb = data_json["zxzb"]  # 主要指标
    if len(zxzb) < 1:
        return None

    data_dict = zxzb[0]
    zxzbOther = data_json["zxzbOther"]  # 其它指标,计算出来
    if len(zxzbOther) > 0:
        zxzbOther = zxzbOther[0]
        data_dict = {**data_dict, **zxzbOther}

    # zxzbhq = data_json["zxzbhq"]  # 其它指标,计算出来
    # if len(zxzbhq) > 0:
    #     data_dict = {**data_dict, **zxzbhq}

    _ssbks = data_json["ssbk"]  # 所属板块
    ssbk = None
    for s in _ssbks:
        _v = s.get('BOARD_NAME')
        if _v is not None:
            if ssbk is None:
                ssbk = f"{_v}"
            else:
                ssbk = f"{ssbk}、{_v}"
    data_dict["BOARD_NAME"] = ssbk

    gdrs = data_json["gdrs"]  # 股东分析
    if len(gdrs) > 0:
        gdrs = gdrs[0]
        data_dict = {**data_dict, **gdrs}

    lhbd = data_json["lhbd"]  # 龙虎榜单
    if len(lhbd) > 0:
        lhbd = lhbd[0]
        lhbd["LHBD_DATE"] = lhbd.pop("TRADE_DATE")
        data_dict = {**data_dict, **lhbd}

    dzjy = data_json["dzjy"]  # 大宗交易
    if len(dzjy) > 0:
        dzjy = dzjy[0]
        dzjy["DZJY_DATE"] = dzjy.pop("TRADE_DATE")
        data_dict = {**data_dict, **dzjy}

    rzrq = data_json["rzrq"]  # 融资融券
    if len(rzrq) > 0:
        rzrq = rzrq[0]
        rzrq["RZRQ_DATE"] = rzrq.pop("TRADE_DATE")
        data_dict = {**data_dict, **rzrq}

    tbs.CN_STOCK_CPBD


    # temp_df["报告期"] = pd.to_datetime(temp_df["报告期"], errors="coerce").dt.date
    # temp_df["每股收益"] = pd.to_numeric(temp_df["每股收益"], errors="coerce")
    # temp_df["每股净资产"] = pd.to_numeric(temp_df["每股净资产"], errors="coerce")
    # temp_df["每股经营现金流"] = pd.to_numeric(temp_df["每股经营现金流"], errors="coerce")
    # temp_df["每股公积金"] = pd.to_numeric(temp_df["每股公积金"], errors="coerce")
    # temp_df["每股未分配利润"] = pd.to_numeric(temp_df["每股未分配利润"], errors="coerce")
    # temp_df["加权净资产收益率"] = pd.to_numeric(temp_df["加权净资产收益率"], errors="coerce")
    # temp_df["毛利率"] = pd.to_numeric(temp_df["毛利率"], errors="coerce")
    # temp_df["资产负债率"] = pd.to_numeric(temp_df["资产负债率"], errors="coerce")
    # temp_df["营业收入"] = pd.to_numeric(temp_df["营业收入"], errors="coerce")
    # temp_df["营业收入滚动环比增长"] = pd.to_numeric(temp_df["营业收入同比增长"], errors="coerce")
    # temp_df["营业收入同比增长"] = pd.to_numeric(temp_df["营业收入同比增长"], errors="coerce")
    # temp_df["归属净利润"] = pd.to_numeric(temp_df["归属净利润"], errors="coerce")
    # temp_df["归属净利润滚动环比增长"] = pd.to_numeric(temp_df["归属净利润滚动环比增长"], errors="coerce")
    # temp_df["归属净利润同比增长"] = pd.to_numeric(temp_df["归属净利润同比增长"], errors="coerce")
    # temp_df["扣非净利润"] = pd.to_numeric(temp_df["归属净利润"], errors="coerce")
    # temp_df["扣非净利润滚动环比增长"] = pd.to_numeric(temp_df["扣非净利润滚动环比增长"], errors="coerce")
    # temp_df["扣非净利润同比增长"] = pd.to_numeric(temp_df["扣非净利润同比增长"], errors="coerce")
    # temp_df["总股本"] = pd.to_numeric(temp_df["总股本"], errors="coerce")
    # temp_df["已流通股份"] = pd.to_numeric(temp_df["已流通股份"], errors="coerce")


def stock_zjlx_em(symbol: str = "688041") -> pd.DataFrame:
    """
    东方财富网-个股-资金流向
    https://data.eastmoney.com/zjlx/688041.html
    :param symbol: 带市场标识的股票代码
    :type symbol: str
    :return: 操盘必读
    :rtype: pandas.DataFrame
    """
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    if symbol.startswith("6"):
        symbol = f"1.{symbol}"
    else:
        symbol = f"0.{symbol}"
    params = {
        "lmt": "0",
        "klt": "1",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": symbol
    }

    r = requests.get(url, params=params)
    data_json = r.json()
    klines = data_json["klines"]  # 主要指标
    "日期","主力净流入额","小单净流入额","中单净流入额","大单净流入额","超大单净流入额","主力净流入占比", "小单净流入占比", "中单净流入占比", "大单净流入占比", "超大单净流入占比"
    "收盘价","涨跌幅"
    if len(klines) < 1:
        return None



if __name__ == "__main__":
    stock_cpbd_em_df = stock_cpbd_em(symbol="688041")
    print(stock_cpbd_em_df)
