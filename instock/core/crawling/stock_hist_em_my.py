#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd

__author__ = 'myh '
__date__ = '2023/5/7 '


def stock_zh_a_spot_em() -> pd.DataFrame:
    """
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    """
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f9,f12,f14,f20,f21,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df.columns = [
        "最新价",
        "市盈率动",
        "代码",
        "名称",
        "总市值",
        "流通市值",
        "市净率",
        "60日涨跌幅",
        "年初至今涨跌幅",
        "上市时间",
        "加权净资产收益率",
        "总股本",
        "已流通股份",
        "营业收入",
        "营业收入同比增长",
        "归属净利润",
        "归属净利润同比增长",
        "每股未分配利润",
        "毛利率",
        "资产负债率",
        "每股公积金",
        "所处行业",
        "每股收益",
        "每股净资产",
        "市盈率静",
        "市盈率TTM",
        "报告期"
    ]
    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "60日涨跌幅",
            "年初至今涨跌幅",
            "市盈率动",
            "市盈率TTM",
            "市盈率静",
            "市净率",
            "每股收益",
            "每股净资产",
            "每股公积金",
            "每股未分配利润",
            "加权净资产收益率",
            "毛利率",
            "资产负债率",
            "营业收入",
            "营业收入同比增长",
            "归属净利润",
            "归属净利润同比增长",
            "报告期",
            "总股本",
            "已流通股份",
            "总市值",
            "流通市值",
            "所处行业",
            "上市时间"
        ]
    ]
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
    temp_df["60日涨跌幅"] = pd.to_numeric(temp_df["60日涨跌幅"], errors="coerce")
    temp_df["年初至今涨跌幅"] = pd.to_numeric(temp_df["年初至今涨跌幅"], errors="coerce")
    temp_df["市盈率动"] = pd.to_numeric(temp_df["市盈率动"], errors="coerce")
    temp_df["市盈率TTM"] = pd.to_numeric(temp_df["市盈率TTM"], errors="coerce")
    temp_df["市盈率静"] = pd.to_numeric(temp_df["市盈率静"], errors="coerce")
    temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
    temp_df["每股收益"] = pd.to_numeric(temp_df["每股收益"], errors="coerce")
    temp_df["每股净资产"] = pd.to_numeric(temp_df["每股净资产"], errors="coerce")
    temp_df["每股公积金"] = pd.to_numeric(temp_df["每股公积金"], errors="coerce")
    temp_df["每股未分配利润"] = pd.to_numeric(temp_df["每股未分配利润"], errors="coerce")
    temp_df["加权净资产收益率"] = pd.to_numeric(temp_df["加权净资产收益率"], errors="coerce")
    temp_df["毛利率"] = pd.to_numeric(temp_df["毛利率"], errors="coerce")
    temp_df["资产负债率"] = pd.to_numeric(temp_df["资产负债率"], errors="coerce")
    temp_df["营业收入"] = pd.to_numeric(temp_df["营业收入"], errors="coerce")
    temp_df["营业收入同比增长"] = pd.to_numeric(temp_df["营业收入同比增长"], errors="coerce")
    temp_df["归属净利润"] = pd.to_numeric(temp_df["归属净利润"], errors="coerce")
    temp_df["归属净利润同比增长"] = pd.to_numeric(temp_df["归属净利润同比增长"], errors="coerce")
    temp_df["报告期"] = pd.to_datetime(temp_df["报告期"], format='%Y%m%d', errors="coerce")
    temp_df["总股本"] = pd.to_numeric(temp_df["总股本"], errors="coerce")
    temp_df["已流通股份"] = pd.to_numeric(temp_df["已流通股份"], errors="coerce")
    temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
    temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
    temp_df["上市时间"] = pd.to_datetime(temp_df["上市时间"], format='%Y%m%d', errors="coerce")

    return temp_df
