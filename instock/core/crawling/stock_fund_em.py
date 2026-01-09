#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2023/5/16 15:30
Desc: 东方财富网-数据中心-资金流向
https://data.eastmoney.com/zjlx/detail.html
"""
import json
import random
import time
import math
import pandas as pd
from instock.core.eastmoney_fetcher import eastmoney_fetcher

__author__ = 'myh '
__date__ = '2025/12/31 '

# 创建全局实例，供所有函数使用
fetcher = eastmoney_fetcher()

def stock_individual_fund_flow_rank(indicator: str = "5日") -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-排名
    https://data.eastmoney.com/zjlx/detail.html
    :param indicator: choice of {"今日", "3日", "5日", "10日"}
    :type indicator: str
    :return: 指定 indicator 资金流向排行
    :rtype: pandas.DataFrame
    """
    indicator_map = {
        "今日": [
            "f62",
            "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",
        ],
        "3日": [
            "f267",
            "f12,f14,f2,f127,f267,f268,f269,f270,f271,f272,f273,f274,f275,f276,f257,f258,f124",
        ],
        "5日": [
            "f164",
            "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124",
        ],
        "10日": [
            "f174",
            "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124",
        ],
    }
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    page_current = 1
    params = {
        "fid": indicator_map[indicator][0],
        "po": "1",
        "pz": page_size,
        "pn": page_current,
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "fs": "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
        "fields": indicator_map[indicator][1],
    }
    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    data = data_json["data"]["diff"]
    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    while page_count > 1:
        # 添加随机延迟，避免爬取过快
        time.sleep(random.uniform(1, 1.5))
        page_current = page_current + 1
        params["pn"] = page_current
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        _data = data_json["data"]["diff"]
        data.extend(_data)
        page_count =page_count - 1

    temp_df = pd.DataFrame(data)
    temp_df = temp_df[~temp_df["f2"].isin(["-"])]
    if indicator == "今日":
        temp_df.columns = [
            "最新价",
            "今日涨跌幅",
            "代码",
            "名称",
            "今日主力净流入-净额",
            "今日超大单净流入-净额",
            "今日超大单净流入-净占比",
            "今日大单净流入-净额",
            "今日大单净流入-净占比",
            "今日中单净流入-净额",
            "今日中单净流入-净占比",
            "今日小单净流入-净额",
            "今日小单净流入-净占比",
            "_",
            "今日主力净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "今日涨跌幅",
                "今日主力净流入-净额",
                "今日主力净流入-净占比",
                "今日超大单净流入-净额",
                "今日超大单净流入-净占比",
                "今日大单净流入-净额",
                "今日大单净流入-净占比",
                "今日中单净流入-净额",
                "今日中单净流入-净占比",
                "今日小单净流入-净额",
                "今日小单净流入-净占比",
            ]
        ]
    elif indicator == "3日":
        temp_df.columns = [
            "最新价",
            "代码",
            "名称",
            "_",
            "3日涨跌幅",
            "_",
            "_",
            "_",
            "3日主力净流入-净额",
            "3日主力净流入-净占比",
            "3日超大单净流入-净额",
            "3日超大单净流入-净占比",
            "3日大单净流入-净额",
            "3日大单净流入-净占比",
            "3日中单净流入-净额",
            "3日中单净流入-净占比",
            "3日小单净流入-净额",
            "3日小单净流入-净占比",
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "3日涨跌幅",
                "3日主力净流入-净额",
                "3日主力净流入-净占比",
                "3日超大单净流入-净额",
                "3日超大单净流入-净占比",
                "3日大单净流入-净额",
                "3日大单净流入-净占比",
                "3日中单净流入-净额",
                "3日中单净流入-净占比",
                "3日小单净流入-净额",
                "3日小单净流入-净占比",
            ]
        ]
    elif indicator == "5日":
        temp_df.columns = [
            "最新价",
            "代码",
            "名称",
            "5日涨跌幅",
            "_",
            "5日主力净流入-净额",
            "5日主力净流入-净占比",
            "5日超大单净流入-净额",
            "5日超大单净流入-净占比",
            "5日大单净流入-净额",
            "5日大单净流入-净占比",
            "5日中单净流入-净额",
            "5日中单净流入-净占比",
            "5日小单净流入-净额",
            "5日小单净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "5日涨跌幅",
                "5日主力净流入-净额",
                "5日主力净流入-净占比",
                "5日超大单净流入-净额",
                "5日超大单净流入-净占比",
                "5日大单净流入-净额",
                "5日大单净流入-净占比",
                "5日中单净流入-净额",
                "5日中单净流入-净占比",
                "5日小单净流入-净额",
                "5日小单净流入-净占比",
            ]
        ]
    elif indicator == "10日":
        temp_df.columns = [
            "最新价",
            "代码",
            "名称",
            "_",
            "10日涨跌幅",
            "10日主力净流入-净额",
            "10日主力净流入-净占比",
            "10日超大单净流入-净额",
            "10日超大单净流入-净占比",
            "10日大单净流入-净额",
            "10日大单净流入-净占比",
            "10日中单净流入-净额",
            "10日中单净流入-净占比",
            "10日小单净流入-净额",
            "10日小单净流入-净占比",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "10日涨跌幅",
                "10日主力净流入-净额",
                "10日主力净流入-净占比",
                "10日超大单净流入-净额",
                "10日超大单净流入-净占比",
                "10日大单净流入-净额",
                "10日大单净流入-净占比",
                "10日中单净流入-净额",
                "10日中单净流入-净占比",
                "10日小单净流入-净额",
                "10日小单净流入-净占比",
            ]
        ]
        # temp_df['最新价'] = pd.to_numeric(temp_df['最新价'], errors="coerce").fillna(0)
    return temp_df


def stock_sector_fund_flow_rank(
    indicator: str = "10日", sector_type: str = "行业资金流"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-板块资金流-排名
    https://data.eastmoney.com/bkzj/hy.html
    :param indicator: choice of {"今日", "5日", "10日"}
    :type indicator: str
    :param sector_type: choice of {"行业资金流", "概念资金流", "地域资金流"}
    :type sector_type: str
    :return: 指定参数的资金流排名数据
    :rtype: pandas.DataFrame
    """
    sector_type_map = {"行业资金流": "2", "概念资金流": "3", "地域资金流": "1"}
    indicator_map = {
        "今日": [
            "f62",
            "1",
            "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",
        ],
        "5日": [
            "f164",
            "5",
            "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124",
        ],
        "10日": [
            "f174",
            "10",
            "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124",
        ],
    }
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "fltt": "2",
        "invt": "2",
        "fid0": indicator_map[indicator][0],
        "fs": f"m:90 t:{sector_type_map[sector_type]}",
        "stat": indicator_map[indicator][1],
        "fields": indicator_map[indicator][2],
        "rt": "52975239",
        "cb": "jQuery18308357908311220152_1589256588824",
        "_": int(time.time() * 1000),
    }
    r = fetcher.make_request(url, params=params)
    text_data = r.text
    data_json = json.loads(text_data[text_data.find("{") : -2])
    data = data_json["data"]["diff"]

    data_count = data_json["data"]["total"]
    page_count = math.ceil(data_count/page_size)
    while page_count > 1:
        # 添加随机延迟，避免爬取过快
        time.sleep(random.uniform(1, 1.5))
        page_current = page_current + 1
        params["pn"] = page_current
        r = fetcher.make_request(url, params=params)
        text_data = r.text
        json_data = json.loads(text_data[text_data.find("{"): -2])
        _data = json_data["data"]["diff"]
        data.extend(_data)
        page_count =page_count - 1

    temp_df = pd.DataFrame(data)


    temp_df = temp_df[~temp_df["f2"].isin(["-"])]
    if indicator == "今日":
        temp_df.columns = [
            "-",
            "今日涨跌幅",
            "_",
            "名称",
            "今日主力净流入-净额",
            "今日超大单净流入-净额",
            "今日超大单净流入-净占比",
            "今日大单净流入-净额",
            "今日大单净流入-净占比",
            "今日中单净流入-净额",
            "今日中单净流入-净占比",
            "今日小单净流入-净额",
            "今日小单净流入-净占比",
            "-",
            "今日主力净流入-净占比",
            "今日主力净流入最大股",
            "今日主力净流入最大股代码",
            "是否净流入",
        ]

        temp_df = temp_df[
            [
                "名称",
                "今日涨跌幅",
                "今日主力净流入-净额",
                "今日主力净流入-净占比",
                "今日超大单净流入-净额",
                "今日超大单净流入-净占比",
                "今日大单净流入-净额",
                "今日大单净流入-净占比",
                "今日中单净流入-净额",
                "今日中单净流入-净占比",
                "今日小单净流入-净额",
                "今日小单净流入-净占比",
                "今日主力净流入最大股",
            ]
        ]
    elif indicator == "5日":
        temp_df.columns = [
            "-",
            "_",
            "名称",
            "5日涨跌幅",
            "_",
            "5日主力净流入-净额",
            "5日主力净流入-净占比",
            "5日超大单净流入-净额",
            "5日超大单净流入-净占比",
            "5日大单净流入-净额",
            "5日大单净流入-净占比",
            "5日中单净流入-净额",
            "5日中单净流入-净占比",
            "5日小单净流入-净额",
            "5日小单净流入-净占比",
            "5日主力净流入最大股",
            "_",
            "_",
        ]

        temp_df = temp_df[
            [
                "名称",
                "5日涨跌幅",
                "5日主力净流入-净额",
                "5日主力净流入-净占比",
                "5日超大单净流入-净额",
                "5日超大单净流入-净占比",
                "5日大单净流入-净额",
                "5日大单净流入-净占比",
                "5日中单净流入-净额",
                "5日中单净流入-净占比",
                "5日小单净流入-净额",
                "5日小单净流入-净占比",
                "5日主力净流入最大股",
            ]
        ]
    elif indicator == "10日":
        temp_df.columns = [
            "-",
            "_",
            "名称",
            "_",
            "10日涨跌幅",
            "10日主力净流入-净额",
            "10日主力净流入-净占比",
            "10日超大单净流入-净额",
            "10日超大单净流入-净占比",
            "10日大单净流入-净额",
            "10日大单净流入-净占比",
            "10日中单净流入-净额",
            "10日中单净流入-净占比",
            "10日小单净流入-净额",
            "10日小单净流入-净占比",
            "10日主力净流入最大股",
            "_",
            "_",
        ]

        temp_df = temp_df[
            [
                "名称",
                "10日涨跌幅",
                "10日主力净流入-净额",
                "10日主力净流入-净占比",
                "10日超大单净流入-净额",
                "10日超大单净流入-净占比",
                "10日大单净流入-净额",
                "10日大单净流入-净占比",
                "10日中单净流入-净额",
                "10日中单净流入-净占比",
                "10日小单净流入-净额",
                "10日小单净流入-净占比",
                "10日主力净流入最大股",
            ]
        ]

    return temp_df


if __name__ == "__main__":
    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(indicator="今日")
    print(stock_individual_fund_flow_rank_df)

    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(indicator="3日")
    print(stock_individual_fund_flow_rank_df)

    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(indicator="5日")
    print(stock_individual_fund_flow_rank_df)

    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(
        indicator="10日"
    )
    print(stock_individual_fund_flow_rank_df)

    stock_sector_fund_flow_rank_df = stock_sector_fund_flow_rank(
        indicator="5日", sector_type="概念资金流"
    )
    print(stock_sector_fund_flow_rank_df)

    stock_sector_fund_flow_rank_df = stock_sector_fund_flow_rank(
        indicator="今日", sector_type="行业资金流"
    )
    print(stock_sector_fund_flow_rank_df)
