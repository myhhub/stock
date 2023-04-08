#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import instock.core.tablestructure as tbs

__author__ = 'myh '
__date__ = '2023/3/10 '


class StockWebData:
    def __init__(self, mode, type, name, table_name, columns, column_names, primary_key, order_by):
        self.mode = mode  # 模式，query，editor 查询和编辑模式
        self.type = type
        self.name = name
        self.table_name = table_name
        self.columns = columns
        self.column_names = column_names
        self.primary_key = primary_key
        self.order_by = order_by
        if mode == "query":
            self.url = f"/stock/data?table_name={self.table_name}"
        elif mode == "editor":
            self.url = f"/data/editor?table_name={self.table_name}"


STOCK_WEB_DATA_LIST = [StockWebData(
    mode="query",
    type="1、股票基本数据",
    name=tbs.TABLE_CN_STOCK_SPOT['cn'],
    table_name=tbs.TABLE_CN_STOCK_SPOT['name'],
    columns=tuple(tbs.TABLE_CN_STOCK_SPOT['columns']),
    column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SPOT['columns'].values()),
    primary_key=[],
    order_by=" code asc "
), StockWebData(
    mode="query",
    type="1、股票基本数据",
    name=tbs.TABLE_CN_STOCK_TOP['cn'],
    table_name=tbs.TABLE_CN_STOCK_TOP['name'],
    columns=tuple(tbs.TABLE_CN_STOCK_TOP['columns']),
    column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_TOP['columns'].values()),
    primary_key=[],
    order_by=" code asc "
), StockWebData(
    mode="query",
    type="1、股票基本数据",
    name=tbs.TABLE_CN_STOCK_BLOCKTRADE['cn'],
    table_name=tbs.TABLE_CN_STOCK_BLOCKTRADE['name'],
    columns=tuple(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns']),
    column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns'].values()),
    primary_key=[],
    order_by=" code asc "
), StockWebData(
    mode="query",
    type="2、股票指标数据",
    name=tbs.TABLE_CN_STOCK_INDICATORS['cn'],
    table_name=tbs.TABLE_CN_STOCK_INDICATORS['name'],
    columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS['columns']),
    column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS['columns'].values()),
    primary_key=[],
    order_by=" code desc  "
), StockWebData(
    mode="query",
    type="2、股票指标数据",
    name=tbs.TABLE_CN_STOCK_INDICATORS_BUY['cn'],
    table_name=tbs.TABLE_CN_STOCK_INDICATORS_BUY['name'],
    columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns']),
    column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns'].values()),
    primary_key=[],
    order_by=" code desc  "
), StockWebData(
    mode="query",
    type="2、股票指标数据",
    name=tbs.TABLE_CN_STOCK_INDICATORS_SELL['cn'],
    table_name=tbs.TABLE_CN_STOCK_INDICATORS_SELL['name'],
    columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns']),
    column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns'].values()),
    primary_key=[],
    order_by=" code desc  "
), StockWebData(
    mode="query",
    type="3、股票K线形态",
    name=tbs.TABLE_CN_STOCK_KLINE_PATTERN['cn'],
    table_name=tbs.TABLE_CN_STOCK_KLINE_PATTERN['name'],
    columns=tuple(tbs.TABLE_CN_STOCK_KLINE_PATTERN['columns']),
    column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_KLINE_PATTERN['columns'].values()),
    primary_key=[],
    order_by=" code desc  "
)]

for table in tbs.TABLE_CN_STOCK_STRATEGIES:
    STOCK_WEB_DATA_LIST.append(
        StockWebData(
            mode="query",
            type="4、股票策略数据",
            name=table['cn'],
            table_name=table['name'],
            columns=tuple(table['columns']),
            column_names=tbs.get_field_cns(table['columns'].values()),
            primary_key=[],
            order_by=" code asc "
        )
    )

STOCK_WEB_DATA_MAP = {}
WEB_EASTMONEY_URL = "http://quote.eastmoney.com/%s.html"
# 再拼接成Map使用。
for tmp in STOCK_WEB_DATA_LIST:
    STOCK_WEB_DATA_MAP[tmp.table_name] = tmp
