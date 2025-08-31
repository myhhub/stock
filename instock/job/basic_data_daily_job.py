#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import os.path
import sys
import datetime

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.trade_time as trd
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.lib.database_factory import get_database, execute_sql, insert_db_from_df
from instock.lib.common_check import check_and_delete_old_data_for_realtime_data
import instock.core.stockfetch as stf
from instock.core.singleton_stock import stock_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票实时行情数据。
def save_nph_stock_spot_data(date, before=True):
    if before:
        return
    # 股票列表
    try:
        data = stock_data(date).get_data()
        if data is None or len(data.index) == 0:
            return
        check_and_delete_old_data_for_realtime_data(tbs.TABLE_CN_STOCK_SPOT, data, date)
    except Exception as e:
        logging.error(f"basic_data_daily_job.save_stock_spot_data处理异常：{e}")


# 基金实时行情数据。
def save_nph_etf_spot_data(date, before=True):
    if before:
        return
    # 股票列表
    try:
        data = stf.fetch_etfs(date)
        if data is None or len(data.index) == 0:
            return
        check_and_delete_old_data_for_realtime_data(tbs.TABLE_CN_ETF_SPOT, data, date)
    except Exception as e:
        logging.error(f"basic_data_daily_job.save_nph_etf_spot_data处理异常：{e}")



def main():
    if not trd.is_market_close():
        runt.run_with_args(save_nph_stock_spot_data)
        runt.run_with_args(save_nph_etf_spot_data)


# main函数入口
if __name__ == '__main__':
    main()
