#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import pandas as pd
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf
from instock.core.singleton_stock import stock_data

__author__ = 'myh '
__date__ = '2023/5/5 '


def save_nph_stock_fundamentals_data(date, before=True):
    if before:
        return

    try:
        dataVal = stf.fetch_stocks_financial_indicator()
        if dataVal is None:
            return

        _subset = stock_data(date).get_data()[list(tbs.CN_STOCK_SPOT_FOREIGN_KEY['columns'])]
        stocks = [tuple(x) for x in _subset.values]
        if stocks is None:
            return

        table_name = tbs.TABLE_CN_STOCK_FUNDAMENTALS['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUNDAMENTALS['columns'])

        dataKey = pd.DataFrame(stocks)
        _columns = tuple(tbs.CN_STOCK_SPOT_FOREIGN_KEY['columns'])
        dataKey.columns = _columns

        dataVal.drop('index', axis=1, inplace=True)  # 删除下标
        dataVal.drop('name', axis=1, inplace=True)  # 删除名称

        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        # data.set_index('code', inplace=True)
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")

    except Exception as e:
        logging.error(f"fundamentals_data_daily_job.save_nph_stock_fundamentals_data处理异常：{e}")


def main():
    runt.run_with_args(save_nph_stock_fundamentals_data)


# main函数入口
if __name__ == '__main__':
    main()
