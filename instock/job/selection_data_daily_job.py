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

__author__ = 'myh '
__date__ = '2023/5/5 '


def save_nph_stock_selection_data(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stock_selection()
        if data is None:
            return

        table_name = tbs.TABLE_CN_STOCK_SELECTION['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            _date = data.iloc[0]['date']
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{_date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SELECTION['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"selection_data_daily_job.save_nph_stock_selection_data处理异常：{e}")


def main():
    runt.run_with_args(save_nph_stock_selection_data)


# main函数入口
if __name__ == '__main__':
    main()
