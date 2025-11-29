#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import pandas as pd
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)
# 配置日志，同时输出到控制台和文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_path, 'stock_selection_data_daily_job.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
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
        data = stf.fetch_stock_selection(date)
        if data is None:
            return

        table_name = tbs.TABLE_CN_STOCK_SELECTION['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            # 使用当前日期删除老数据
            _date = date.strftime("%Y-%m-%d")
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
