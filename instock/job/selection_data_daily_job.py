#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import pandas as pd
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

from instock.lib.logger import setup_logger
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf

# 配置日志
log_file = os.path.join(cpath_current, 'log', 'selection_data_daily_job.log')
logger = setup_logger('selection_data_daily_job', log_file)

__author__ = 'myh '
__date__ = '2023/5/5 '


def save_nph_stock_selection_data(date, before=True):
    if before:
        logger.info(f"跳过股票选择数据保存（before=True），日期：{date}")
        return

    logger.info(f"开始保存股票选择数据，日期：{date}")
    try:
        data = stf.fetch_stock_selection()
        if data is None:
            logger.warning(f"股票选择数据为空，无法保存，日期：{date}")
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
        logger.info(f"股票选择数据保存成功，共保存 {len(data)} 条数据，日期：{date}")
    except Exception as e:
        logger.error(f"save_nph_stock_selection_data处理异常：{e}")


def main():
    logger.info("开始执行股票选择数据每日任务")
    runt.run_with_args(save_nph_stock_selection_data)
    logger.info("股票选择数据每日任务执行完成")


# main函数入口
if __name__ == '__main__':
    main()
