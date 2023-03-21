#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd

import os.path
import sys
# 在项目运行时，临时将项目路径添加到环境变量
cpath = os.path.dirname(os.path.dirname(__file__))
sys.path.append(cpath)

import libs.run_template as runt
import libs.tablestructure as tbs
import libs.database as mdb
import backtest.rate_stats as rate
from libs.singleton import stock_hist_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票策略回归测试。
def prepare():
    tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
    tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'].keys())
    backtest_columns.insert(0, 'code')
    backtest_columns.insert(0, 'date')
    backtest_column = backtest_columns

    stocks_data = stock_hist_data().get_data()
    if stocks_data is None:
        return
    for k in stocks_data.keys():
        date = k[0]
        break
    # 回归测试表
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for table in tables:
            executor.submit(process, table, stocks_data, date, backtest_column)


def process(table, data_all, date, backtest_column):
    table_name = table['name']
    if not mdb.checkTableIsExist(table_name):
        return

    column_tail = list(table['columns'].keys())[-1]
    sql = "SELECT * FROM `%s` WHERE `date` < '%s' AND `%s` is NULL" % (table_name, runt.get_current_date(), column_tail)
    try:
        data = pd.read_sql(sql=sql, con=mdb.conn_not_cursor())
        if data is None or len(data.index) == 0:
            return

        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'].keys())]
        subset = subset.astype({'date': 'string'})
        stocks = [tuple(x) for x in subset.values]

        results = run_check(stocks, data_all, date, backtest_column)
        if results is None:
            return

        data_new = pd.DataFrame(results.values())
        mdb.update_db_from_df(data_new, table_name, ('date', 'code'))

    except Exception as e:
        logging.debug("{}处理异常：{}表{}".format('backtest_data_daily_job.process', table, e))


def run_check(stocks, data_all, date, backtest_column, workers=40):
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(rate.get_rates, stock,
                                              data_all.get((date, stock[1], stock[2])), backtest_column,
                                              len(backtest_column) - 1): stock for stock in stocks}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[stock] = _data_
                except Exception as e:
                    logging.debug("{}处理异常：{}代码{}".format('backtest_data_daily_job.run_get_rate', stock[1], e))
    except Exception as e:
        logging.debug("{}处理异常：{}".format('backtest_data_daily_job.run_get_rate', e))
    if not data:
        return None
    else:
        return data


def main():
    prepare()


# main函数入口
if __name__ == '__main__':
    main()
