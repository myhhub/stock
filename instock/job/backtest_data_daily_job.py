#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
import datetime

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.lib.database_factory import get_database, execute_sql, insert_db_from_df, read_sql_to_df
import instock.core.backtest.rate_stats as rate
from instock.core.singleton_stock import stock_hist_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票策略回归测试。
def main():
    tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
    tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
    backtest_columns.insert(0, 'code')
    backtest_columns.insert(0, 'date')
    backtest_column = backtest_columns

    stocks_data = stock_hist_data().get_data()
    if stocks_data is None:
        return
    for k in stocks_data:
        date = k[0]
        break
    # 回归测试表
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     for table in tables:
    #         executor.submit(process, table, stocks_data, date, backtest_column)

    # TODO 当前的表是单例，后续改成多线程
    for table in tables:
        process(table, stocks_data, date, backtest_column)
def process(table, data_all, date, backtest_column):
    table_name = table['name']
    if not mdb.checkTableIsExist(table_name):
        return

    column_tail = tuple(table['columns'])[-1]
    now_date = datetime.datetime.now().date()
    sql = f"SELECT * FROM `{table_name}` WHERE `date` < '{now_date}' AND `{column_tail}` is NULL"
    try:
        data = read_sql_to_df(sql)
        if data is None or len(data.index) == 0:
            return

        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        # subset['date'] = subset['date'].values.astype('str')
        subset = subset.astype({'date': 'string'})
        stocks = [tuple(x) for x in subset.values]

        results = run_check(stocks, data_all, date, backtest_column)
        if results is None:
            return

        data_new = pd.DataFrame(results.values())
        update_db_from_df(data_new, table_name, ('date', 'code'))

    except Exception as e:
        logging.error(f"backtest_data_daily_job.process处理异常：{table}表{e}")

def update_db_from_df(data, table_name, where):
    data = data.where(data.notnull(), None)
    update_string = f'UPDATE `{table_name}` set '
    where_string = ' where '
    cols = tuple(data.columns)
    db = get_database()
    try:
        for row in data.values:
            sql = update_string
            sql_where = where_string
            for index, col in enumerate(cols):
                if col in where:
                    if len(sql_where) == len(where_string):
                        if type(row[index]) == str:
                            sql_where = f'''{sql_where}`{col}` = '{row[index]}' '''
                        else:
                            sql_where = f'''{sql_where}`{col}` = {row[index]} '''
                    else:
                        if type(row[index]) == str:
                            sql_where = f'''{sql_where} and `{col}` = '{row[index]}' '''
                        else:
                            sql_where = f'''{sql_where} and `{col}` = {row[index]} '''
                else:
                    if type(row[index]) == str:
                        if row[index] is None or row[index] != row[index]:
                            sql = f'''{sql}`{col}` = NULL, '''
                        else:
                            sql = f'''{sql}`{col}` = '{row[index]}', '''
                    else:
                        if row[index] is None or row[index] != row[index]:
                            sql = f'''{sql}`{col}` = NULL, '''
                        else:
                            sql = f'''{sql}`{col}` = {row[index]}, '''
            sql = f'{sql[:-2]}{sql_where}'
            execute_sql(sql, db)
    except Exception as e:
        logging.error(f"database.update_db_from_df处理异常：{sql}{e}")

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
                    logging.error(f"backtest_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
    except Exception as e:
        logging.error(f"backtest_data_daily_job.run_check处理异常：{e}")
    if not data:
        return None
    else:
        return data

# main函数入口
if __name__ == '__main__':
    main()