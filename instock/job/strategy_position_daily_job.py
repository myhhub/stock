#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import concurrent.futures
import logging
import pandas as pd
import os.path
import sys
import datetime

from instock.core.stockfetch import fetch_stock_top_entity_data

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.lib.run_template as runt
from instock.core.singleton_stock import stock_hist_data

__author__ = 'your_name'
__date__ = '2023/current_date'


def prepare(date, strategy):
    try:
        if date is None:
            date = datetime.date.today().strftime("%Y-%m-%d")
        # 获取当前持仓
        positionTableName = tbs.TABLE_CN_STOCK_POSITION['name']
        if not mdb.checkTableIsExist(positionTableName):
            logging.info(f"Table {positionTableName} does not exist. Creating it.")
            pdata = pd.DataFrame(data=[['2024-12-09', '603078', '江化微', 18.775,1000, 18.560,18560.0, 0]] , columns=list(tbs.TABLE_CN_STOCK_POSITION['columns']))
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_POSITION['columns'])
            mdb.insert_db_from_df(pdata, positionTableName, cols_type, False, "`date`,`code`")
            del_sql = f"DELETE FROM `{positionTableName}`"
            mdb.executeSql(del_sql)
            return
        # 获取当前持仓
        result = mdb.executeSqlFetch(f"SELECT * FROM `{positionTableName}`")


        if result is None:
            logging.info("No positions found.")
            return
        pd_result = pd.DataFrame(result, columns=list(tbs.TABLE_CN_STOCK_POSITION['columns']))
        # 获取持仓股票代码列表
        stocks = [(x[0].strftime('%Y-%m-%d'),) + x[1:] for x in pd_result[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])].itertuples(index=False, name=None)]

        # 获取历史数据
        stocks_data = stock_hist_data(date=date, stocks=stocks).get_data()

        if stocks_data is None:
            logging.error("Failed to get historical data.")
            return

        sell_list = []

        strategy_name = strategy['name']
        strategy_func = strategy['func']
        # 计算成本
        cost_dict = {row['code']: row['cost_price'] for _, row in pd_result.iterrows()}

        results = run_check(strategy_func, strategy_name, stocks_data, date, cost_dict)
        # 遍历结果
        if results is None:
            return

        table_name = strategy['name']
        if mdb.checkTableIsExist(table_name):
            # 删除当日旧数据
            del_sql = f"DELETE FROM `{table_name}` WHERE `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.DataFrame(results)
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        data.columns = columns
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        # 插入新数据
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")

        logging.info(f"Added {len(sell_list)} stocks to sell list.")

    except Exception as e:
        logging.error(f"Error in check_position_and_sell: {e}")
def run_check(strategy_fun, strategy_name, stocks, date, code_price = None, workers=40):

    data = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date, cost=code_price.get(k[1])): k for k in stocks}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    if future.result():
                        data.append(stock)
                except Exception as e:
                    logging.error(f"strategy_data_daily_job.run_check处理异常：{stock[1]}代码{e}策略{strategy_name}")
    except Exception as e:
        logging.error(f"strategy_data_daily_job.run_check处理异常：策略{strategy_name}", e)
    if not data:
        return None
    else:
        return data


def main():
    # 使用方法传递。
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for strategy in tbs.TABLE_CN_STOCK_POSITION_CHECK:
            executor.submit(runt.run_with_args, prepare, strategy)



if __name__ == '__main__':
    main()
