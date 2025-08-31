#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import pandas as pd
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.lib.database_factory import get_database, execute_sql, insert_db_from_df
from instock.core.singleton_stock import stock_hist_data
from instock.core.stockfetch import fetch_stock_top_entity_data
from instock.lib.common_check import check_and_delete_old_data_for_realtime_data
__author__ = 'myh '
__date__ = '2023/3/10 '


def prepare(date, strategy, stocks_data=None):
    """
    准备策略所需的股票数据
    :param date: 交易日期，由run_template自动传递
    :param strategy: 策略字典，也就是主函数传递的参数
    :param stocks_data: 从数据库获取的股票数据(批量策略计算时建议传递，减少重复查询)
    """
    try:
        if stocks_data is None:
            stocks_data = stock_hist_data(date=date).get_data()
        print(f"策略{strategy['cn']}处理时间{date}股票数{0 if stocks_data is None else len(stocks_data)}")
        if stocks_data is None:
            return
        table_name = strategy['name']
        strategy_func = strategy['func']
        results = run_check(strategy_func, table_name, stocks_data, date)
        print(f"{strategy['cn']}策略选股结果{results}")
        if results is None:
            return

        data = pd.DataFrame(results)
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        data.columns = columns
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        date_str = pd.to_datetime(date)
        if date_str != data.iloc[0]['date']:
            data['date'] = date_str
        # 更新数据
        check_and_delete_old_data_for_realtime_data(strategy, data, date)

    except Exception as e:
        logging.exception(f"strategy_data_daily_job.prepare处理异常：{strategy}策略{e}")


def run_check(strategy_fun, table_name, stocks, date, workers=40):
    is_check_high_tight = False
    if strategy_fun.__name__ == 'check_high_tight':
        stock_tops = fetch_stock_top_entity_data(date)
        if stock_tops is not None:
            is_check_high_tight = True
    data = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            if is_check_high_tight:
                future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date, istop=(k[1] in stock_tops)): k for k in stocks}
            else:
                future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date): k for k in stocks}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    if future.result():
                        data.append(stock)
                except Exception as e:
                    logging.error(f"strategy_data_daily_job.run_check处理异常：{stock[1]}代码{e}策略{table_name}")
    except Exception as e:
        logging.exception(f"strategy_data_daily_job.run_check处理异常：{e}策略{table_name}")
    if not data:
        return None
    else:
        return data


def main():
    # 使用方法传递。
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
    #         executor.submit(runt.run_with_args, prepare, strategy)
    stocks_data = stock_hist_data().get_data()
    for strategy in tbs.TABLE_CN_STOCK_STRATEGIES[1:]:
        runt.run_with_args(prepare, strategy, stocks_data)
        import pdb; pdb.set_trace()
        print("debug")
    # runt.run_with_args(prepare, tbs.TABLE_CN_STOCK_STRATEGIES[2], stocks_data)
    # runt.run_with_args(prepare, tbs.TABLE_CN_STOCK_STRATEGIES[3], stocks_data)
    # runt.run_with_args(prepare, tbs.TABLE_CN_STOCK_STRATEGIES[1])

# main函数入口
if __name__ == '__main__':
    main()
