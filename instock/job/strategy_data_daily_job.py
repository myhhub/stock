#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import concurrent.futures
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
from instock.core.singleton_stock import stock_hist_data
from instock.core.stockfetch import fetch_stock_top_entity_data

# 配置日志
log_file = os.path.join(cpath_current, 'log', 'strategy_data_daily_job.log')
logger = setup_logger('strategy_data_daily_job', log_file)

__author__ = 'myh '
__date__ = '2023/3/10 '


def prepare(date, strategy):
    logger.info(f"开始准备股票策略数据，策略：{strategy['name']}，日期：{date}")
    try:
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            logger.warning(f"股票历史数据为空，无法准备策略数据，策略：{strategy['name']}，日期：{date}")
            return
        table_name = strategy['name']
        strategy_func = strategy['func']
        results = run_check(strategy_func, table_name, stocks_data, date)
        if results is None:
            logger.warning(f"股票策略数据计算结果为空，策略：{strategy['name']}，日期：{date}")
            return

        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES[0]['columns'])

        data = pd.DataFrame(results)
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        data.columns = columns
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logger.info(f"股票策略数据保存成功，策略：{strategy['name']}，日期：{date}")

    except Exception as e:
        logger.error(f"prepare处理异常：{strategy}策略{e}")


def run_check(strategy_fun, table_name, stocks, date, workers=40):
    is_check_high_tight = False
    if strategy_fun.__name__ == 'check_high_tight':
        stock_tops = fetch_stock_top_entity_data(date)
        if stock_tops is not None:
            is_check_high_tight = True
    data = []
    logger.info(f"开始运行检查股票策略，策略：{table_name}，共 {len(stocks)} 只股票需要检查")
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
                    logger.error(f"run_check处理异常：{stock[1]}代码{e}策略{table_name}")
    except Exception as e:
        logger.error(f"run_check处理异常：{e}策略{table_name}")
    if not data:
        logger.warning(f"股票策略检查结果为空，策略：{table_name}")
        return None
    else:
        logger.info(f"股票策略检查完成，策略：{table_name}，共获得 {len(data)} 条有效结果")
        return data


def main():
    logger.info("开始执行股票策略数据每日任务")
    # 使用方法传递。
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
            executor.submit(runt.run_with_args, prepare, strategy)
    logger.info("股票策略数据每日任务执行完成")


# main函数入口
if __name__ == '__main__':
    main()
