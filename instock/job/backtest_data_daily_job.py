#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import concurrent.futures
import pandas as pd
import os.path
import sys
import datetime

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

from instock.lib.logger import setup_logger
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.backtest.rate_stats as rate
from instock.core.singleton_stock import stock_hist_data

# 配置日志
log_file = os.path.join(cpath_current, 'log', 'backtest_data_daily_job.log')
logger = setup_logger('backtest_data_daily_job', log_file)

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票策略回归测试。
def prepare():
    logger.info("开始准备股票策略回归测试数据")
    tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
    tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
    backtest_columns.insert(0, 'code')
    backtest_columns.insert(0, 'date')
    backtest_column = backtest_columns

    stocks_data = stock_hist_data().get_data()
    if stocks_data is None:
        logger.warning("股票历史数据为空，无法进行回归测试")
        return
    for k in stocks_data:
        date = k[0]
        break
    logger.info(f"使用日期：{date} 进行回归测试")
    # 回归测试表
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for table in tables:
            executor.submit(process, table, stocks_data, date, backtest_column)
    logger.info("股票策略回归测试数据准备完成")


def process(table, data_all, date, backtest_column):
    table_name = table['name']
    logger.info(f"开始处理表：{table_name}")
    if not mdb.checkTableIsExist(table_name):
        logger.warning(f"表：{table_name} 不存在，跳过处理")
        return

    column_tail = tuple(table['columns'])[-1]
    now_date = datetime.datetime.now().date()
    sql = f"SELECT * FROM `{table_name}` WHERE `date` < '{now_date}' AND `{column_tail}` is NULL"
    try:
        data = pd.read_sql(sql=sql, con=mdb.engine())
        if data is None or len(data.index) == 0:
            logger.warning(f"表：{table_name} 中没有需要处理的数据")
            return

        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        # subset['date'] = subset['date'].values.astype('str')
        subset = subset.astype({'date': 'string'})
        stocks = [tuple(x) for x in subset.values]
        logger.info(f"表：{table_name} 中有 {len(stocks)} 条数据需要处理")

        results = run_check(stocks, data_all, date, backtest_column)
        if results is None:
            logger.warning(f"表：{table_name} 处理结果为空")
            return

        data_new = pd.DataFrame(results.values())
        mdb.update_db_from_df(data_new, table_name, ('date', 'code'))
        logger.info(f"表：{table_name} 处理完成，更新了 {len(results)} 条数据")

    except Exception as e:
        logger.error(f"process处理异常：{table_name}表{e}")


def run_check(stocks, data_all, date, backtest_column, workers=40):
    data = {}
    logger.info(f"开始运行检查，共 {len(stocks)} 只股票需要检查")
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
                    logger.error(f"run_check处理异常：{stock[1]}代码{e}")
    except Exception as e:
        logger.error(f"run_check处理异常：{e}")
    if not data:
        logger.warning("运行检查结果为空")
        return None
    else:
        logger.info(f"运行检查完成，共获得 {len(data)} 条有效结果")
        return data


def main():
    logger.info("开始执行股票策略回归测试每日任务")
    prepare()
    logger.info("股票策略回归测试每日任务执行完成")


# main函数入口
if __name__ == '__main__':
    main()
