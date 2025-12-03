#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
import datetime

# 获取logger实例
logger = logging.getLogger(__name__)

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.backtest.rate_stats as rate
from instock.core.singleton_stock import stock_hist_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票策略回归测试。
def prepare():
    logger.info("开始执行prepare()函数")
    try:
        tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
        tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
        logger.info(f"准备处理 {len(tables)} 个表")
        
        backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        backtest_columns.insert(0, 'code')
        backtest_columns.insert(0, 'date')
        backtest_column = backtest_columns
        logger.info(f"回测数据列：{backtest_column}")

        # 获取当前日期
        date = datetime.datetime.now().date()
        logger.info(f"使用当前日期：{date}")

        logger.info("开始获取股票历史数据")
        stocks_data = stock_hist_data(date=date).get_data()
        
        if stocks_data is None:
            logger.warning("股票历史数据为空，无法执行回测")
            return
            
        logger.info(f"成功获取股票历史数据，共 {len(stocks_data)} 只股票")
        
        # 回归测试表
        logger.info("开始并行处理各个表")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for table in tables:
                logger.info(f"提交任务：处理表 {table['name']}")
                executor.submit(process, table, stocks_data, date, backtest_column)
        
        logger.info("prepare()函数执行完成")
        
    except Exception as e:
        logger.error(f"backtest_data_daily_job.prepare处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")


def process(table, data_all, date, backtest_column):
    table_name = table['name']
    logger.info(f"开始处理表：{table_name}")
    try:
        if not mdb.checkTableIsExist(table_name):
            logger.warning(f"表 {table_name} 不存在，跳过处理")
            return
            
        logger.info(f"表 {table_name} 存在，开始处理")

        column_tail = tuple(table['columns'])[-1]
        now_date = datetime.datetime.now().date()
        sql = f"SELECT * FROM `{table_name}` WHERE `date` < '{now_date}' AND `{column_tail}` is NULL"
        logger.info(f"执行SQL查询：{sql}")
        
        data = pd.read_sql(sql=sql, con=mdb.engine())
        
        if data is None or len(data.index) == 0:
            logger.info(f"表 {table_name} 中没有需要处理的数据")
            return
            
        logger.info(f"表 {table_name} 中获取到 {len(data)} 条需要处理的数据")

        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        # subset['date'] = subset['date'].values.astype('str')
        subset = subset.astype({'date': 'string'})
        stocks = [tuple(x) for x in subset.values]
        logger.info(f"提取到 {len(stocks)} 只股票需要处理")

        logger.info(f"开始执行回测检查，股票数量：{len(stocks)}")
        results = run_check(stocks, data_all, date, backtest_column)
        
        if results is None:
            logger.warning(f"回测检查没有返回任何结果，表：{table_name}")
            return
            
        logger.info(f"回测检查完成，共返回 {len(results)} 个结果")

        data_new = pd.DataFrame(results.values())
        logger.info(f"开始更新数据库，表：{table_name}，记录数：{len(data_new)}")
        mdb.update_db_from_df(data_new, table_name, ('date', 'code'))
        logger.info(f"数据库更新完成，表：{table_name}")

    except Exception as e:
        logger.error(f"backtest_data_daily_job.process处理异常：{table_name}表{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")


def run_check(stocks, data_all, date, backtest_column, workers=40):
    logger.info(f"开始执行run_check()函数，股票数量：{len(stocks)}，线程数：{workers}")
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(rate.get_rates, stock, 
                                              data_all.get((date, stock[1], stock[2])), backtest_column,
                                              len(backtest_column) - 1): stock for stock in stocks}
            
            logger.info(f"已提交 {len(future_to_data)} 个回测任务")
            
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                completed_count += 1
                
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[stock] = _data_
                        logger.info(f"回测完成：{stock[1]}，进度：{completed_count}/{len(stocks)}")
                    else:
                        logger.warning(f"回测没有返回结果：{stock[1]}，进度：{completed_count}/{len(stocks)}")
                except Exception as e:
                    logger.error(f"backtest_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
                    import traceback
                    logger.error(f"异常堆栈信息: {traceback.format_exc()}")
            
            logger.info(f"所有回测任务执行完成，共完成 {completed_count} 个任务")
            
    except Exception as e:
        logger.error(f"backtest_data_daily_job.run_check处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
        
    if not data:
        logger.warning("run_check()函数没有返回任何数据")
        return None
    else:
        logger.info(f"run_check()函数执行完成，共返回 {len(data)} 个结果")
        return data


def main():
    logger.info("开始执行backtest_data_daily_job.main()函数")
    try:
        prepare()
        logger.info("backtest_data_daily_job.main()函数执行完成")
    except Exception as e:
        logger.error(f"backtest_data_daily_job.main处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")


# main函数入口
if __name__ == '__main__':
    main()
