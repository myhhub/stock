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
import instock.core.pattern.pattern_recognitions as kpr

# 配置日志
log_file = os.path.join(cpath_current, 'log', 'klinepattern_data_daily_job.log')
logger = setup_logger('klinepattern_data_daily_job', log_file)

__author__ = 'myh '
__date__ = '2023/3/10 '


def prepare(date):
    logger.info(f"开始准备股票K线形态数据，日期：{date}")
    try:
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            logger.warning(f"股票历史数据为空，无法准备K线形态数据，日期：{date}")
            return
        results = run_check(stocks_data, date=date)
        if results is None:
            logger.warning(f"股票K线形态数据计算结果为空，日期：{date}")
            return

        table_name = tbs.TABLE_CN_STOCK_KLINE_PATTERN['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_KLINE_PATTERN['columns'])

        dataKey = pd.DataFrame(results.keys())
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        dataKey.columns = _columns

        dataVal = pd.DataFrame(results.values())

        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logger.info(f"股票K线形态数据保存成功，日期：{date}")

    except Exception as e:
        logger.error(f"prepare处理异常：{e}")


def run_check(stocks, date=None, workers=40):
    data = {}
    columns = tbs.STOCK_KLINE_PATTERN_DATA['columns']
    data_column = columns
    logger.info(f"开始运行检查股票K线形态，共 {len(stocks)} 只股票需要检查")
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(kpr.get_pattern_recognition, k, stocks[k], data_column, date=date): k for k in stocks}
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
        logger.warning("股票K线形态检查结果为空")
        return None
    else:
        logger.info(f"股票K线形态检查完成，共获得 {len(data)} 条有效结果")
        return data


def main():
    logger.info("开始执行股票K线形态数据每日任务")
    # 使用方法传递。
    runt.run_with_args(prepare)
    logger.info("股票K线形态数据每日任务执行完成")


# main函数入口
if __name__ == '__main__':
    main()
