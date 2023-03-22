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
from libs.singleton import stock_hist_data
import indicator.stockstats_data as ssd

__author__ = 'myh '
__date__ = '2023/3/10 '


def prepare(date):
    try:
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            return
        results = run_check(stocks_data, date=date)
        if results is None:
            return

        table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = " DELETE FROM `" + table_name + "` WHERE `date`= '%s' " % date
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_cols_type(tbs.TABLE_CN_STOCK_INDICATORS['columns'])

        dataKey = pd.DataFrame(results.keys())
        _columns = list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'].keys())
        dataKey.columns = _columns

        dataVal = pd.DataFrame(results.values())
        dataVal.drop('date', axis=1, inplace=True)  # 删除日期字段，然后和原始数据合并。

        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        # data = data.round(2)  # 数据保留2位小数
        # data.set_index('code', inplace=True)
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")

    except Exception as e:
        logging.debug("{}处理异常：{}".format('indicators_data_daily_job.prepare', e))


def run_check(stocks, date=None, workers=40):
    data = {}
    columns = list(tbs.STOCK_STATS_DATA['columns'].keys())
    columns.insert(0, 'code')
    columns.insert(0, 'date')
    data_column = columns
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(ssd.get_indicator_tail, k, v, data_column, date=date): k for k, v in
                              stocks.items()}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[stock] = _data_
                except Exception as e:
                    logging.debug(
                        "{}处理异常：{}代码{}".format('indicators_data_daily_job.run_get_indicator', stock[1], e))
    except Exception as e:
        logging.debug("{}处理异常：{}".format('indicators_data_daily_job.run_get_indicator', e))
    if not data:
        return None
    else:
        return data


# 对每日指标数据，进行筛选。将符合条件的。二次筛选出来。
# 只是做简单筛选
def guess_buy(date):
    try:
        _table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        _columns = list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'].keys())
        _selcol = '`' + '`,`'.join(_columns) + '`'
        sql = "SELECT " + _selcol + " FROM `" + _table_name + \
              "` WHERE `date` = %(date)s and `kdjk` >= 80 and `kdjd` >= 70 and `kdjj` >= 100" \
              " and `rsi_6` >= 80 and `cci` >= 100 and `cr` >= 300 and `wr_6` >= -20 and `vr` >= 160"
        data = pd.read_sql(sql=sql, con=mdb.conn_not_cursor(), params={'date': date})
        data = data.drop_duplicates(subset="code", keep="last")
        # data.set_index('code', inplace=True)

        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_INDICATORS_BUY['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = " DELETE FROM `" + table_name + "` WHERE `date`= '%s' " % date
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_cols_type(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns'])

        _columns_backtest = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'].keys())
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.debug("{}处理异常：{}".format('indicators_data_daily_job.stat_all_lite_buy', e))


# 设置卖出数据。
def guess_sell(date):
    try:
        _table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        _columns = list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'].keys())
        _selcol = '`' + '`,`'.join(_columns) + '`'
        sql = "SELECT " + _selcol + " FROM `" + _table_name + \
              "` WHERE `date` = %(date)s and `kdjk` < 20 and `kdjd` < 30" \
              " and `kdjj` < 10 and `rsi_6` < 20 and `cci` < -100 and `cr` < 40 and `wr_6` < -80 and `vr` < 40"
        data = pd.read_sql(sql=sql, con=mdb.conn_not_cursor(), params={'date': date})
        data = data.drop_duplicates(subset="code", keep="last")
        # data.set_index('code', inplace=True)
        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_INDICATORS_SELL['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = " DELETE FROM `" + table_name + "` WHERE `date`= '%s' " % date
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_cols_type(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns'])

        _columns_backtest = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'].keys())
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.debug("{}处理异常：{}".format('indicators_data_daily_job.stat_all_lite_sell', e))


def main():
    # 使用方法传递。
    runt.run_with_args(prepare)
    # 二次筛选数据。直接计算买卖股票数据。
    runt.run_with_args(guess_buy)
    runt.run_with_args(guess_sell)


# main函数入口
if __name__ == '__main__':
    main()
