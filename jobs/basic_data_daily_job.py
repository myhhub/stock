#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures

import os.path
import sys
# 在项目运行时，临时将项目路径添加到环境变量
cpath = os.path.dirname(os.path.dirname(__file__))
sys.path.append(cpath)


import libs.run_template as runt
import libs.tablestructure as tbs
import libs.database as mdb
import libs.stockfetch as stf
from libs.singleton import stock_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票实时行情数据。
def save_stock_spot_data(date):
    if not runt.is_get_data(date):
        return
    # 股票列表
    try:
        data = stock_data(date).get_data()
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = " DELETE FROM `" + table_name + "` where `date` = '%s' " % date
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.debug("{}处理异常：{}".format('basic_data_daily_job.save_stock_spot_data', e))


# 龙虎榜-个股上榜统计
# 接口: stock_lhb_ggtj_sina
# 目标地址: http://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/ggtj/index.phtml
# 描述: 获取新浪财经-龙虎榜-个股上榜统计
def save_stock_top_data(date):
    if not runt.is_get_data(date):
        return

    try:
        data = stf.fetch_stock_top_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_TOP['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = " DELETE FROM `" + table_name + "` where `date` = '%s' " % date
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_TOP['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.debug("{}处理异常：{}".format('basic_data_daily_job.save_stock_top_data', e))


# 每日统计
# 接口: stock_dzjy_mrtj
# 目标地址: http://data.eastmoney.com/dzjy/dzjy_mrtj.aspx
# 描述: 获取东方财富网-数据中心-大宗交易-每日统计
def save_stock_blocktrade_data(date):
    try:
        data = stf.fetch_stock_blocktrade_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_BLOCKTRADE['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = " DELETE FROM `" + table_name + "` where `date` = '%s' " % date
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.debug("{}处理异常：{}".format('basic_data_daily_job.save_stock_blocktrade_data', e))


def main():
    # 执行数据初始化。
    # 使用方法传递。
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(runt.run_with_args, save_stock_spot_data)
        executor.submit(runt.run_with_args, save_stock_top_data)
        executor.submit(runt.run_with_args, save_stock_blocktrade_data)


# main函数入口
if __name__ == '__main__':
    main()
