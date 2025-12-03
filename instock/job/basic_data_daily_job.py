#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf
from instock.core.singleton_stock import stock_data

# 获取logger实例
logger = logging.getLogger(__name__)

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票实时行情数据。
def save_nph_stock_spot_data(date, before=True):
    logger.info(f"开始执行save_nph_stock_spot_data，日期: {date}, before: {before}")
    
    if before:
        logger.info("before参数为True，跳过执行")
        return
    
    try:
        # 获取股票数据
        logger.info("开始获取股票数据")
        data = stock_data(date).get_data()
        
        if data is None or len(data.index) == 0:
            logger.warning("获取到的股票数据为空")
            return
        
        logger.info(f"成功获取股票数据，共 {len(data.index)} 条记录")
        
        table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        logger.info(f"目标表: {table_name}")
        
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            logger.info(f"执行删除老数据SQL: {del_sql}")
            mdb.executeSql(del_sql)
            cols_type = None
            logger.info("老数据删除完成")
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT['columns'])
            logger.info("表不存在，将创建新表")
        
        # 插入新数据
        logger.info("开始插入新数据到数据库")
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logger.info("股票数据保存完成")
        
    except Exception as e:
        logger.error(f"save_nph_stock_spot_data处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")


# 基金实时行情数据。
def save_nph_etf_spot_data(date, before=True):
    logger.info(f"开始执行save_nph_etf_spot_data，日期: {date}, before: {before}")
    
    if before:
        logger.info("before参数为True，跳过执行")
        return
    
    try:
        # 获取ETF数据
        logger.info("开始获取ETF数据")
        data = stf.fetch_etfs(date)
        
        if data is None or len(data.index) == 0:
            logger.warning("获取到的ETF数据为空")
            return
        
        logger.info(f"成功获取ETF数据，共 {len(data.index)} 条记录")
        
        table_name = tbs.TABLE_CN_ETF_SPOT['name']
        logger.info(f"目标表: {table_name}")
        
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            logger.info(f"执行删除老数据SQL: {del_sql}")
            mdb.executeSql(del_sql)
            cols_type = None
            logger.info("老数据删除完成")
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_ETF_SPOT['columns'])
            logger.info("表不存在，将创建新表")
        
        # 插入新数据
        logger.info("开始插入新数据到数据库")
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logger.info("ETF数据保存完成")
        
    except Exception as e:
        logger.error(f"save_nph_etf_spot_data处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")



def main():
    logger.info("basic_data_daily_job任务开始执行")
    
    try:
        # 执行股票实时行情数据保存
        logger.info("第1步: 保存股票实时行情数据")
        runt.run_with_args(save_nph_stock_spot_data)
        logger.info("股票实时行情数据保存完成")
        
        # 执行基金实时行情数据保存
        logger.info("第2步: 保存基金实时行情数据")
        runt.run_with_args(save_nph_etf_spot_data)
        logger.info("基金实时行情数据保存完成")
        
    except Exception as e:
        logger.error(f"basic_data_daily_job任务执行异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
    
    logger.info("basic_data_daily_job任务执行完成")


# main函数入口
if __name__ == '__main__':
    main()
