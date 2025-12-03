#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import pymysql
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.database as mdb

# 获取logger实例
logger = logging.getLogger(__name__)

__author__ = 'myh '
__date__ = '2023/3/10 '


# 创建新数据库。
def create_new_database():
    logger.info("开始创建新数据库")
    _MYSQL_CONN_DBAPI = mdb.MYSQL_CONN_DBAPI.copy()
    _MYSQL_CONN_DBAPI['database'] = "mysql"
    
    try:
        with pymysql.connect(**_MYSQL_CONN_DBAPI) as conn:
            logger.info("成功连接到MySQL服务器")
            with conn.cursor() as db:
                create_sql = f"CREATE DATABASE IF NOT EXISTS `{mdb.db_database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
                logger.info(f"执行SQL: {create_sql}")
                db.execute(create_sql)
                logger.info("数据库创建成功")
                
                # 创建基础表
                create_new_base_table()
                
    except Exception as e:
        logger.error(f"create_new_database处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")


# 创建基础表。
def create_new_base_table():
    logger.info("开始创建基础表")
    try:
        with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
            logger.info("成功连接到目标数据库")
            with conn.cursor() as db:
                create_table_sql = """CREATE TABLE IF NOT EXISTS `cn_stock_attention` (
                                      `datetime` datetime(0) NULL DEFAULT NULL, 
                                      `code` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                                      PRIMARY KEY (`code`) USING BTREE,
                                      INDEX `INIX_DATETIME`(`datetime`) USING BTREE
                                      ) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;"""
                logger.info("执行创建表SQL")
                db.execute(create_table_sql)
                logger.info("基础表创建成功")
                
    except Exception as e:
        logger.error(f"create_new_base_table处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")


def check_database():
    logger.info("开始检查数据库连接")
    try:
        with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
            logger.info("成功连接到数据库")
            with conn.cursor() as db:
                logger.info("执行数据库连接测试: select 1")
                db.execute(" select 1 ")
                logger.info("数据库连接测试成功")
                
    except Exception as e:
        logger.error(f"check_database处理异常：{e}")
        import traceback
        logger.error(f"异常堆栈信息: {traceback.format_exc()}")
        raise  # 重新抛出异常，让调用者知道检查失败


def main():
    logger.info("init_job任务开始执行")
    
    try:
        # 检查数据库连接
        logger.info("第1步: 检查数据库连接")
        check_database()
        logger.info("数据库连接检查通过")
        
    except Exception as e:
        logger.warning(f"数据库连接检查失败: {e}")
        logger.info("开始创建数据库")
        create_new_database()
        logger.info("数据库创建完成")
    
    logger.info("init_job任务执行完成")


# main函数入口
if __name__ == '__main__':
    main()
