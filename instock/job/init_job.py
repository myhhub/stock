#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

from instock.lib.logger import setup_logger
import instock.lib.database as mdb

# 配置日志
log_file = os.path.join(cpath_current, 'log', 'init_job.log')
logger = setup_logger('init_job', log_file)

__author__ = 'myh '
__date__ = '2023/3/10 '


# 创建新数据库。
def create_new_database():
    import pymysql
    _MYSQL_CONN_DBAPI = mdb.MYSQL_CONN_DBAPI.copy()
    _MYSQL_CONN_DBAPI['database'] = "mysql"
    with pymysql.connect(**_MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            try:
                create_sql = f"CREATE DATABASE IF NOT EXISTS `{mdb.db_database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
                db.execute(create_sql)
                logger.info("数据库创建成功")
                create_new_base_table()
            except Exception as e:
                logger.error(f"create_new_database处理异常：{e}")


# 创建基础表。
def create_new_base_table():
    import pymysql
    with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            try:
                create_table_sql = """CREATE TABLE IF NOT EXISTS `cn_stock_attention` (
                                      `datetime` datetime(0) NULL DEFAULT NULL, 
                                      `code` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                                      PRIMARY KEY (`code`) USING BTREE,
                                      INDEX `INIX_DATETIME`(`datetime`) USING BTREE
                                      ) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;"""
                db.execute(create_table_sql)
                logger.info("基础表创建成功")
            except Exception as e:
                logger.error(f"create_new_base_table处理异常：{e}")


def check_database():
    import pymysql
    with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            db.execute(" select 1 ")
            logger.info("数据库连接正常")


def main():
    logger.info("开始执行初始化任务")
    # 检查，如果执行 select 1 失败，说明数据库不存在，然后创建一个新的数据库。
    try:
        check_database()
    except Exception as e:
        logger.error(f"数据库连接失败：{e}，将创建新数据库")
        # 检查数据库失败，
        create_new_database()
    # 执行数据初始化。
    logger.info("初始化任务执行完成")


# main函数入口
if __name__ == '__main__':
    main()
