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
from instock.lib.database_factory import get_database, db_config, DatabaseType

__author__ = 'myh '
__date__ = '2023/3/10 '


# 创建新数据库。
def create_new_database():
    """根据配置的数据库类型创建数据库"""
    if db_config.db_type == DatabaseType.MYSQL:
        create_mysql_database()
    elif db_config.db_type == DatabaseType.CLICKHOUSE:
        create_clickhouse_database()
    else:
        logging.error(f"不支持的数据库类型: {db_config.db_type}")

def create_mysql_database():
    """创建MySQL数据库"""
    _MYSQL_CONN_DBAPI = mdb.MYSQL_CONN_DBAPI.copy()
    _MYSQL_CONN_DBAPI['database'] = "mysql"
    with pymysql.connect(**_MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            try:
                create_sql = f"CREATE DATABASE IF NOT EXISTS `{mdb.db_database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
                db.execute(create_sql)
                create_mysql_base_table()
            except Exception as e:
                logging.error(f"init_job.create_mysql_database处理异常：{e}")

# 创建MySQL基础表。
def create_mysql_base_table():
    with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            create_table_sql = """CREATE TABLE IF NOT EXISTS `cn_stock_attention` (
                                  `datetime` datetime(0) NULL DEFAULT NULL, 
                                  `code` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                                  PRIMARY KEY (`code`) USING BTREE,
                                  INDEX `INIX_DATETIME`(`datetime`) USING BTREE
                                  ) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;"""
            db.execute(create_table_sql)

def create_clickhouse_database():
    """创建ClickHouse数据库"""
    try:
        db = get_database()
        # ClickHouse通常数据库已经存在，主要是创建表
        create_clickhouse_tables(db)
        db.close()
        logging.info("ClickHouse数据库初始化完成")
    except Exception as e:
        logging.error(f"init_job.create_clickhouse_database处理异常：{e}")

def create_clickhouse_tables(db):
    """创建ClickHouse表结构"""
    table_sqls = {
        'cn_stock_attention': '''
            CREATE TABLE IF NOT EXISTS cn_stock_attention (
                datetime DateTime,
                code String
            ) ENGINE = MergeTree()
            ORDER BY code
            SETTINGS index_granularity = 8192
        ''',
    }
    
    for table_name, sql in table_sqls.items():
        try:
            success = db.execute(sql.strip())
            if success:
                logging.info(f"ClickHouse表 {table_name} 创建成功")
            else:
                logging.error(f"ClickHouse表 {table_name} 创建失败")
        except Exception as e:
            logging.error(f"创建ClickHouse表 {table_name} 异常: {e}")


# 创建基础表。
def create_new_base_table():
    with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            create_table_sql = """CREATE TABLE IF NOT EXISTS `cn_stock_attention` (
                                  `datetime` datetime(0) NULL DEFAULT NULL, 
                                  `code` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                                  PRIMARY KEY (`code`) USING BTREE,
                                  INDEX `INIX_DATETIME`(`datetime`) USING BTREE
                                  ) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;"""
            db.execute(create_table_sql)


def check_database():
    """检查数据库连接"""
    try:
        if db_config.db_type == DatabaseType.MYSQL:
            check_mysql_database()
        elif db_config.db_type == DatabaseType.CLICKHOUSE:
            check_clickhouse_database()
        else:
            raise Exception(f"不支持的数据库类型: {db_config.db_type}")
    except Exception as e:
        logging.error(f"数据库检查失败: {e}")
        raise

def check_mysql_database():
    """检查MySQL数据库"""
    with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            db.execute("SELECT 1")

def check_clickhouse_database():
    """检查ClickHouse数据库"""
    db = get_database()
    result = db.query("SELECT 1")
    db.close()
    if not result:
        raise Exception("ClickHouse连接测试失败")


def main():
    """主函数 - 根据数据库类型进行初始化"""
    logging.info(f"开始数据库初始化，数据库类型: {db_config.db_type.value}")
    
    # 检查，如果执行 select 1 失败，说明数据库不存在，然后创建一个新的数据库。
    try:
        check_database()
        logging.info("数据库连接检查成功")
    except Exception as e:
        logging.error("执行信息：数据库不存在或连接失败，将创建/初始化数据库。")
        # 检查数据库失败，
        create_new_database()
    
    # 对于ClickHouse，即使连接成功也要确保表结构存在
    if db_config.db_type == DatabaseType.CLICKHOUSE:
        try:
            logging.info("确保ClickHouse表结构存在...")
            db = get_database()
            create_clickhouse_tables(db)
            db.close()
        except Exception as e:
            logging.error(f"ClickHouse表结构检查失败: {e}")
    
    logging.info("数据库初始化完成")


# main函数入口
if __name__ == '__main__':
    main()
