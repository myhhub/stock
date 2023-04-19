#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect

__author__ = 'myh '
__date__ = '2023/3/10 '

db_host = "localhost"  # 数据库服务主机
db_user = "root"  # 数据库访问用户
db_password = "root"  # 数据库访问密码
db_database = "instockdb"  # 数据库名称
db_port = 3306  # 数据库服务端口
db_charset = "utf8mb4"  # 数据库字符集

MYSQL_CONN_URL = "mysql+pymysql://%s:%s@%s:%s/%s?charset=%s" % (
    db_user, db_password, db_host, db_port, db_database, db_charset)
logging.info("{}执行信息：{}".format('数据库链接', MYSQL_CONN_URL))

MYSQL_CONN_DBAPI = {'host': db_host, 'user': db_user, 'password': db_password, 'database': db_database,
                    'charset': db_charset, 'port': db_port, 'autocommit': True}

MYSQL_CONN_TORNDB = {'host': f'{db_host}:{str(db_port)}', 'user': db_user, 'password': db_password,
                     'database': db_database, 'charset': db_charset, 'max_idle_time': 3600, 'connect_timeout': 1000}


# 通过数据库链接 engine
def engine():
    return create_engine(MYSQL_CONN_URL)


def engine_to_db(to_db):
    _engine = create_engine(MYSQL_CONN_URL.replace(f'/{db_database}?', f'/{to_db}?'))
    return _engine


# DB Api -数据库连接对象connection，有游标
def conn_with_cursor():
    return conn_not_cursor().cursor()


# DB Api -数据库连接对象connection，无游标
def conn_not_cursor():
    try:
        _db = pymysql.connect(**MYSQL_CONN_DBAPI)
    except Exception as e:
        logging.debug("{}处理异常：{}{}".format('database.conn_not_cursor', MYSQL_CONN_DBAPI, e))
    return _db


# 定义通用方法函数，插入数据库表，并创建数据库主键，保证重跑数据的时候索引唯一。
def insert_db_from_df(data, table_name, cols_type, write_index, primary_keys):
    # 插入默认的数据库。
    insert_other_db_from_df(None, data, table_name, cols_type, write_index, primary_keys)


# 增加一个插入到其他数据库的方法。
def insert_other_db_from_df(to_db, data, table_name, cols_type, write_index, primary_keys):
    # 定义engine
    if to_db is None:
        engine_mysql = engine()
    else:
        engine_mysql = engine_to_db(to_db)
    # 使用 http://docs.sqlalchemy.org/en/latest/core/reflection.html
    # 使用检查检查数据库表是否有主键。
    ipt = inspect(engine_mysql)
    col_name_list = data.columns.tolist()
    # 如果有索引，把索引增加到varchar上面。
    if write_index:
        # 插入到第一个位置：
        col_name_list.insert(0, data.index.name)
    try:
        if cols_type is None:
            data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                        index=write_index, )
        elif not cols_type:
            data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                        dtype={col_name: NVARCHAR(255) for col_name in col_name_list}, index=write_index, )
        else:
            data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                        dtype=cols_type, index=write_index, )
    except Exception as e:
        logging.debug("{}处理异常：{}表{}".format('database.insert_other_db_from_df', table_name, e))

    # 判断是否存在主键
    if not ipt.get_pk_constraint(table_name)['constrained_columns']:
        try:
            # 执行数据库插入数据。
            conn_with_cursor().execute(f'ALTER TABLE `{table_name}` ADD PRIMARY KEY ({primary_keys});')
        except Exception as e:
            logging.debug("{}处理异常：{}表{}".format('database.insert_other_db_from_df', table_name, e))


# 更新数据
def update_db_from_df(data, table_name, where):
    data = data.where(data.notnull(), None)
    update_string = f'UPDATE `{table_name}` set '
    where_string = ' where '
    cols = tuple(data.columns)
    with conn_with_cursor() as db:
        try:
            for row in data.values:
                sql = update_string
                sql_where = where_string
                for index, col in enumerate(cols):
                    if col in where:
                        if len(sql_where) == len(where_string):
                            if type(row[index]) == str:
                                sql_where = f'''{sql_where}`{col}` = '{row[index]}' '''
                            else:
                                sql_where = f'''{sql_where}`{col}` = {row[index]} '''
                        else:
                            if type(row[index]) == str:
                                sql_where = f'''{sql_where} and `{col}` = '{row[index]}' '''
                            else:
                                sql_where = f'''{sql_where} and `{col}` = {row[index]} '''
                    else:
                        if type(row[index]) == str:
                            if row[index] is None or row[index] != row[index]:
                                sql = f'''{sql}`{col}` = NULL, '''
                            else:
                                sql = f'''{sql}`{col}` = '{row[index]}', '''
                        else:
                            if row[index] is None or row[index] != row[index]:
                                sql = f'''{sql}`{col}` = NULL, '''
                            else:
                                sql = f'''{sql}`{col}` = {row[index]}, '''
                sql = f'{sql[:-2]}{sql_where}'
                db.execute(sql)
            db.close()
        except Exception as e:
            logging.debug("{}处理异常：{}{}".format('database.update_db_from_df', sql, e))


# 检查表是否存在
def checkTableIsExist(tableName):
    with conn_with_cursor() as db:
        db.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(tableName.replace('\'', '\'\'')))

    if db.fetchone()[0] == 1:
        db.close()
        return True

    db.close()
    return False


# 增删改数据
def executeSql(sql, params=()):
    with conn_with_cursor() as db:
        try:
            db.execute(sql, params)
            db.close()
        except Exception as e:
            logging.debug("{}处理异常：{}{}".format('database.executeSql', sql, e))


# 查询数据
def executeSqlFetch(sql, params=()):
    with conn_with_cursor() as db:
        try:
            db.execute(sql, params)
        except Exception as e:
            logging.debug("{}处理异常：{}{}".format('database.executeSqlFetch', sql, e))

        result = db.fetchall()
        db.close()
        return result


# 计算数量
def executeSqlCount(sql, params=()):
    with conn_with_cursor() as db:
        try:
            db.execute(sql, params)
        except Exception as e:
            logging.debug("{}处理异常：{}".format('database.select_count计算数量', e))

        result = db.fetchall()
        db.close()
        # 只有一个数组中的第一个数据
        if len(result) == 1:
            return int(result[0][0])
        else:
            return 0
