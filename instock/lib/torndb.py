#!/usr/local/bin/python
# -*- coding: utf-8 -*-

"""A lightweight wrapper around pymysql.
Originally part of the Tornado framework.  The tornado.database module
is slated for removal in Tornado 3.0, and it is now available separately
as torndb.
"""

from __future__ import absolute_import, division, with_statement
import copy
import itertools
import logging
import os
import time
import pymysql

__author__ = 'myh '
__date__ = '2023/3/10 '

try:

    # import MySQLdb.constants
    # import MySQLdb.converters
    # import MySQLdb.cursors

    # 修改
    import pymysql.connections
    import pymysql.converters
    import pymysql.cursors
    import pymysql.constants.FLAG


except ImportError:
    # If MySQLdb isn't available this module won't actually be useable,
    # but we want it to at least be importable on readthedocs.org,
    # which has limitations on third-party modules.
    if 'READTHEDOCS' in os.environ:
        pymysql = None
    else:
        raise

version = "0.3"
version_info = (0, 3, 0, 0)


class Connection(object):
    """A lightweight wrapper around MySQLdb DB-API connections.
    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage::
        db = torndb.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title
    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.
    We explicitly set the timezone to UTC and assume the character encoding to
    UTF-8 (can be changed) on all connections to avoid time zone and encoding errors.
    The sql_mode parameter is set by default to "traditional", which "gives an error instead of a warning"
    (http://dev.mysql.com/doc/refman/5.0/en/server-sql-mode.html). However, it can be set to
    any other mode including blank (None) thereby explicitly clearing the SQL mode.
    """

    def __init__(self, host, database, user=None, password=None,
                 max_idle_time=7 * 3600, connect_timeout=10,
                 time_zone="+0:00", charset="utf8", sql_mode="TRADITIONAL"):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        #  自定义
        args = dict(conv=CONVERSIONS, charset=charset,
                    db=database, init_command=('SET time_zone = "%s"' % time_zone),
                    connect_timeout=connect_timeout, sql_mode=sql_mode)
        # args = dict(conv=CONVERSIONS, use_unicode=True, charset=charset,
        #             db=database, init_command=('SET time_zone = "%s"' % time_zone),
        #             connect_timeout=connect_timeout, sql_mode=sql_mode)
        #

        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error(f"Cannot connect to MySQL on {self.host}", exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()

        # self._db = MySQLdb.connect(**self._db_args)
        self._db = pymysql.connect(**self._db_args)

        self._db.autocommit(True)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = pymysql.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    # 自定义
    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.zip_longest(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    # def query(self, query, *parameters, **kwparameters):
    #     """Returns a row list for the given query and parameters."""
    #     cursor = self._cursor()
    #     try:
    #         self._execute(cursor, query, parameters, kwparameters)
    #         column_names = [d[0] for d in cursor.description]
    #         return [Row(itertools.izip(column_names, row)) for row in cursor]
    #     finally:
    #         cursor.close()
    #

    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    update = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None or
                (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            return cursor.execute(query, kwparameters or parameters)
        except OperationalError:
            logging.error(f"Error connecting to MySQL on {self.host}")
            self.close()
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


if pymysql is not None:
    # Fix the access conversions to properly recognize unicode/binary
    FIELD_TYPE = pymysql.constants.FIELD_TYPE
    FLAG = pymysql.constants.FLAG
    CONVERSIONS = copy.copy(pymysql.converters.conversions)

    field_types = [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING]
    if 'VARCHAR' in vars(FIELD_TYPE):
        field_types.append(FIELD_TYPE.VARCHAR)

    for field_type in field_types:
        # CONVERSIONS[field_type] = [(FLAG.BINARY, str)] + CONVERSIONS[field_type]
        CONVERSIONS[field_type] = [(FLAG.BINARY, str)].append(CONVERSIONS[field_type])

    # Alias some common MySQL exceptions
    IntegrityError = pymysql.IntegrityError
    OperationalError = pymysql.OperationalError
