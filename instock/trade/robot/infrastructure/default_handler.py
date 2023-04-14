#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logbook
from logbook import Logger, StreamHandler, FileHandler

__author__ = 'myh '
__date__ = '2023/4/10 '

logbook.set_datetime_format('local')


class DefaultLogHandler(object):
    """默认的 Log 类"""

    def __init__(self, name='default', log_type='stdout', filepath='default.log', loglevel='DEBUG'):
        """Log对象
        :param name: log 名字
        :param :logtype: 'stdout' 输出到屏幕, 'file' 输出到指定文件
        :param :filename: log 文件名
        :param :loglevel: 设定log等级 ['CRITICAL', 'ERROR', 'WARNING', 'NOTICE', 'INFO', 'DEBUG', 'TRACE', 'NOTSET']
        :return log handler object
        """
        self.log = Logger(name)
        if log_type == 'stdout':
            StreamHandler(sys.stdout, level=loglevel).push_application()
        if log_type == 'file':
            if os.path.isdir(filepath) and not os.path.exists(filepath):
                os.makedirs(os.path.dirname(filepath))
            file_handler = FileHandler(filepath, level=loglevel)
            self.log.handlers.append(file_handler)

    def __getattr__(self, item, *args, **kwargs):
        return self.log.__getattribute__(item, *args, **kwargs)
