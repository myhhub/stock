#!/usr/local/bin/python
# -*- coding: utf-8 -*-


__author__ = 'myh '
__date__ = '2023/5/11 '


class web_module_data:
    def __init__(self, mode, type, ico, name, table_name, columns, column_names, primary_key, is_realtime, order_columns=None, order_by=None):
        self.mode = mode  # 模式，query，editor 查询和编辑模式
        self.type = type
        self.ico = ico
        self.name = name
        self.table_name = table_name
        self.columns = columns
        self.column_names = column_names
        self.primary_key = primary_key
        self.is_realtime = is_realtime
        self.order_by = order_by
        self.order_columns = order_columns
        self.url = f"/instock/data?table_name={self.table_name}"
