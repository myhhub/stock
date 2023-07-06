#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import json
from abc import ABC
from tornado import gen
# import logging
import datetime
import instock.lib.version as version
import instock.lib.trade_time as trd
import instock.core.singleton_stock_web_module_data as sswmd
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


# 获得页面数据。
class GetStockHtmlHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        name = self.get_argument("table_name", default=None, strip=False)
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        run_date, run_date_nph = trd.get_trade_date_last()
        if web_module_data.is_realtime:
            date_now_str = run_date_nph.strftime("%Y-%m-%d")
        else:
            date_now_str = run_date.strftime("%Y-%m-%d")

        self.render("stock_web.html", web_module_data=web_module_data, date_now=date_now_str,
                    stockVersion=version.__version__,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


# 获得股票数据内容。
class GetStockDataHandler(webBase.BaseHandler, ABC):
    def get(self):
        # 获得分页参数。
        start_param = self.get_argument("start", default=0, strip=False)
        length_param = self.get_argument("length", default=10, strip=False)
        name = self.get_argument("name", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        # https://datatables.net/manual/server-side
        self.set_header('Content-Type', 'application/json;charset=UTF-8')
        order_by_column = []
        order_by_dir = []
        # 支持多排序。使用shift+鼠标左键。
        for item, val in self.request.arguments.items():
            if str(item).startswith("order[") and str(item).endswith("[column]"):
                order_by_column.append(int(val[0]))
            if str(item).startswith("order[") and str(item).endswith("[dir]"):
                order_by_dir.append(val[0].decode("utf-8"))  # bytes转换字符串

        search_by_column = []
        search_by_data = []

        # 返回search字段。
        for item, val in self.request.arguments.items():
            if str(item).startswith("columns[") and str(item).endswith("[search][value]"):
                str_idx = item.replace("columns[", "").replace("][search][value]", "")
                int_idx = int(str_idx)
                # 找到字符串
                str_val = val[0].decode("utf-8")
                if str_val != "":  # 字符串。
                    search_by_column.append(web_module_data.columns[int_idx])
                    search_by_data.append(val[0].decode("utf-8"))  # bytes转换字符串

        # 打印日志。
        search_sql = ""
        search_idx = 0
        isHasDate = False
        for item in search_by_column:
            val = search_by_data[search_idx]
            # 查询sql
            if search_idx == 0:
                if item == 'date':
                    search_sql = f" WHERE `{item}` = '{val}'"
                    isHasDate = True
                else:
                    search_sql = f" WHERE `{item}` like '%%{val}%%'"
            else:
                if item == 'date':
                    search_sql = f"{search_sql} AND `{item}` == '{val}'"
                    isHasDate = True
                else:
                    search_sql = f"{search_sql} AND `{item}` like '%%{val}%%'"
            search_idx = search_idx + 1

        if not isHasDate and date is not None:
            if search_idx == 0:
                search_sql = f" WHERE `date` = '{date}'"
            else:
                search_sql = f"{search_sql} AND `date` = '{date}'"

        order_by_sql = ""
        # 增加排序。
        if len(order_by_column) != 0 and len(order_by_dir) != 0:
            order_by_sql = " ORDER BY "
            idx = 0
            for key in order_by_column:
                # 找到排序字段和dir。
                col_tmp = web_module_data.columns[key]
                dir_tmp = order_by_dir[idx]
                if idx != 0:
                    order_by_sql = f"{order_by_sql} ,`{col_tmp}` {dir_tmp}"
                else:
                    order_by_sql = f"{order_by_sql} `{col_tmp}` {dir_tmp}"
                idx += 1

        if order_by_sql == "":
            if web_module_data.order_by is not None:
                order_by_sql = f" ORDER BY {web_module_data.order_by}"

        # 查询数据库。
        limit_sql = ""
        if int(length_param) > 0:
            limit_sql = " LIMIT %s , %s " % (start_param, length_param)
        order_columns = ""
        if web_module_data.order_columns is not None:
            order_columns = f",{web_module_data.order_columns}"
        sql = f"SELECT *{order_columns} FROM `{web_module_data.table_name}`{search_sql}{order_by_sql}{limit_sql}"
        count_sql = f"SELECT count(1) as num FROM `{web_module_data.table_name}`{search_sql}"

        stock_web_list = self.db.query(sql)
        stock_web_size = self.db.query(count_sql)

        obj = {
            "recordsTotal": stock_web_size[0]["num"],
            "recordsFiltered": stock_web_size[0]["num"],
            "data": stock_web_list
        }
        self.write(json.dumps(obj, cls=MyEncoder))
