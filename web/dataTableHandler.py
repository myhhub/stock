#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import json
from abc import ABC
from tornado import gen
import logging
import datetime
import libs.version as version
import libs.stock_web_dic as stock_web_dic
import web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '

WEB_EASTMONEY_URL = u"""
    <a class='btn btn-danger btn-xs tooltip-danger' data-rel="tooltip" data-placement="right" data-original-title="东方财富，股票详细地址，新窗口跳转。"
    href='http://quote.eastmoney.com/%s.html' target='_blank'>东财</a>
    <a class='btn btn-success btn-xs tooltip-success' data-rel="tooltip" data-placement="right" data-original-title="本地MACD，KDJ等指标，本地弹窗窗口，数据加载中，请稍候。"
    href='/stock/data/indicators?code=%s&date=%s' target='_blank'>指标</a>
    <a class='btn btn-warning btn-xs tooltip-warning' data-rel="tooltip" data-placement="right" data-original-title="东方财富，研报地址，本地弹窗窗口。"
    onclick="showDFCFWindow('%s');">东研</a>
    """

# 和在dic中的字符串一致。字符串前面都不特别声明是u""
eastmoney_name = "查看股票"


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
        stockWeb = stock_web_dic.STOCK_WEB_DATA_MAP[name]
        # self.uri_ = ("self.request.url:", self.request.uri)
        # print self.uri_
        date_now = datetime.datetime.now()
        date_now_str = date_now.strftime("%Y-%m-%d")
        # 每天的 16 点前显示昨天数据。
        if date_now.hour < 16:
            date_now_str = (date_now + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

        try:
            # 增加columns 字段中的【查看股票 东方财富】
            # logging.info(eastmoney_name in stockWeb.column_names)
            if eastmoney_name in stockWeb.column_names:
                tmp_idx = stockWeb.column_names.index(eastmoney_name)
                # logging.info(tmp_idx)
                # 防止重复插入数据。可能会报错。
                if "eastmoney_url" in stockWeb.columns:
                    stockWeb.columns.remove("eastmoney_url")
                stockWeb.columns.insert(tmp_idx, "eastmoney_url")
        except Exception as e:
            logging.debug("{}处理异常：{}代码{}".format('dataTableHandler.GetStockHtmlHandler', e))

        self.render("stock_web.html", stockWeb=stockWeb, date_now=date_now_str,
                    stockVersion=version.__version__,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


# 获得股票数据内容。
class GetStockDataHandler(webBase.BaseHandler, ABC):
    def get(self):
        # 获得分页参数。
        start_param = self.get_argument("start", default=0, strip=False)
        length_param = self.get_argument("length", default=10, strip=False)

        name_param = self.get_argument("name", default=None, strip=False)
        type_param = self.get_argument("type", default=None, strip=False)
        stock_web = stock_web_dic.STOCK_WEB_DATA_MAP[name_param]
        # https://datatables.net/manual/server-side
        self.set_header('Content-Type', 'application/json;charset=UTF-8')
        order_by_column = []
        order_by_dir = []
        # 支持多排序。使用shift+鼠标左键。
        for item, val in self.request.arguments.items():
            # logging.info("item: %s, val: %s" % (item, val) )
            if str(item).startswith("order["):
                print("order:", item, ",val:", val[0])
            if str(item).startswith("order[") and str(item).endswith("[column]"):
                order_by_column.append(int(val[0]))
            if str(item).startswith("order[") and str(item).endswith("[dir]"):
                order_by_dir.append(val[0].decode("utf-8"))  # bytes转换字符串

        search_by_column = []
        search_by_data = []

        # 返回search字段。
        for item, val in self.request.arguments.items():
            # logging.info("item: %s, val: %s" % (item, val))
            if str(item).startswith("columns[") and str(item).endswith("[search][value]"):
                # logging.info("item: %s, val: %s" % (item, val))
                str_idx = item.replace("columns[", "").replace("][search][value]", "")
                int_idx = int(str_idx)
                # 找到字符串
                str_val = val[0].decode("utf-8")
                if str_val != "":  # 字符串。
                    search_by_column.append(stock_web.columns[int_idx])
                    search_by_data.append(val[0].decode("utf-8"))  # bytes转换字符串

        # 打印日志。
        search_sql = ""
        search_idx = 0
        for item in search_by_column:
            val = search_by_data[search_idx]
            # logging.info("idx: %s, column: %s, value: %s " % (search_idx, item, val))
            # 查询sql
            if search_idx == 0:
                search_sql = " WHERE `%s` = '%s' " % (item, val)
            else:
                search_sql = search_sql + " AND `%s` = '%s' " % (item, val)
            search_idx = search_idx + 1

        # print("stockWeb :", stock_web)
        order_by_sql = ""
        # 增加排序。
        if len(order_by_column) != 0 and len(order_by_dir) != 0:
            order_by_sql = "  ORDER BY "
            idx = 0
            for key in order_by_column:
                # 找到排序字段和dir。
                col_tmp = stock_web.columns[key]
                dir_tmp = order_by_dir[idx]
                if idx != 0:
                    order_by_sql += " ,`%s` %s" % (col_tmp, dir_tmp)
                else:
                    order_by_sql += " `%s` %s" % (col_tmp, dir_tmp)
                idx += 1
        # 查询数据库。
        limit_sql = ""
        if int(length_param) > 0:
            limit_sql = " LIMIT %s , %s " % (start_param, length_param)
        sql = " SELECT * FROM `%s` %s %s %s " % (
            stock_web.table_name, search_sql, order_by_sql, limit_sql)
        count_sql = " SELECT count(1) as num FROM `%s` %s " % (stock_web.table_name, search_sql)

        stock_web_list = self.db.query(sql)
        self.db.close()

        for tmp_obj in (stock_web_list):
            try:
                # 增加columns 字段中的【东方财富】
                # logging.info("eastmoney_name : %s " % eastmoney_name)
                if eastmoney_name in stock_web.column_names:
                    tmp_idx = stock_web.column_names.index(eastmoney_name)

                    code_tmp = tmp_obj["code"]
                    # 判断上海还是 深圳，东方财富 接口要求。
                    if code_tmp.startswith("6"):
                        code_tmp = "SH" + code_tmp
                    else:
                        code_tmp = "SZ" + code_tmp

                    tmp_url = WEB_EASTMONEY_URL % (tmp_obj["code"], tmp_obj["code"], tmp_obj["date"], code_tmp)
                    tmp_obj["eastmoney_url"] = tmp_url
            except Exception as e:
                logging.debug("{}处理异常：{}代码{}".format('dataTableHandler.GetStockDataHandler', e))

        stock_web_size = self.db.query(count_sql)
        self.db.close()

        obj = {
            "draw": 0,
            "recordsTotal": stock_web_size[0]["num"],
            "recordsFiltered": stock_web_size[0]["num"],
            "data": stock_web_list
        }
        self.write(json.dumps(obj, cls=MyEncoder))
