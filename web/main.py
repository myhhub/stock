#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import os.path
import sys
from abc import ABC
import tornado.escape
from tornado import gen
import tornado.httpserver
import tornado.ioloop
import tornado.options
import pandas as pd
import numpy as np
import akshare as ak
import bokeh as bh
import talib as tl

# 在项目运行时，临时将项目路径添加到环境变量
cpath = os.path.dirname(os.path.dirname(__file__))
sys.path.append(cpath)
logging.basicConfig(format='%(asctime)s %(message)s', filename=os.path.join(cpath, 'logs', 'stock_web.log'))

import torndb.torndb as torndb
import libs.database as mdb
import libs.common as common
import web.dataTableHandler as dataTableHandler
import web.dataIndicatorsHandler as dataIndicatorsHandler
import web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # 设置路由
            (r"/", HomeHandler),
            (r"/stock/", HomeHandler),
            # 使用datatable 展示报表数据模块。
            (r"/stock/api_data", dataTableHandler.GetStockDataHandler),
            (r"/stock/data", dataTableHandler.GetStockHtmlHandler),
            # 获得股票指标数据。
            (r"/stock/data/indicators", dataIndicatorsHandler.GetDataIndicatorsHandler),
        ]
        settings = dict(  # 配置
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=False,  # True,
            # cookie加密
            cookie_secret="027bb1b670eddf0392cdda8709268a17b58b7",
            debug=True,
        )
        super(Application, self).__init__(handlers, **settings)
        # Have one global connection to the blog DB across all handlers
        self.db = torndb.Connection(**mdb.MYSQL_CONN_TORNDB)


# 首页handler。
class HomeHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        pandasVersion = pd.__version__
        numpyVersion = np.__version__
        akshareVersion = ak.__version__
        bokehVersion = bh.__version__
        stockstatsVersion = '0.5.2'  # ss.__version__
        talibVersion = tl.__version__
        self.render("index.html", pandasVersion=pandasVersion, numpyVersion=numpyVersion,
                    akshareVersion=akshareVersion, bokehVersion=bokehVersion,
                    stockstatsVersion=stockstatsVersion,
                    stockVersion=common.__version__,
                    talibVersion=talibVersion,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    port = 9999
    http_server.listen(port)

    # tornado.options.log_file_prefix = os.path.join(cpath, '/logs/stock_web.log')
    tornado.options.options.logging = None
    # tornado.options.parse_command_line()
    logging.getLogger().setLevel(logging.INFO)

    print("服务已启动，web地址 : http://localhost:9999/")
    logging.info("服务已启动，web地址 : http://localhost:9999/")

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
