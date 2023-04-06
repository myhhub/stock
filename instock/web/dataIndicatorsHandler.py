#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
from tornado import gen
import logging
import instock.core.stockfetch as stf
import instock.lib.version as version
import instock.core.kline.visualization as vis
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '


# 获得页面数据。
class GetDataIndicatorsHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        code = self.get_argument("code", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        # self.uri_ = ("self.request.url:", self.request.uri)
        # print self.uri_
        comp_list = []
        try:
            stock = stf.fetch_stock_hist((date, code))
            if stock is None:
                return

            pk = vis.get_plot_kline(code, stock, date)
            if pk is None:
                return

            comp_list.append(pk)
        except Exception as e:
            logging.debug("{}处理异常：{}".format('dataIndicatorsHandler.GetDataIndicatorsHandler', e))

        self.render("stock_indicators.html", comp_list=comp_list,
                    stockVersion=version.__version__,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


