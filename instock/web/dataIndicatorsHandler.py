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
        name = self.get_argument("name", default=None, strip=False)
        comp_list = []
        try:
            if code.startswith(('1', '5')):
                stock = stf.fetch_etf_hist((date, code))
            else:
                stock = stf.fetch_stock_hist((date, code))
            if stock is None:
                return

            pk = vis.get_plot_kline(code, stock, date, name)
            if pk is None:
                return

            comp_list.append(pk)
        except Exception as e:
            logging.error(f"dataIndicatorsHandler.GetDataIndicatorsHandler处理异常：{e}")

        self.render("stock_indicators.html", comp_list=comp_list,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


# 关注股票。
class SaveCollectHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        import datetime
        import pandas as pd
        import instock.lib.database as mdb
        import instock.core.tablestructure as tbs
        code = self.get_argument("code", default=None, strip=False)
        otype = self.get_argument("otype", default=None, strip=False)
        try:
            table_name = tbs.TABLE_CN_STOCK_ATTENTION['name']
            if otype == '1':
                del_sql = f"DELETE FROM `{table_name}` where `code` = '{code}'"
                mdb.executeSql(del_sql)
            else:
                data = pd.DataFrame([[datetime.datetime.now(), code]], columns=tbs.TABLE_CN_STOCK_ATTENTION['columns'])
                if mdb.checkTableIsExist(table_name):
                    cols_type = None
                else:
                    cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_ATTENTION['columns'])
                mdb.insert_db_from_df(data, table_name, cols_type, False, "`code`", {"IX_DATETIME": "`datetime`"})
        except Exception as e:
            err = {"error": str(e)}
            logging.info(err)
            self.write(err)
            return
        self.write("{\"data\":[{}]}")
