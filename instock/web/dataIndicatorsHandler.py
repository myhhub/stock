#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
from tornado import gen
import logging
import instock.core.stockfetch as stf
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
        error_msg = None
        
        # 参数验证
        if not code:
            error_msg = "请提供股票代码参数"
        elif not code.isdigit() or len(code) != 6:
            error_msg = f"股票代码格式错误: {code}，请提供6位数字代码"
        else:
            try:
                # 获取股票数据
                print(f"正在获取股票数据: {code}, 日期: {date}, 名称: {name}")
                
                if code.startswith(('1', '5')):
                    stock = stf.fetch_etf_hist((date, code))
                    data_type = "ETF"
                else:
                    stock = stf.fetch_stock_hist((date, code))
                    data_type = "股票"
                
                print(f"股票数据获取结果: {stock is not None}, 数据类型: {data_type}")
                
                if stock is None or (hasattr(stock, 'empty') and stock.empty):
                    error_msg = f"未找到股票代码 {code} 的历史数据，请检查代码是否正确"
                else:
                    print(f"数据行数: {len(stock) if hasattr(stock, '__len__') else '未知'}")
                    pk = vis.get_plot_kline(code, stock, date, name)
                    print(f"K线图生成结果: {pk is not None}")
                    
                    if pk is None:
                        error_msg = f"生成K线图失败，可能是数据不足或格式错误"
                    else:
                        comp_list.append(pk)
                        print("K线图生成成功，准备渲染页面")
                        
            except Exception as e:
                error_msg = f"处理股票数据时发生错误: {str(e)}"
                logging.error(f"dataIndicatorsHandler.GetDataIndicatorsHandler处理异常：{e}")
                import traceback
                print(f"详细错误信息: {traceback.format_exc()}")

        # 渲染页面
        self.render("stock_indicators.html", 
                   comp_list=comp_list,
                   error_msg=error_msg,
                   code=code,
                   name=name,
                   date=date,
                   leftMenu=webBase.GetLeftMenu(self.request.uri))


# 关注股票。
class SaveCollectHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        import datetime
        import instock.core.tablestructure as tbs
        code = self.get_argument("code", default=None, strip=False)
        otype = self.get_argument("otype", default=None, strip=False)
        try:
            table_name = tbs.TABLE_CN_STOCK_ATTENTION['name']
            if otype == '1':
                # sql = f"DELETE FROM `{table_name}` WHERE `code` = '{code}'"
                sql = f"DELETE FROM `{table_name}` WHERE `code` = %s"
                self.db.query(sql,code)
            else:
                # sql = f"INSERT INTO `{table_name}`(`datetime`, `code`) VALUE('{datetime.datetime.now()}','{code}')"
                sql = f"INSERT INTO `{table_name}`(`datetime`, `code`) VALUE(%s, %s)"
                self.db.query(sql,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),code)
        except Exception as e:
            err = {"error": str(e)}
            # logging.info(err)
            # self.write(err)
            # return
        self.write("{\"data\":[{}]}")