#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os.path
import datetime
import numpy as np
import pandas as pd
import akshare as ak
import talib as tl
import libs.tablestructure as cons

__author__ = 'myh '
__date__ = '2023/3/10 '

# 设置基础目录，每次加载使用。
cpath = os.path.dirname(os.path.dirname(__file__))
stock_hist_cache_path = os.path.join(cpath, 'datas', 'cache', 'hist')
if not os.path.exists(stock_hist_cache_path):
    os.makedirs(stock_hist_cache_path)  # 创建多个文件夹结构。


# 600开头的股票是上证A股，属于大盘股
# 600开头的股票是上证A股，属于大盘股，其中6006开头的股票是最早上市的股票，
# 6016开头的股票为大盘蓝筹股；900开头的股票是上证B股；
# 000开头的股票是深证A股，001、002开头的股票也都属于深证A股，
# 其中002开头的股票是深证A股中小企业股票；
# 200开头的股票是深证B股；
# 300开头的股票是创业板股票；400开头的股票是三板市场股票。
def stock_a(code):
    # 上证A股  # 深证A股
    if code is None:
        return False
    if code.startswith('600') or code.startswith('601') or \
            code.startswith('000') or code.startswith('001') or \
            code.startswith('002'):
        return True
    else:
        return False


# 过滤掉 st 股票。
def stock_a_filter_st(name):
    # 上证A股  # 深证A股
    if name is None:
        return False
    if name.find("ST") == -1:
        return True
    else:
        return False


# 过滤价格，如果没有基本上是退市了。
def stock_a_filter_price(latest_price):
    # float 在 pandas 里面判断 空。
    if latest_price is None:
        return False
    if np.isnan(latest_price):
        return False
    else:
        return True


# 读取股票交易日历数据
def fetch_stocks_trade_date():
    try:
        data = ak.tool_trade_date_hist_sina()
        if data is None or len(data.index) == 0:
            return None
        data_date = set(data['trade_date'].tolist())
    except Exception as e:
        logging.debug("{}处理异常：{}".format('stockfetch.fetch_stocks_trade_date', e))

    return data_date


# 读取当天股票数据
def fetch_stocks(date):
    try:
        data = ak.stock_zh_a_spot_em()
        if data is None or len(data.index) == 0:
            return None
        columns = list(cons.TABLE_CN_STOCK_SPOT['columns'].keys())
        columns[0] = 'index'
        data.columns = columns
        data = data.loc[data["code"].apply(stock_a)].loc[
            data["latest_price"].apply(stock_a_filter_price)]
        # .loc[data["name"].apply(runtmp.stock_a_filter_st)]

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        # 删除index
        data.drop('index', axis=1, inplace=True)

    except Exception as e:
        logging.debug("{}处理异常：{}".format('stockfetch.fetch_stocks', e))

    return data


# 股票近三月上龙虎榜且必须有2次以上机构参与的
def fetch_stock_top_entity_data(date):
    run_date = date + datetime.timedelta(days=-90)
    start_date = run_date.strftime("%Y%m%d")
    end_date = date.strftime("%Y%m%d")
    code_name = '代码'
    entity_amount_name = '买方机构数'
    try:
        data = ak.stock_lhb_jgmmtj_em(start_date, end_date)
        if data is None or len(data.index) == 0:
            return None

        # 机构买入次数大于1计算方法，首先：每次要有买方机构数(>0),然后：这段时间买方机构数求和大于1
        mask = (data[entity_amount_name] > 0)  # 首先：每次要有买方机构数(>0)
        data = data.loc[mask]

        if len(data.index) == 0:
            return None

        grouped = data.groupby(by=data[code_name])
        data_series = grouped[entity_amount_name].sum()
        data_code = set(data_series[data_series > 1].index.values)  # 然后：这段时间买方机构数求和大于1

        if not data_code:
            return None

        return data_code
    except Exception as e:
        logging.debug("{}处理异常：{}".format('stockfetch.fetch_stock_top_entity_data', e))


# 描述: 获取新浪财经-龙虎榜-个股上榜统计
def fetch_stock_top_data(date):
    try:
        data = ak.stock_lhb_ggtj_sina(recent_day="5")
        if data is None or len(data.index) == 0:
            return None
        _columns = list(cons.TABLE_CN_STOCK_TOP['columns'].keys())
        _columns.pop(0)
        data.columns = _columns

        data = data.loc[data["code"].apply(stock_a)]
        # .loc[data["name"].apply(stock_a_filter_st)]
        data.insert(0, 'date', date)
    except Exception as e:
        logging.debug("{}处理异常：{}".format('stockfetch.fetch_stock_top_data', e))

    return data


# 描述: 获取东方财富网-数据中心-大宗交易-每日统计
def fetch_stock_blocktrade_data(date):
    date_str = date.strftime("%Y%m%d")
    try:
        data = ak.stock_dzjy_mrtj(start_date=date_str, end_date=date_str)
        if data is None or len(data.index) == 0:
            return None

        columns = list(cons.TABLE_CN_STOCK_BLOCKTRADE['columns'].keys())
        columns.insert(0, 'index')
        data.columns = columns

        data = data.loc[data["code"].apply(stock_a)]
        # .loc[data["name"].apply(stock_a_filter_st)]
        # data.set_index('code', inplace=True)
        data.drop('index', axis=1, inplace=True)

    except Exception as e:
        logging.debug("{}处理异常：{}".format('stockfetch.fetch_stock_blocktrade_data', e))

    return data


# 读取股票历史数据
def fetch_stock_hist(data_base):
    date = data_base[0]
    code = data_base[1]

    tmp_year, tmp_month, tmp_day = date.split("-")
    date_end = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
    date_start = (date_end + datetime.timedelta(days=-(365 * 3))).strftime("%Y%m%d")
    # date_end = date_end.strftime("%Y%m%d")
    try:
        data = stock_hist_cache(code, date_start, None, 'qfq')
        if data is not None:
            data.loc[:, 'p_change'] = tl.ROC(data['close'], 1)
    except Exception as e:
        logging.debug("{}处理异常：{}".format('stockfetch.fetch_stock_hist', e))

    return data


# 增加读取股票缓存方法。加快处理速度。多线程解决效率
def stock_hist_cache(code, date_start, date_end=None, adjust=''):
    cache_dir = os.path.join(stock_hist_cache_path, date_start[0:6], date_start)
    # 如果没有文件夹创建一个。月文件夹和日文件夹。方便删除。
    try:
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    except Exception:
        pass
    cache_file = os.path.join(cache_dir, "%s%s.gzip.pickle" % (code, adjust))
    # 如果缓存存在就直接返回缓存数据。压缩方式。
    try:
        if os.path.isfile(cache_file):
            return pd.read_pickle(cache_file, compression="gzip")
        else:
            if date_end is not None:
                stock = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=date_start, end_date=date_end,
                                           adjust=adjust)
            else:
                stock = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=date_start, adjust=adjust)

            if stock is None or len(stock.index) == 0:
                return None
            stock.columns = list(cons.CN_STOCK_HIST_DATA['columns'].keys())
            stock = stock.sort_index()  # 将数据按照日期排序下。
            try:
                stock.to_pickle(cache_file, compression="gzip")
            except Exception:
                pass
            # time.sleep(1)
            return stock
    except Exception as e:
        logging.debug("{}处理异常：{}代码{}".format('stockfetch.stock_hist_cache', code, e))
