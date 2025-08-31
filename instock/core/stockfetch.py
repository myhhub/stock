#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os.path
import datetime
import traceback
import numpy as np
import pandas as pd
import talib as tl
import instock.core.tablestructure as tbs
import instock.lib.trade_time as trd
import instock.core.crawling.trade_date_hist as tdh
import instock.core.crawling.fund_etf_em as fee
import instock.core.crawling.stock_selection as sst
import instock.core.crawling.stock_lhb_em as sle
import instock.core.crawling.stock_lhb_sina as sls
import instock.core.crawling.stock_dzjy_em as sde
import instock.core.crawling.stock_hist_em as she
import instock.core.crawling.stock_fund_em as sff
import instock.core.crawling.stock_fhps_em as sfe
import instock.core.crawling.stock_chip_race as scr
import instock.core.crawling.stock_limitup_reason as slr
from instock.core.proxy_pool import get_proxy
from instock.core.tablestructure import TABLE_CN_STOCK_SPOT
__author__ = 'myh '
__date__ = '2023/3/10 '

# 设置基础目录，每次加载使用。
cpath_current = os.path.dirname(os.path.dirname(__file__))
stock_hist_cache_path = os.path.join(cpath_current, 'cache', 'hist')
if not os.path.exists(stock_hist_cache_path):
    os.makedirs(stock_hist_cache_path)  # 创建多个文件夹结构。


# 600 601 603 605开头的股票是上证A股
# 600开头的股票是上证A股，属于大盘股，其中6006开头的股票是最早上市的股票，
# 6016开头的股票为大盘蓝筹股；900开头的股票是上证B股；
# 688开头的是上证科创板股票；
# 000开头的股票是深证A股，001、002开头的股票也都属于深证A股，
# 其中002开头的股票是深证A股中小企业股票；
# 200开头的股票是深证B股；
# 300、301开头的股票是创业板股票；400开头的股票是三板市场股票。
# 430、83、87开头的股票是北证A股
def is_a_stock(code):
    # 上证A股  # 深证A股
    return code.startswith(('600', '601', '603', '605', '000', '001', '002', '003', '300', '301'))


# 过滤掉 st 股票。
def is_not_st(name):
    return not name.startswith(('*ST', 'ST'))


# 过滤价格，如果没有基本上是退市了。
def is_open(price):
    return not np.isnan(price)


def is_open_with_line(price):
    return price != '-'


# 读取股票交易日历数据
def fetch_stocks_trade_date():
    try:
        data = tdh.tool_trade_date_hist_sina()
        if data is None or len(data.index) == 0:
            return None
        data_date = set(data['trade_date'].values.tolist())
        return data_date
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_trade_date处理异常：{e}")
    return None


# 读取当天ETF数据
def fetch_etfs(date, save_to_db=True):
    try:
        data = fee.fund_etf_spot_em(proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        if date is None:
            date = datetime.datetime.now()
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        data.columns = list(tbs.TABLE_CN_ETF_SPOT['columns'])
        data = data.loc[data['new_price'].apply(is_open)]
        
        # 保存到数据库
        if save_to_db and data is not None and len(data) > 0:
            from instock.lib.common_check import check_and_delete_old_data_for_realtime_data
            try:
                # 保存到实时表（ETF表可能没有历史表映射配置）
                check_and_delete_old_data_for_realtime_data(
                    tbs.TABLE_CN_ETF_SPOT, 
                    data, 
                    date.strftime("%Y-%m-%d") if hasattr(date, 'strftime') else date,
                    save_to_history=False  # ETF可能没有配置历史表映射
                )
                logging.info(f"成功保存 {len(data)} 条ETF实时数据到数据库")
            except Exception as db_e:
                logging.error(f"保存ETF数据到数据库失败：{db_e}")
        
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_etfs处理异常：{e}")
    return None


# 读取当天股票数据
def fetch_stocks(date, save_to_db=True):
    try:
        # 实时获取
        data = she.stock_zh_a_spot_em(proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        if date is None:
            date = datetime.datetime.now()
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        data.columns = list(tbs.TABLE_CN_STOCK_SPOT['columns'])
        data = data.loc[data['code'].apply(is_a_stock)].loc[data['new_price'].apply(is_open)]
        
        # 保存到数据库
        if save_to_db and data is not None and len(data) > 0:
            from instock.lib.common_check import check_and_delete_old_data_for_realtime_data
            try:
                # 保存到实时表和历史表
                check_and_delete_old_data_for_realtime_data(
                    tbs.TABLE_CN_STOCK_SPOT, 
                    data, 
                    date.strftime("%Y-%m-%d") if hasattr(date, 'strftime') else date,
                    save_to_history=True,
                    use_batch=True
                )
                logging.info(f"成功保存 {len(data)} 条股票实时数据到数据库")
            except Exception as db_e:
                logging.error(f"保存股票数据到数据库失败：{db_e}")
        
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks处理异常：{e}")
    return None


def fetch_stock_selection():
    try:
        data = sst.stock_selection(proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        data.columns = list(tbs.TABLE_CN_STOCK_SELECTION['columns'])
        data.drop_duplicates('secucode', keep='last', inplace=True)
        return data
    except Exception as e:
        traceback.print_exc()
        logging.exception(f"stockfetch.fetch_stocks_selection处理异常：{e}")
    return None


# 读取股票资金流向
def fetch_stocks_fund_flow(index):
    try:
        cn_flow = tbs.CN_STOCK_FUND_FLOW[index]
        data = sff.stock_individual_fund_flow_rank(indicator=cn_flow['cn'], proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        data.columns = list(cn_flow['columns'])
        data = data.loc[data['code'].apply(is_a_stock)].loc[data['new_price'].apply(is_open_with_line)]
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_fund_flow处理异常：{e}")
    return None


# 读取板块资金流向
def fetch_stocks_sector_fund_flow(index_sector, index_indicator):
    try:
        cn_flow = tbs.CN_STOCK_SECTOR_FUND_FLOW[1][index_indicator]
        data = sff.stock_sector_fund_flow_rank(indicator=cn_flow['cn'], sector_type=tbs.CN_STOCK_SECTOR_FUND_FLOW[0][index_sector], proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        data.columns = list(cn_flow['columns'])
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_sector_fund_flow处理异常：{e}")
    return None


# 读取股票分红配送
def fetch_stocks_bonus(date):
    try:
        data = sfe.stock_fhps_em(date=trd.get_bonus_report_date(), proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        data.columns = list(tbs.TABLE_CN_STOCK_BONUS['columns'])
        data = data.loc[data['code'].apply(is_a_stock)]
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stocks_bonus处理异常：{e}")
    return None


# 股票近三月上龙虎榜且必须有2次以上机构参与的
def fetch_stock_top_entity_data(date):
    run_date = date + datetime.timedelta(days=-90)
    start_date = run_date.strftime("%Y%m%d")
    end_date = date.strftime("%Y%m%d")
    code_name = '代码'
    entity_amount_name = '买方机构数'
    try:
        now = datetime.datetime.now()
        if trd.is_tradetime(now):
            data = sle.stock_lhb_jgmmtj_em(start_date, end_date, proxy=get_proxy())
        else:
            # TODO 读数据库
            data = sle.stock_lhb_jgmmtj_em(start_date, end_date, proxy=get_proxy())
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
        logging.error(f"stockfetch.fetch_stock_top_entity_data处理异常：{e}")
    return None


# 描述: 获取新浪财经-龙虎榜-个股上榜统计
def fetch_stock_top_data(date):
    try:
        data = sls.stock_lhb_ggtj_sina(proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        _columns = list(tbs.TABLE_CN_STOCK_TOP['columns'])
        _columns.pop(0)
        data.columns = _columns
        data = data.loc[data['code'].apply(is_a_stock)]
        data.drop_duplicates('code', keep='last', inplace=True)
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_top_data处理异常：{e}")
    return None


# 描述: 获取东方财富网-数据中心-大宗交易-每日统计
def fetch_stock_blocktrade_data(date):
    date_str = date.strftime("%Y%m%d")
    try:
        data = sde.stock_dzjy_mrtj(start_date=date_str, end_date=date_str, proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None

        columns = list(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns'])
        columns.insert(0, 'index')
        data.columns = columns
        data = data.loc[data['code'].apply(is_a_stock)]
        data.drop('index', axis=1, inplace=True)
        return data
    except TypeError:
        logging.error("处理异常：目前还没有大宗交易数据，请17:00点后再获取！")
        return None
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_blocktrade_data处理异常：{e}")
    return None

# 读取早盘抢筹
def fetch_stock_chip_race_open(date):
    try:
        date_str =""
        if date != datetime.datetime.now().date():
            date_str = date.strftime("%Y%m%d")
        data = scr.stock_chip_race_open(date_str, proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        data.columns = list(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns'])
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_chip_race_open处理异常：{e}")
    return None

# 读取尾盘抢筹
def fetch_stock_chip_race_end(date):
    try:
        date_str =""
        if date != datetime.datetime.now().date():
            date_str = date.strftime("%Y%m%d")
        data = scr.stock_chip_race_end(date_str, proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        if date is None:
            data.insert(0, 'date', datetime.datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        data.columns = list(tbs.TABLE_CN_STOCK_CHIP_RACE_END['columns'])
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_chip_race_end处理异常：{e}")
    return None

# 读取涨停原因
def fetch_stock_limitup_reason(date):

    try:
        data = slr.stock_limitup_reason(date.strftime("%Y-%m-%d"), proxy=get_proxy())
        if data is None or len(data.index) == 0:
            return None
        data.columns = list(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns'])
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_stock_limitup_reason处理异常：{e}")
    return None

# 读取股票历史数据
def fetch_etf_hist(data_base, date_start=None, date_end=None, adjust='qfq'):
    date = data_base[0]
    code = data_base[1]

    if date_start is None:
        date_start, is_cache = trd.get_trade_hist_interval(date)  # 提高运行效率，只运行一次
    try:
        if date_end is not None:
            data = fee.fund_etf_hist_em(symbol=code, period="daily", start_date=date_start, end_date=date_end,
                                        adjust=adjust, proxy=get_proxy())
        else:
            data = fee.fund_etf_hist_em(symbol=code, period="daily", start_date=date_start, adjust=adjust, proxy=get_proxy())

        if data is None or len(data.index) == 0:
            return None
        data.columns = tuple(tbs.CN_STOCK_HIST_DATA['columns'])
        data = data.sort_index()  # 将数据按照日期排序下。
        if data is not None:
            data.loc[:, 'p_change'] = tl.ROC(data['close'].values, 1)
            data['p_change'].values[np.isnan(data['p_change'].values)] = 0.0
            data["volume"] = data['volume'].values.astype('double') * 100  # 成交量单位从手变成股。
        return data
    except Exception as e:
        logging.error(f"stockfetch.fetch_etf_hist处理异常：{e}")
    return None


# 读取股票历史数据
def fetch_stock_hist(data_base, date_start=None, cached_data=None):
    date = data_base[0]
    code = data_base[1]

    if date_start is None:
        date_start, is_cache = trd.get_trade_hist_interval(date)  # 提高运行效率，只运行一次
        # date_end = date_end.strftime("%Y%m%d")
    
    try:
        if cached_data is not None:
            data = cached_data.loc[cached_data['code'].str.endswith(code)]
        else:
            data = get_stock_hist_from_db(date_start, code=code)
            print(f"fetch_stock_hist: 从数据库获取股票{code}历史数据，结果数量：{len(data) if data is not None else 0}")
        print(f"fetch_stock_hist: 完成股票{code}历史数据获取")
        return data
    except Exception as e:
        logging.exception(f"stockfetch.fetch_stock_hist处理异常：{e}")
    return None

def convert_date_format(date_string):
    try:
        parsed_date = datetime.datetime.strptime(date_string, "%Y%m%d")
        formatted_date = parsed_date.strftime("%Y-%m-%d")
        
        return formatted_date
    
    except Exception as e:
        print(f"错误：无效的日期格式 '{date_string}'。期望格式：YYYYMMDD")
        return date_string
# 增加读取股票缓存方法。加快处理速度。多线程解决效率
def stock_hist_cache(code, date_start, date_end=None, is_cache=True, adjust='', cached_data=None):
    # cache_dir = os.path.join(stock_hist_cache_path, date_start[0:6], date_start)
    # # 如果没有文件夹创建一个。月文件夹和日文件夹。方便删除。
    # try:
    #     if not os.path.exists(cache_dir):
    #         os.makedirs(cache_dir)
    # except Exception:
    #     pass
    # cache_file = os.path.join(cache_dir, "%s%s.gzip.pickle" % (code, adjust))
    # 如果缓存存在就直接返回缓存数据。压缩方式。
    try:

        if is_cache and cached_data is not None:
            stock = cached_data.loc[cached_data['code'].str.endswith(code)]
        if stock is None or len(stock.index) == 0:
            return None
        stock.columns = tuple(tbs.CN_STOCK_HIST_DATA['columns'])
        stock = stock.sort_index()  # 将数据按照日期排序下。
        # try:
        #     if is_cache:
        #         stock.to_pickle(cache_file, compression="gzip")
        # except Exception:
        #     pass
        # time.sleep(1)
        return stock
    except Exception as e:
        logging.error(f"stockfetch.stock_hist_cache处理异常：{code}代码{e}")
    return None

def get_stock_hist_from_db(date_start, date_end=None, code=None):
    from instock.lib.clickhouse_client import create_clickhouse_client
    # date_start = convert_date_format(date_start)
    # date_end = convert_date_format(date_end)
    columns = ["code","date","open","high","low","close","preclose","volume","amount","turn","p_change"]
    
    with create_clickhouse_client() as client:
        stock = client.get_stock_data(code, date_start, date_end, order_by="date ASC")
        if stock is not None and not stock.empty:
            stock = stock[columns]
            
            # 确保所有数值列为float类型，避免Decimal和序列化问题
            numeric_columns = ["open","high","low","close","preclose","volume","amount","turn","p_change"]
            for col in numeric_columns:
                if col in stock.columns:
                    try:
                        stock[col] = stock[col].astype(float)
                    except:
                        pass
        
    #from instock.lib.database import query_history_data_by_date_range
    # stock = query_history_data_by_date_range(
    #     date_start, date_end, 
    #     columns=columns,
    #     where_extra=f" code = %s" if code else "",
    #     params=(code,) if code else None
    # )
    
    # 将结果列的turn修改为turnover，和CN_STOCK_HIST_DATA保持一致
    stock.rename(columns={"turn": "turnover"}, inplace=True)
    
    if stock is None or len(stock.index) == 0:
        return None
        
    # 计算生成新列
    stock["ups_downs"] = stock["close"] - stock["preclose"]  # 涨跌额：收盘价-昨收价
    # stock["new_price"] = stock["close"]
    # 规避preclose为0的情况，避免除零错误
    # 确保相关列为float64类型，避免数据类型不兼容问题
    stock["high"] = stock["high"].astype(float)
    stock["low"] = stock["low"].astype(float)
    stock["preclose"] = stock["preclose"].astype(float)
    
    stock["amplitude"] = 0.0  # 初始化为0
    mask = stock["preclose"] != 0  # 创建非零掩码
    stock.loc[mask, "amplitude"] = (stock.loc[mask, "high"] - stock.loc[mask, "low"]) / stock.loc[mask, "preclose"] * 100  # 振幅百分比

    # 只选择 CN_STOCK_HIST_DATA 需要的列
    expected_columns = list(tbs.CN_STOCK_HIST_DATA['columns'].keys())
    expected_columns.append('code')
    result_stock = stock[expected_columns]
    result_stock = result_stock.sort_index()  # 将数据按照日期排序下。
    print("Finished rearranging and renaming to match CN_STOCK_HIST_DATA structure.")
    return result_stock
def get_stock_code_name(date=None):
    try:
        from instock.lib.database_factory import read_sql_to_df
        # 查询最大的date作为时间，获取最新的股票代码和名称
        sql = f"SELECT `date`,`code`,`name` FROM `{TABLE_CN_STOCK_SPOT['name']}` WHERE `date` = (SELECT MAX(`date`) FROM `{TABLE_CN_STOCK_SPOT['name']}`)"
        data = read_sql_to_df(sql)
        if data is not None and len(data.index) > 0:
            # 数据已经包含date列，直接设置列名
            data.columns = ['date', 'code', 'name']
            # 元组需要是date, code, name的顺序并且date是字符串

            data = [(x[0].strftime('%Y-%m-%d'), x[1], x[2]) for x in data.values]
            return data
    except Exception as e:
        logging.error(f"stockfetch.get_stock_code_name处理异常：{e}")
    return None


# 批量获取并保存实时数据
def fetch_and_save_realtime_data(date=None, include_etf=True):
    """
    获取并保存实时股票和ETF数据到数据库
    
    Args:
        date: 指定日期，None表示当前日期
        include_etf: 是否包含ETF数据
    
    Returns:
        dict: 包含处理结果的字典
    """
    result = {
        'success': False,
        'stock_count': 0,
        'etf_count': 0,
        'errors': []
    }
    
    try:
        # 获取并保存股票数据
        logging.info("开始获取股票实时数据...")
        stock_data = fetch_stocks(date, save_to_db=True)
        if stock_data is not None:
            result['stock_count'] = len(stock_data)
            logging.info(f"成功获取并保存 {result['stock_count']} 条股票数据")
        else:
            result['errors'].append("获取股票数据失败")
            logging.warning("获取股票数据失败")
        
        # 获取并保存ETF数据
        if include_etf:
            logging.info("开始获取ETF实时数据...")
            etf_data = fetch_etfs(date, save_to_db=True)
            if etf_data is not None:
                result['etf_count'] = len(etf_data)
                logging.info(f"成功获取并保存 {result['etf_count']} 条ETF数据")
            else:
                result['errors'].append("获取ETF数据失败")
                logging.warning("获取ETF数据失败")
        
        # 判断整体是否成功
        result['success'] = (result['stock_count'] > 0 or result['etf_count'] > 0)
        
        if result['success']:
            total_count = result['stock_count'] + result['etf_count']
            logging.info(f"实时数据获取和保存完成，共处理 {total_count} 条记录")
        else:
            logging.error("实时数据获取和保存失败")
        
        return result
        
    except Exception as e:
        error_msg = f"批量获取实时数据时发生异常：{e}"
        logging.error(error_msg)
        result['errors'].append(error_msg)
        return result
