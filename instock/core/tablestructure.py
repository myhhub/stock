#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlalchemy import DATE, NVARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME
from sqlalchemy.dialects.mysql import BIT
import talib as tl
from instock.core.strategy import enter
from instock.core.strategy import turtle_trade
from instock.core.strategy import climax_limitdown
from instock.core.strategy import low_atr
from instock.core.strategy import backtrace_ma250
from instock.core.strategy import breakthrough_platform
from instock.core.strategy import parking_apron
from instock.core.strategy import low_backtrace_increase
from instock.core.strategy import keep_increasing
from instock.core.strategy import high_tight_flag

__author__ = 'myh '
__date__ = '2023/3/10 '

RATE_FIELDS_COUNT = 100  # N日收益率字段数目，即N值

TABLE_CN_STOCK_ATTENTION = {'name': 'cn_stock_attention', 'cn': '我的关注',
                            'columns': {'datetime': {'type': DATETIME, 'cn': '时间', 'size': 0},
                                        'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60}}}

TABLE_CN_ETF_SPOT = {'name': 'cn_etf_spot', 'cn': '每日ETF数据',
                     'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                 'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                 'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 120},
                                 'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70},
                                 'change_rate': {'type': FLOAT, 'cn': '涨跌幅', 'size': 70},
                                 'ups_downs': {'type': FLOAT, 'cn': '涨跌额', 'size': 70},
                                 'volume': {'type': BIGINT, 'cn': '成交量', 'size': 90},
                                 'deal_amount': {'type': BIGINT, 'cn': '成交额', 'size': 100},
                                 'open_price': {'type': FLOAT, 'cn': '开盘价', 'size': 70},
                                 'high_price': {'type': FLOAT, 'cn': '最高价', 'size': 70},
                                 'low_price': {'type': FLOAT, 'cn': '最低价', 'size': 70},
                                 'pre_close_price': {'type': FLOAT, 'cn': '昨收', 'size': 70},
                                 'turnoverrate': {'type': FLOAT, 'cn': '换手率', 'size': 70},
                                 'total_market_cap': {'type': BIGINT, 'cn': '总市值', 'size': 120},
                                 'free_cap': {'type': BIGINT, 'cn': '流通市值', 'size': 120}}}

TABLE_CN_STOCK_SPOT = {'name': 'cn_stock_spot', 'cn': '每日股票数据',
                       'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                   'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                   'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70},
                                   'change_rate': {'type': FLOAT, 'cn': '涨跌幅', 'size': 70},
                                   'ups_downs': {'type': FLOAT, 'cn': '涨跌额', 'size': 70},
                                   'volume': {'type': BIGINT, 'cn': '成交量', 'size': 90},
                                   'deal_amount': {'type': BIGINT, 'cn': '成交额', 'size': 100},
                                   'amplitude': {'type': FLOAT, 'cn': '振幅', 'size': 70},
                                   'volume_ratio': {'type': FLOAT, 'cn': '量比', 'size': 70},
                                   'turnoverrate': {'type': FLOAT, 'cn': '换手率', 'size': 70},
                                   'open_price': {'type': FLOAT, 'cn': '今开', 'size': 70},
                                   'high_price': {'type': FLOAT, 'cn': '最高', 'size': 70},
                                   'low_price': {'type': FLOAT, 'cn': '最低', 'size': 70},
                                   'pre_close_price': {'type': FLOAT, 'cn': '昨收', 'size': 70},
                                   'speed_increase': {'type': FLOAT, 'cn': '涨速', 'size': 70},
                                   'speed_increase_5': {'type': FLOAT, 'cn': '5分钟涨跌', 'size': 70},
                                   'speed_increase_60': {'type': FLOAT, 'cn': '60日涨跌幅', 'size': 70},
                                   'speed_increase_all': {'type': FLOAT, 'cn': '年初至今涨跌幅', 'size': 70},
                                   'dtsyl': {'type': FLOAT, 'cn': '市盈率动', 'size': 70},
                                   'pe9': {'type': FLOAT, 'cn': '市盈率TTM', 'size': 70},
                                   'pe': {'type': FLOAT, 'cn': '市盈率静', 'size': 70},
                                   'pbnewmrq': {'type': FLOAT, 'cn': '市净率', 'size': 70},
                                   'basic_eps': {'type': FLOAT, 'cn': '每股收益', 'size': 70},
                                   'bvps': {'type': FLOAT, 'cn': '每股净资产', 'size': 70},
                                   'per_capital_reserve': {'type': FLOAT, 'cn': '每股公积金', 'size': 70},
                                   'per_unassign_profit': {'type': FLOAT, 'cn': '每股未分配利润', 'size': 70},
                                   'roe_weight': {'type': FLOAT, 'cn': '加权净资产收益率', 'size': 70},
                                   'sale_gpr': {'type': FLOAT, 'cn': '毛利率', 'size': 70},
                                   'debt_asset_ratio': {'type': FLOAT, 'cn': '资产负债率', 'size': 70},
                                   'total_operate_income': {'type': BIGINT, 'cn': '营业收入', 'size': 120},
                                   'toi_yoy_ratio': {'type': FLOAT, 'cn': '营业收入同比增长', 'size': 70},
                                   'parent_netprofit': {'type': BIGINT, 'cn': '归属净利润', 'size': 110},
                                   'netprofit_yoy_ratio': {'type': FLOAT, 'cn': '归属净利润同比增长', 'size': 70},
                                   'report_date': {'type': DATE, 'cn': '报告期', 'size': 80},
                                   'total_shares': {'type': BIGINT, 'cn': '总股本', 'size': 120},
                                   'free_shares': {'type': BIGINT, 'cn': '已流通股份', 'size': 120},
                                   'total_market_cap': {'type': BIGINT, 'cn': '总市值', 'size': 120},
                                   'free_cap': {'type': BIGINT, 'cn': '流通市值', 'size': 120},
                                   'industry': {'type': NVARCHAR(20), 'cn': '所处行业', 'size': 100},
                                   'listing_date': {'type': DATE, 'cn': '上市时间', 'size': 80}}}

TABLE_CN_STOCK_SPOT_BUY = {'name': 'cn_stock_spot_buy', 'cn': '基本面选股',
                           'columns': TABLE_CN_STOCK_SPOT['columns'].copy()}

CN_STOCK_FUND_FLOW = ({'name': 'stock_individual_fund_flow_rank', 'cn': '今日',
                       'columns': {'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                   'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70},
                                   'change_rate': {'type': FLOAT, 'cn': '今日涨跌幅', 'size': 70},
                                   'fund_amount': {'type': BIGINT, 'cn': '今日主力净流入-净额', 'size': 100},
                                   'fund_rate': {'type': FLOAT, 'cn': '今日主力净流入-净占比', 'size': 70},
                                   'fund_amount_super': {'type': BIGINT, 'cn': '今日超大单净流入-净额', 'size': 100},
                                   'fund_rate_super': {'type': FLOAT, 'cn': '今日超大单净流入-净占比', 'size': 70},
                                   'fund_amount_large': {'type': BIGINT, 'cn': '今日大单净流入-净额', 'size': 100},
                                   'fund_rate_large': {'type': FLOAT, 'cn': '今日大单净流入-净占比', 'size': 70},
                                   'fund_amount_medium': {'type': BIGINT, 'cn': '今日中单净流入-净额', 'size': 100},
                                   'fund_rate_medium': {'type': FLOAT, 'cn': '今日中单净流入-净占比', 'size': 70},
                                   'fund_amount_small': {'type': BIGINT, 'cn': '今日小单净流入-净额', 'size': 100},
                                   'fund_rate_small': {'type': FLOAT, 'cn': '今日小单净流入-净占比', 'size': 70}}},
                      {'name': 'stock_individual_fund_flow_rank', 'cn': '3日',
                       'columns': {'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                   'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70},
                                   'change_rate_3': {'type': FLOAT, 'cn': '3日涨跌幅', 'size': 70},
                                   'fund_amount_3': {'type': BIGINT, 'cn': '3日主力净流入-净额', 'size': 100},
                                   'fund_rate_3': {'type': FLOAT, 'cn': '3日主力净流入-净占比', 'size': 70},
                                   'fund_amount_super_3': {'type': BIGINT, 'cn': '3日超大单净流入-净额', 'size': 100},
                                   'fund_rate_super_3': {'type': FLOAT, 'cn': '3日超大单净流入-净占比', 'size': 70},
                                   'fund_amount_large_3': {'type': BIGINT, 'cn': '3日大单净流入-净额', 'size': 100},
                                   'fund_rate_large_3': {'type': FLOAT, 'cn': '3日大单净流入-净占比', 'size': 70},
                                   'fund_amount_medium_3': {'type': BIGINT, 'cn': '3日中单净流入-净额', 'size': 100},
                                   'fund_rate_medium_3': {'type': FLOAT, 'cn': '3日中单净流入-净占比', 'size': 70},
                                   'fund_amount_small_3': {'type': BIGINT, 'cn': '3日小单净流入-净额', 'size': 100},
                                   'fund_rate_small_3': {'type': FLOAT, 'cn': '3日小单净流入-净占比', 'size': 70}}},
                      {'name': 'stock_individual_fund_flow_rank', 'cn': '5日',
                       'columns': {'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                   'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70},
                                   'change_rate_5': {'type': FLOAT, 'cn': '5日涨跌幅', 'size': 70},
                                   'fund_amount_5': {'type': BIGINT, 'cn': '5日主力净流入-净额', 'size': 100},
                                   'fund_rate_5': {'type': FLOAT, 'cn': '5日主力净流入-净占比', 'size': 70},
                                   'fund_amount_super_5': {'type': BIGINT, 'cn': '5日超大单净流入-净额', 'size': 100},
                                   'fund_rate_super_5': {'type': FLOAT, 'cn': '5日超大单净流入-净占比', 'size': 70},
                                   'fund_amount_large_5': {'type': BIGINT, 'cn': '5日大单净流入-净额', 'size': 100},
                                   'fund_rate_large_5': {'type': FLOAT, 'cn': '5日大单净流入-净占比', 'size': 70},
                                   'fund_amount_medium_5': {'type': BIGINT, 'cn': '5日中单净流入-净额', 'size': 100},
                                   'fund_rate_medium_5': {'type': FLOAT, 'cn': '5日中单净流入-净占比', 'size': 70},
                                   'fund_amount_small_5': {'type': BIGINT, 'cn': '5日小单净流入-净额', 'size': 100},
                                   'fund_rate_small_5': {'type': FLOAT, 'cn': '5日小单净流入-净占比', 'size': 70}}},
                      {'name': 'stock_individual_fund_flow_rank', 'cn': '10日',
                       'columns': {'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                   'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70},
                                   'change_rate_10': {'type': FLOAT, 'cn': '10日涨跌幅', 'size': 70},
                                   'fund_amount_10': {'type': BIGINT, 'cn': '10日主力净流入-净额', 'size': 100},
                                   'fund_rate_10': {'type': FLOAT, 'cn': '10日主力净流入-净占比', 'size': 70},
                                   'fund_amount_super_10': {'type': BIGINT, 'cn': '10日超大单净流入-净额', 'size': 100},
                                   'fund_rate_super_10': {'type': FLOAT, 'cn': '10日超大单净流入-净占比', 'size': 70},
                                   'fund_amount_large_10': {'type': BIGINT, 'cn': '10日大单净流入-净额', 'size': 100},
                                   'fund_rate_large_10': {'type': FLOAT, 'cn': '10日大单净流入-净占比', 'size': 70},
                                   'fund_amount_medium_10': {'type': BIGINT, 'cn': '10日中单净流入-净额', 'size': 100},
                                   'fund_rate_medium_10': {'type': FLOAT, 'cn': '10日中单净流入-净占比', 'size': 70},
                                   'fund_amount_small_10': {'type': BIGINT, 'cn': '10日小单净流入-净额', 'size': 100},
                                   'fund_rate_small_10': {'type': FLOAT, 'cn': '10日小单净流入-净占比', 'size': 70}}})

TABLE_CN_STOCK_FUND_FLOW = {'name': 'cn_stock_fund_flow', 'cn': '股票资金流向',
                            'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0}}}
for cf in CN_STOCK_FUND_FLOW:
    TABLE_CN_STOCK_FUND_FLOW['columns'].update(cf['columns'].copy())

CN_STOCK_SECTOR_FUND_FLOW = (('行业资金流', '概念资金流'),
                             ({'name': 'stock_sector_fund_flow_rank', 'cn': '今日',
                              'columns': {'name': {'type': NVARCHAR(30), 'cn': '名称', 'size': 70},
                                          'change_rate': {'type': FLOAT, 'cn': '今日涨跌幅', 'size': 70},
                                          'fund_amount': {'type': BIGINT, 'cn': '今日主力净流入-净额', 'size': 100},
                                          'fund_rate': {'type': FLOAT, 'cn': '今日主力净流入-净占比', 'size': 70},
                                          'fund_amount_super': {'type': BIGINT, 'cn': '今日超大单净流入-净额', 'size': 100},
                                          'fund_rate_super': {'type': FLOAT, 'cn': '今日超大单净流入-净占比', 'size': 70},
                                          'fund_amount_large': {'type': BIGINT, 'cn': '今日大单净流入-净额', 'size': 100},
                                          'fund_rate_large': {'type': FLOAT, 'cn': '今日大单净流入-净占比', 'size': 70},
                                          'fund_amount_medium': {'type': BIGINT, 'cn': '今日中单净流入-净额', 'size': 100},
                                          'fund_rate_medium': {'type': FLOAT, 'cn': '今日中单净流入-净占比', 'size': 70},
                                          'fund_amount_small': {'type': BIGINT, 'cn': '今日小单净流入-净额', 'size': 100},
                                          'fund_rate_small': {'type': FLOAT, 'cn': '今日小单净流入-净占比', 'size': 70},
                                          'stock_name': {'type': NVARCHAR(20), 'cn': '今日主力净流入最大股', 'size': 70}}},
                             {'name': 'stock_individual_fund_flow_rank', 'cn': '5日',
                              'columns': {'name': {'type': NVARCHAR(30), 'cn': '名称', 'size': 70},
                                          'change_rate_5': {'type': FLOAT, 'cn': '5日涨跌幅', 'size': 70},
                                          'fund_amount_5': {'type': BIGINT, 'cn': '5日主力净流入-净额', 'size': 100},
                                          'fund_rate_5': {'type': FLOAT, 'cn': '5日主力净流入-净占比', 'size': 70},
                                          'fund_amount_super_5': {'type': BIGINT, 'cn': '5日超大单净流入-净额', 'size': 100},
                                          'fund_rate_super_5': {'type': FLOAT, 'cn': '5日超大单净流入-净占比', 'size': 70},
                                          'fund_amount_large_5': {'type': BIGINT, 'cn': '5日大单净流入-净额', 'size': 100},
                                          'fund_rate_large_5': {'type': FLOAT, 'cn': '5日大单净流入-净占比', 'size': 70},
                                          'fund_amount_medium_5': {'type': BIGINT, 'cn': '5日中单净流入-净额', 'size': 100},
                                          'fund_rate_medium_5': {'type': FLOAT, 'cn': '5日中单净流入-净占比', 'size': 70},
                                          'fund_amount_small_5': {'type': BIGINT, 'cn': '5日小单净流入-净额', 'size': 100},
                                          'fund_rate_small_5': {'type': FLOAT, 'cn': '5日小单净流入-净占比', 'size': 70},
                                          'stock_name_5': {'type': NVARCHAR(20), 'cn': '5日主力净流入最大股', 'size': 70}}},
                             {'name': 'stock_individual_fund_flow_rank', 'cn': '10日',
                              'columns': {'name': {'type': NVARCHAR(30), 'cn': '名称', 'size': 70},
                                          'change_rate_10': {'type': FLOAT, 'cn': '10日涨跌幅', 'size': 70},
                                          'fund_amount_10': {'type': BIGINT, 'cn': '10日主力净流入-净额', 'size': 100},
                                          'fund_rate_10': {'type': FLOAT, 'cn': '10日主力净流入-净占比', 'size': 70},
                                          'fund_amount_super_10': {'type': BIGINT, 'cn': '10日超大单净流入-净额', 'size': 100},
                                          'fund_rate_super_10': {'type': FLOAT, 'cn': '10日超大单净流入-净占比', 'size': 70},
                                          'fund_amount_large_10': {'type': BIGINT, 'cn': '10日大单净流入-净额', 'size': 100},
                                          'fund_rate_large_10': {'type': FLOAT, 'cn': '10日大单净流入-净占比', 'size': 70},
                                          'fund_amount_medium_10': {'type': BIGINT, 'cn': '10日中单净流入-净额', 'size': 100},
                                          'fund_rate_medium_10': {'type': FLOAT, 'cn': '10日中单净流入-净占比', 'size': 70},
                                          'fund_amount_small_10': {'type': BIGINT, 'cn': '10日小单净流入-净额', 'size': 100},
                                          'fund_rate_small_10': {'type': FLOAT, 'cn': '10日小单净流入-净占比', 'size': 70},
                                          'stock_name_10': {'type': NVARCHAR(20), 'cn': '10日主力净流入最大股', 'size': 70}}}))

TABLE_CN_STOCK_FUND_FLOW_INDUSTRY = {'name': 'cn_stock_fund_flow_industry', 'cn': '行业资金流向',
                                     'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0}}}
for cf in CN_STOCK_SECTOR_FUND_FLOW[1]:
    TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns'].update(cf['columns'].copy())

TABLE_CN_STOCK_FUND_FLOW_CONCEPT = {'name': 'cn_stock_fund_flow_concept', 'cn': '概念资金流向',
                                    'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0}}}
for cf in CN_STOCK_SECTOR_FUND_FLOW[1]:
    TABLE_CN_STOCK_FUND_FLOW_CONCEPT['columns'].update(cf['columns'].copy())


TABLE_CN_STOCK_BONUS = {'name': 'cn_stock_bonus', 'cn': '股票分红配送',
                        'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                    'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                    'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                    'convertible_total_rate': {'type': FLOAT, 'cn': '送转股份-送转总比例', 'size': 70},
                                    'convertible_rate': {'type': FLOAT, 'cn': '送转股份-送转比例', 'size': 70},
                                    'convertible_transfer_rate': {'type': FLOAT, 'cn': '送转股份-转股比例', 'size': 70},
                                    'bonusaward_rate': {'type': FLOAT, 'cn': '现金分红-现金分红比例', 'size': 70},
                                    'bonusaward_yield': {'type': FLOAT, 'cn': '现金分红-股息率', 'size': 70},
                                    'basic_eps': {'type': FLOAT, 'cn': '每股收益', 'size': 70},
                                    'bvps': {'type': FLOAT, 'cn': '每股净资产', 'size': 70},
                                    'per_capital_reserve': {'type': FLOAT, 'cn': '每股公积金', 'size': 70},
                                    'per_unassign_profit': {'type': FLOAT, 'cn': '每股未分配利润', 'size': 70},
                                    'netprofit_yoy_ratio': {'type': FLOAT, 'cn': '净利润同比增长', 'size': 70},
                                    'total_shares': {'type': BIGINT, 'cn': '总股本', 'size': 120},
                                    'plan_date': {'type': DATE, 'cn': '预案公告日', 'size': 80},
                                    'record_date': {'type': DATE, 'cn': '股权登记日', 'size': 80},
                                    'ex_dividend_date': {'type': DATE, 'cn': '除权除息日', 'size': 80},
                                    'progress': {'type': NVARCHAR(50), 'cn': '方案进度', 'size': 100},
                                    'report_date': {'type': DATE, 'cn': '最新公告日期', 'size': 80}}}

TABLE_CN_STOCK_TOP = {'name': 'cn_stock_top', 'cn': '股票龙虎榜',
                      'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                  'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                  'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                  'ranking_times': {'type': FLOAT, 'cn': '上榜次数', 'size': 70},
                                  'sum_buy': {'type': FLOAT, 'cn': '累积购买额', 'size': 100},
                                  'sum_sell': {'type': FLOAT, 'cn': '累积卖出额', 'size': 100},
                                  'net_amount': {'type': FLOAT, 'cn': '净额', 'size': 100},
                                  'buy_seat': {'type': FLOAT, 'cn': '买入席位数', 'size': 100},
                                  'sell_seat': {'type': FLOAT, 'cn': '卖出席位数', 'size': 100}}}

TABLE_CN_STOCK_BLOCKTRADE = {'name': 'cn_stock_blocktrade', 'cn': '股票大宗交易',
                             'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                         'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                         'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70},
                                         'new_price': {'type': FLOAT, 'cn': '收盘价', 'size': 70},
                                         'change_rate': {'type': FLOAT, 'cn': '涨跌幅', 'size': 70},
                                         'average_price': {'type': FLOAT, 'cn': '成交均价', 'size': 70},
                                         'overflow_rate': {'type': FLOAT, 'cn': '折溢率', 'size': 120},
                                         'trade_number': {'type': FLOAT, 'cn': '成交笔数', 'size': 70},
                                         'sum_volume': {'type': FLOAT, 'cn': '成交总量', 'size': 100},
                                         'sum_turnover': {'type': FLOAT, 'cn': '成交总额', 'size': 100},
                                         'turnover_market_rate': {'type': FLOAT, 'cn': '成交占比流通市值',
                                                                  'size': 120}}}

CN_STOCK_HIST_DATA = {'name': 'fund_etf_hist_em', 'cn': '基金某时间段的日行情数据库',
                      'columns': {'date': {'type': DATE, 'cn': '日期'},
                                  'open': {'type': FLOAT, 'cn': '开盘'},
                                  'close': {'type': FLOAT, 'cn': '收盘'},
                                  'high': {'type': FLOAT, 'cn': '最高'},
                                  'low': {'type': FLOAT, 'cn': '最低'},
                                  'volume': {'type': FLOAT, 'cn': '成交量'},
                                  'amount': {'type': FLOAT, 'cn': '成交额'},
                                  'amplitude': {'type': FLOAT, 'cn': '振幅'},
                                  'quote_change': {'type': FLOAT, 'cn': '涨跌幅'},
                                  'ups_downs': {'type': FLOAT, 'cn': '涨跌额'},
                                  'turnover': {'type': FLOAT, 'cn': '换手率'}}}

TABLE_CN_STOCK_FOREIGN_KEY = {'name': 'cn_stock_foreign_key', 'cn': '股票外键',
                              'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                          'code': {'type': NVARCHAR(6), 'cn': '代码', 'size': 60},
                                          'name': {'type': NVARCHAR(20), 'cn': '名称', 'size': 70}}}

TABLE_CN_STOCK_BACKTEST_DATA = {'name': 'cn_stock_backtest_data', 'cn': '股票回归测试数据',
                                'columns': {'rate_%s' % i: {'type': FLOAT, 'cn': '%s日收益率' % i, 'size': 100} for i in
                                            range(1, RATE_FIELDS_COUNT + 1, 1)}}

STOCK_STATS_DATA = {'name': 'calculate_indicator', 'cn': '股票统计/指标计算助手库',
                    'columns': {'close': {'type': FLOAT, 'cn': '价格', 'size': 0},
                                'macd': {'type': FLOAT, 'cn': 'dif', 'size': 70},
                                'macds': {'type': FLOAT, 'cn': 'macd', 'size': 70},
                                'macdh': {'type': FLOAT, 'cn': 'histogram', 'size': 70},
                                'kdjk': {'type': FLOAT, 'cn': 'kdjk', 'size': 70},
                                'kdjd': {'type': FLOAT, 'cn': 'kdjd', 'size': 70},
                                'kdjj': {'type': FLOAT, 'cn': 'kdjj', 'size': 70},
                                'boll_ub': {'type': FLOAT, 'cn': 'boll上轨', 'size': 70},
                                'boll': {'type': FLOAT, 'cn': 'boll', 'size': 70},
                                'boll_lb': {'type': FLOAT, 'cn': 'boll下轨', 'size': 70},
                                'trix': {'type': FLOAT, 'cn': 'trix', 'size': 70},
                                'trix_20_sma': {'type': FLOAT, 'cn': 'trma', 'size': 70},
                                'tema': {'type': FLOAT, 'cn': 'tema', 'size': 70},
                                'cr': {'type': FLOAT, 'cn': 'cr', 'size': 70},
                                'cr-ma1': {'type': FLOAT, 'cn': 'cr-ma1', 'size': 70},
                                'cr-ma2': {'type': FLOAT, 'cn': 'cr-ma2', 'size': 70},
                                'cr-ma3': {'type': FLOAT, 'cn': 'cr-ma3', 'size': 70},
                                'rsi_6': {'type': FLOAT, 'cn': 'rsi_6', 'size': 70},
                                'rsi_12': {'type': FLOAT, 'cn': 'rsi_12', 'size': 70},
                                'rsi': {'type': FLOAT, 'cn': 'rsi', 'size': 70},
                                'rsi_24': {'type': FLOAT, 'cn': 'rsi_24', 'size': 70},
                                'vr': {'type': FLOAT, 'cn': 'vr', 'size': 70},
                                'vr_6_sma': {'type': FLOAT, 'cn': 'mavr', 'size': 70},
                                'roc': {'type': FLOAT, 'cn': 'roc', 'size': 70},
                                'rocma': {'type': FLOAT, 'cn': 'rocma', 'size': 70},
                                'rocema': {'type': FLOAT, 'cn': 'rocema', 'size': 70},
                                'pdi': {'type': FLOAT, 'cn': 'pdi', 'size': 70},
                                'mdi': {'type': FLOAT, 'cn': 'mdi', 'size': 70},
                                'dx': {'type': FLOAT, 'cn': 'dx', 'size': 70},
                                'adx': {'type': FLOAT, 'cn': 'adx', 'size': 70},
                                'adxr': {'type': FLOAT, 'cn': 'adxr', 'size': 70},
                                'wr_6': {'type': FLOAT, 'cn': 'wr_6', 'size': 70},
                                'wr_10': {'type': FLOAT, 'cn': 'wr_10', 'size': 70},
                                'wr_14': {'type': FLOAT, 'cn': 'wr_14', 'size': 70},
                                'cci': {'type': FLOAT, 'cn': 'cci', 'size': 70},
                                'cci_84': {'type': FLOAT, 'cn': 'cci_84', 'size': 70},
                                'tr': {'type': FLOAT, 'cn': 'tr', 'size': 70},
                                'atr': {'type': FLOAT, 'cn': 'atr', 'size': 70},
                                'dma': {'type': FLOAT, 'cn': 'dma', 'size': 70},
                                'dma_10_sma': {'type': FLOAT, 'cn': 'ama', 'size': 70},
                                'obv': {'type': FLOAT, 'cn': 'obv', 'size': 70},
                                'sar': {'type': FLOAT, 'cn': 'sar', 'size': 70},
                                'psy': {'type': FLOAT, 'cn': 'psy', 'size': 70},
                                'psyma': {'type': FLOAT, 'cn': 'psyma', 'size': 70},
                                'br': {'type': FLOAT, 'cn': 'br', 'size': 70},
                                'ar': {'type': FLOAT, 'cn': 'ar', 'size': 70},
                                'emv': {'type': FLOAT, 'cn': 'emv', 'size': 70},
                                'emva': {'type': FLOAT, 'cn': 'emva', 'size': 70},
                                'bias': {'type': FLOAT, 'cn': 'bias', 'size': 70},
                                'mfi': {'type': FLOAT, 'cn': 'mfi', 'size': 70},
                                'mfisma': {'type': FLOAT, 'cn': 'mfisma', 'size': 70},
                                'vwma': {'type': FLOAT, 'cn': 'vwma', 'size': 70},
                                'mvwma': {'type': FLOAT, 'cn': 'mvwma', 'size': 70},
                                'ppo': {'type': FLOAT, 'cn': 'ppo', 'size': 70},
                                'ppos': {'type': FLOAT, 'cn': 'ppos', 'size': 70},
                                'ppoh': {'type': FLOAT, 'cn': 'ppoh', 'size': 70},
                                'wt1': {'type': FLOAT, 'cn': 'wt1', 'size': 70},
                                'wt2': {'type': FLOAT, 'cn': 'wt2', 'size': 70},
                                'supertrend_ub': {'type': FLOAT, 'cn': 'supertrend_ub', 'size': 70},
                                'supertrend': {'type': FLOAT, 'cn': 'supertrend', 'size': 70},
                                'supertrend_lb': {'type': FLOAT, 'cn': 'supertrend_lb', 'size': 70},
                                'dpo': {'type': FLOAT, 'cn': 'dpo', 'size': 70},
                                'madpo': {'type': FLOAT, 'cn': 'madpo', 'size': 70},
                                'vhf': {'type': FLOAT, 'cn': 'vhf', 'size': 70},
                                'rvi': {'type': FLOAT, 'cn': 'rvi', 'size': 70},
                                'rvis': {'type': FLOAT, 'cn': 'rvis', 'size': 70},
                                'fi': {'type': FLOAT, 'cn': 'fi', 'size': 70},
                                'force_2': {'type': FLOAT, 'cn': 'force_2', 'size': 70},
                                'force_13': {'type': FLOAT, 'cn': 'force_13', 'size': 70},
                                'ene_ue': {'type': FLOAT, 'cn': 'ene上轨', 'size': 70},
                                'ene': {'type': FLOAT, 'cn': 'ene', 'size': 70},
                                'ene_le': {'type': FLOAT, 'cn': 'ene下轨', 'size': 70},
                                'stochrsi_k': {'type': FLOAT, 'cn': 'stochrsi_k', 'size': 70},
                                'stochrsi_d': {'type': FLOAT, 'cn': 'stochrsi_d', 'size': 70}}}

TABLE_CN_STOCK_INDICATORS = {'name': 'cn_stock_indicators', 'cn': '股票指标数据',
                             'columns': TABLE_CN_STOCK_FOREIGN_KEY['columns'].copy()}
TABLE_CN_STOCK_INDICATORS['columns'].update(STOCK_STATS_DATA['columns'])

_tmp_columns = TABLE_CN_STOCK_FOREIGN_KEY['columns'].copy()
_tmp_columns.update(TABLE_CN_STOCK_BACKTEST_DATA['columns'])

TABLE_CN_STOCK_INDICATORS_BUY = {'name': 'cn_stock_indicators_buy', 'cn': '股票指标买入',
                                 'columns': _tmp_columns}

TABLE_CN_STOCK_INDICATORS_SELL = {'name': 'cn_stock_indicators_sell', 'cn': '股票指标卖出',
                                  'columns': _tmp_columns}

TABLE_CN_STOCK_STRATEGIES = [
    {'name': 'cn_stock_strategy_enter', 'cn': '放量上涨', 'size': 70, 'func': enter.check_volume,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_keep_increasing', 'cn': '均线多头', 'size': 70, 'func': keep_increasing.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_parking_apron', 'cn': '停机坪', 'size': 70, 'func': parking_apron.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_backtrace_ma250', 'cn': '回踩年线', 'size': 70, 'func': backtrace_ma250.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_breakthrough_platform', 'cn': '突破平台', 'size': 70,
     'func': breakthrough_platform.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_low_backtrace_increase', 'cn': '无大幅回撤', 'size': 70,
     'func': low_backtrace_increase.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_turtle_trade', 'cn': '海龟交易法则', 'size': 70, 'func': turtle_trade.check_enter,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_high_tight_flag', 'cn': '高而窄的旗形', 'size': 70,
     'func': high_tight_flag.check_high_tight,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_climax_limitdown', 'cn': '放量跌停', 'size': 70, 'func': climax_limitdown.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_low_atr', 'cn': '低ATR成长', 'size': 70, 'func': low_atr.check_low_increase,
     'columns': _tmp_columns}
]

STOCK_KLINE_PATTERN_DATA = {'name': 'cn_stock_pattern_recognitions', 'cn': 'K线形态',
                            'columns': {
                                'tow_crows': {'type': SmallInteger, 'cn': '两只乌鸦', 'size': 70, 'func': tl.CDL2CROWS},
                                'upside_gap_two_crows': {'type': SmallInteger, 'cn': '向上跳空的两只乌鸦', 'size': 70,
                                                         'func': tl.CDLUPSIDEGAP2CROWS},
                                'three_black_crows': {'type': SmallInteger, 'cn': '三只乌鸦', 'size': 70,
                                                      'func': tl.CDL3BLACKCROWS},
                                'identical_three_crows': {'type': SmallInteger, 'cn': '三胞胎乌鸦', 'size': 70,
                                                          'func': tl.CDLIDENTICAL3CROWS},
                                'three_line_strike': {'type': SmallInteger, 'cn': '三线打击', 'size': 70,
                                                      'func': tl.CDL3LINESTRIKE},
                                'dark_cloud_cover': {'type': SmallInteger, 'cn': '乌云压顶', 'size': 70,
                                                     'func': tl.CDLDARKCLOUDCOVER},
                                'evening_doji_star': {'type': SmallInteger, 'cn': '十字暮星', 'size': 70,
                                                      'func': tl.CDLEVENINGDOJISTAR},
                                'doji_Star': {'type': SmallInteger, 'cn': '十字星', 'size': 70, 'func': tl.CDLDOJISTAR},
                                'hanging_man': {'type': SmallInteger, 'cn': '上吊线', 'size': 70,
                                                'func': tl.CDLHANGINGMAN},
                                'hikkake_pattern': {'type': SmallInteger, 'cn': '陷阱', 'size': 70,
                                                    'func': tl.CDLHIKKAKE},
                                'modified_hikkake_pattern': {'type': SmallInteger, 'cn': '修正陷阱', 'size': 70,
                                                             'func': tl.CDLHIKKAKEMOD},
                                'in_neck_pattern': {'type': SmallInteger, 'cn': '颈内线', 'size': 70,
                                                    'func': tl.CDLINNECK},
                                'on_neck_pattern': {'type': SmallInteger, 'cn': '颈上线', 'size': 70,
                                                    'func': tl.CDLONNECK},
                                'thrusting_pattern': {'type': SmallInteger, 'cn': '插入', 'size': 70,
                                                      'func': tl.CDLTHRUSTING},
                                'shooting_star': {'type': SmallInteger, 'cn': '射击之星', 'size': 70,
                                                  'func': tl.CDLSHOOTINGSTAR},
                                'stalled_pattern': {'type': SmallInteger, 'cn': '停顿形态', 'size': 70,
                                                    'func': tl.CDLSTALLEDPATTERN},
                                'advance_block': {'type': SmallInteger, 'cn': '大敌当前', 'size': 70,
                                                  'func': tl.CDLADVANCEBLOCK},
                                'high_wave_candle': {'type': SmallInteger, 'cn': '风高浪大线', 'size': 70,
                                                     'func': tl.CDLHIGHWAVE},
                                'engulfing_pattern': {'type': SmallInteger, 'cn': '吞噬模式', 'size': 70,
                                                      'func': tl.CDLENGULFING},
                                'abandoned_baby': {'type': SmallInteger, 'cn': '弃婴', 'size': 70,
                                                   'func': tl.CDLABANDONEDBABY},
                                'closing_marubozu': {'type': SmallInteger, 'cn': '收盘缺影线', 'size': 70,
                                                     'func': tl.CDLCLOSINGMARUBOZU},
                                'doji': {'type': SmallInteger, 'cn': '十字', 'size': 70, 'func': tl.CDLDOJI},
                                'up_down_gap': {'type': SmallInteger, 'cn': '向上/下跳空并列阳线', 'size': 70,
                                                'func': tl.CDLGAPSIDESIDEWHITE},
                                'long_legged_doji': {'type': SmallInteger, 'cn': '长脚十字', 'size': 70,
                                                     'func': tl.CDLLONGLEGGEDDOJI},
                                'rickshaw_man': {'type': SmallInteger, 'cn': '黄包车夫', 'size': 70,
                                                 'func': tl.CDLRICKSHAWMAN},
                                'marubozu': {'type': SmallInteger, 'cn': '光头光脚/缺影线', 'size': 70,
                                             'func': tl.CDLMARUBOZU},
                                'three_inside_up_down': {'type': SmallInteger, 'cn': '三内部上涨和下跌', 'size': 70,
                                                         'func': tl.CDL3INSIDE},
                                'three_outside_up_down': {'type': SmallInteger, 'cn': '三外部上涨和下跌', 'size': 70,
                                                          'func': tl.CDL3OUTSIDE},
                                'three_stars_in_the_south': {'type': SmallInteger, 'cn': '南方三星', 'size': 70,
                                                             'func': tl.CDL3STARSINSOUTH},
                                'three_white_soldiers': {'type': SmallInteger, 'cn': '三个白兵', 'size': 70,
                                                         'func': tl.CDL3WHITESOLDIERS},
                                'belt_hold': {'type': SmallInteger, 'cn': '捉腰带线', 'size': 70,
                                              'func': tl.CDLBELTHOLD},
                                'breakaway': {'type': SmallInteger, 'cn': '脱离', 'size': 70, 'func': tl.CDLBREAKAWAY},
                                'concealing_baby_swallow': {'type': SmallInteger, 'cn': '藏婴吞没', 'size': 70,
                                                            'func': tl.CDLCONCEALBABYSWALL},
                                'counterattack': {'type': SmallInteger, 'cn': '反击线', 'size': 70,
                                                  'func': tl.CDLCOUNTERATTACK},
                                'dragonfly_doji': {'type': SmallInteger, 'cn': '蜻蜓十字/T形十字', 'size': 70,
                                                   'func': tl.CDLDRAGONFLYDOJI},
                                'evening_star': {'type': SmallInteger, 'cn': '暮星', 'size': 70,
                                                 'func': tl.CDLEVENINGSTAR},
                                'gravestone_doji': {'type': SmallInteger, 'cn': '墓碑十字/倒T十字', 'size': 70,
                                                    'func': tl.CDLGRAVESTONEDOJI},
                                'hammer': {'type': SmallInteger, 'cn': '锤头', 'size': 70, 'func': tl.CDLHAMMER},
                                'harami_pattern': {'type': SmallInteger, 'cn': '母子线', 'size': 70,
                                                   'func': tl.CDLHARAMI},
                                'harami_cross_pattern': {'type': SmallInteger, 'cn': '十字孕线', 'size': 70,
                                                         'func': tl.CDLHARAMICROSS},
                                'homing_pigeon': {'type': SmallInteger, 'cn': '家鸽', 'size': 70,
                                                  'func': tl.CDLHOMINGPIGEON},
                                'inverted_hammer': {'type': SmallInteger, 'cn': '倒锤头', 'size': 70,
                                                    'func': tl.CDLINVERTEDHAMMER},
                                'kicking': {'type': SmallInteger, 'cn': '反冲形态', 'size': 70, 'func': tl.CDLKICKING},
                                'kicking_bull_bear': {'type': SmallInteger, 'cn': '由较长缺影线决定的反冲形态',
                                                      'size': 70,
                                                      'func': tl.CDLKICKINGBYLENGTH},
                                'ladder_bottom': {'type': SmallInteger, 'cn': '梯底', 'size': 70,
                                                  'func': tl.CDLLADDERBOTTOM},
                                'long_line_candle': {'type': SmallInteger, 'cn': '长蜡烛', 'size': 70,
                                                     'func': tl.CDLLONGLINE},
                                'matching_low': {'type': SmallInteger, 'cn': '相同低价', 'size': 70,
                                                 'func': tl.CDLMATCHINGLOW},
                                'mat_hold': {'type': SmallInteger, 'cn': '铺垫', 'size': 70, 'func': tl.CDLMATHOLD},
                                'morning_doji_star': {'type': SmallInteger, 'cn': '十字晨星', 'size': 70,
                                                      'func': tl.CDLMORNINGDOJISTAR},
                                'morning_star': {'type': SmallInteger, 'cn': '晨星', 'size': 70,
                                                 'func': tl.CDLMORNINGSTAR},
                                'piercing_pattern': {'type': SmallInteger, 'cn': '刺透形态', 'size': 70,
                                                     'func': tl.CDLPIERCING},
                                'rising_falling_three': {'type': SmallInteger, 'cn': '上升/下降三法', 'size': 70,
                                                         'func': tl.CDLRISEFALL3METHODS},
                                'separating_lines': {'type': SmallInteger, 'cn': '分离线', 'size': 70,
                                                     'func': tl.CDLSEPARATINGLINES},
                                'short_line_candle': {'type': SmallInteger, 'cn': '短蜡烛', 'size': 70,
                                                      'func': tl.CDLSHORTLINE},
                                'spinning_top': {'type': SmallInteger, 'cn': '纺锤', 'size': 70,
                                                 'func': tl.CDLSPINNINGTOP},
                                'stick_sandwich': {'type': SmallInteger, 'cn': '条形三明治', 'size': 70,
                                                   'func': tl.CDLSTICKSANDWICH},
                                'takuri': {'type': SmallInteger, 'cn': '探水竿', 'size': 70, 'func': tl.CDLTAKURI},
                                'tasuki_gap': {'type': SmallInteger, 'cn': '跳空并列阴阳线', 'size': 70,
                                               'func': tl.CDLTASUKIGAP},
                                'tristar_pattern': {'type': SmallInteger, 'cn': '三星', 'size': 70,
                                                    'func': tl.CDLTRISTAR},
                                'unique_3_river': {'type': SmallInteger, 'cn': '奇特三河床', 'size': 70,
                                                   'func': tl.CDLUNIQUE3RIVER},
                                'upside_downside_gap': {'type': SmallInteger, 'cn': '上升/下降跳空三法', 'size': 70,
                                                        'func': tl.CDLXSIDEGAP3METHODS}}}

TABLE_CN_STOCK_KLINE_PATTERN = {'name': 'cn_stock_pattern', 'cn': '股票K线形态',
                                'columns': TABLE_CN_STOCK_FOREIGN_KEY['columns'].copy()}
TABLE_CN_STOCK_KLINE_PATTERN['columns'].update(STOCK_KLINE_PATTERN_DATA['columns'])


def get_field_cn(key, table):
    f = table.get('columns').get(key)
    if f is None:
        return key
    return f.get('cn')


def get_field_cns(cols):
    data = []
    for k in cols:
        data.append(cols[k]['cn'])
    return data


def get_field_types(cols):
    data = {}
    for k in cols:
        data[k] = cols[k]['type']
    return data


def get_field_type_name(col_type):
    if col_type == DATE:
        return "datetime"
    elif col_type == FLOAT or col_type == BIGINT or col_type == SmallInteger or col_type == BIT:
        return "numeric"
    else:
        return "string"
