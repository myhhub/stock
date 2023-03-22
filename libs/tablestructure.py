#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlalchemy import DATE, NVARCHAR, FLOAT
from strategy import enter
from strategy import turtle_trade
from strategy import climax_limitdown
from strategy import low_atr
from strategy import backtrace_ma250
from strategy import breakthrough_platform
from strategy import parking_apron
from strategy import low_backtrace_increase
from strategy import keep_increasing
from strategy import high_tight_flag

__author__ = 'myh '
__date__ = '2023/3/10 '

RATE_FIELDS_COUNT = 100  # N日收益率字段数目，即N值

TABLE_CN_STOCK_SPOT = {'name': 'cn_stock_spot', 'cn': '每日股票数据',
                       'columns': {'date': (DATE, '日期'), 'code': (NVARCHAR(6), '代码'),
                                   'name': (NVARCHAR(20), '名称'), 'latest_price': (FLOAT, '最新价'),
                                   'quote_change': (FLOAT, '涨跌幅'), 'ups_downs': (FLOAT, '涨跌额'),
                                   'volume': (FLOAT, '成交量'), 'turnover': (FLOAT, '成交额'),
                                   'amplitude': (FLOAT, '振幅'), 'high': (FLOAT, '最高'),
                                   'low': (FLOAT, '最低'), 'open': (FLOAT, '今开'),
                                   'closed': (FLOAT, '昨收'), 'quantity_ratio': (FLOAT, '量比'),
                                   'turnover_rate': (FLOAT, '换手率'), 'pe_dynamic': (FLOAT, '动态市盈率'),
                                   'pb': (FLOAT, '市净率'), 'value_total': (FLOAT, '总市值'),
                                   'value_liquidity': (FLOAT, '流通市值'), 'speed_increase': (FLOAT, '涨速'),
                                   'speed_increase_5': (FLOAT, '5分钟涨跌'), 'speed_increase_60': (FLOAT, '60日涨跌幅'),
                                   'speed_increase_all': (FLOAT, '年初至今涨跌幅')}}

TABLE_CN_STOCK_TOP = {'name': 'cn_stock_top', 'cn': '龙虎榜',
                      'columns': {'date': (DATE, '日期'), 'code': (NVARCHAR(6), '代码'),
                                  'name': (NVARCHAR(20), '名称'), 'ranking_times': (FLOAT, '上榜次数'),
                                  'sum_buy': (FLOAT, '累积购买额'), 'sum_sell': (FLOAT, '累积卖出额'),
                                  'net_amount': (FLOAT, '净额'), 'buy_seat': (FLOAT, '买入席位数'),
                                  'sell_seat': (FLOAT, '卖出席位数')}}

TABLE_CN_STOCK_BLOCKTRADE = {'name': 'cn_stock_blocktrade', 'cn': '大宗交易',
                             'columns': {'date': (DATE, '日期'), 'code': (NVARCHAR(6), '代码'),
                                         'name': (NVARCHAR(20), '名称'), 'quote_change': (FLOAT, '涨跌幅'),
                                         'close_price': (FLOAT, '收盘价'), 'average_price': (FLOAT, '成交均价'),
                                         'overflow_rate': (FLOAT, '折溢率'), 'trade_number': (FLOAT, '成交笔数'),
                                         'sum_volume': (FLOAT, '成交总量'), 'sum_turnover': (FLOAT, '成交总额'),
                                         'turnover_market_rate': (FLOAT, '成交总额/流通市值')}}

CN_STOCK_HIST_DATA = {'name': 'stock_zh_a_hist', 'cn': '股票某时间段的日行情数据库',
                      'columns': {'date': (DATE, '日期'), 'open': (FLOAT, '开盘价'),
                                  'close': (FLOAT, '收盘价'), 'high': (FLOAT, '最高'),
                                  'low': (FLOAT, '最低'), 'volume': (FLOAT, '成交量'),
                                  'amount': (FLOAT, '成交额'), 'amplitude': (FLOAT, '振幅'),
                                  'quote_change': (FLOAT, '涨跌幅'), 'ups_downs': (FLOAT, '涨跌额'),
                                  'turnover': (FLOAT, '换手率')}}

TABLE_CN_STOCK_FOREIGN_KEY = {'name': 'cn_stock_foreign_key', 'cn': '股票外键',
                              'columns': {'date': (DATE, '日期'), 'code': (NVARCHAR(6), '代码'),
                                          'name': (NVARCHAR(20), '名称')}}

TABLE_CN_STOCK_BACKTEST_DATA = {'name': 'cn_stock_backtest_data', 'cn': '股票回归测试数据',
                                'columns': {'rate_%s' % i: (FLOAT, '%s日收益率' % i) for i in
                                            range(1, RATE_FIELDS_COUNT + 1, 1)}}

TABLE_CN_STOCK_HIST = {'name': 'cn_stock_hist', 'cn': '股票日行情数据',
                       'columns': TABLE_CN_STOCK_FOREIGN_KEY.copy()}
_tmp_data_ = CN_STOCK_HIST_DATA['columns'].copy()
_tmp_data_.pop('date')
TABLE_CN_STOCK_HIST['columns'].update(_tmp_data_)

STOCK_STATS_DATA = {'name': 'stockstats', 'cn': '股票统计/指标计算助手库',
                    'columns': {'adx': (FLOAT, 'adx'), 'adxr': (FLOAT, 'adxr'),
                                'boll': (FLOAT, 'boll'), 'boll_lb': (FLOAT, 'boll_lb'),
                                'boll_ub': (FLOAT, 'boll_ub'), 'cci': (FLOAT, 'cci'),
                                'cci_20': (FLOAT, 'cci_20'), 'close_-1_r': (FLOAT, 'close_-1_r'),
                                'close_-2_r': (FLOAT, 'close_-2_r'), 'cr': (FLOAT, 'cr'),
                                'cr-ma1': (FLOAT, 'cr-ma1'), 'cr-ma2': (FLOAT, 'cr-ma2'),
                                'cr-ma3': (FLOAT, 'cr-ma3 '), 'dma': (FLOAT, 'dma'),
                                'dx': (FLOAT, 'dx'), 'kdjd': (FLOAT, 'kdjd'),
                                'kdjj': (FLOAT, 'kdjj'), 'kdjk': (FLOAT, 'kdjk'),
                                'macd': (FLOAT, 'macd'), 'macdh': (FLOAT, 'macdh'),
                                'macds': (FLOAT, 'macds'), 'mdi': (FLOAT, 'mdi'),
                                'pdi': (FLOAT, 'pdi'), 'rsi_12': (FLOAT, 'rsi_12'),
                                'rsi_6': (FLOAT, 'rsi_6'), 'trix': (FLOAT, 'trix'),
                                'trix_9_sma': (FLOAT, 'trix_9_sma'), 'vr': (FLOAT, 'vr'),
                                'vr_6_sma': (FLOAT, 'vr_6_sma'), 'wr_10': (FLOAT, 'wr_10'),
                                'wr_6': (FLOAT, 'wr_6')}}

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
    {'name': 'cn_stock_strategy_enter', 'cn': '放量上涨', 'func': enter.check_volume,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_keep_increasing', 'cn': '均线多头', 'func': keep_increasing.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_parking_apron', 'cn': '停机坪', 'func': parking_apron.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_backtrace_ma250', 'cn': '回踩年线', 'func': backtrace_ma250.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_breakthrough_platform', 'cn': '突破平台', 'func': breakthrough_platform.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_low_backtrace_increase', 'cn': '无大幅回撤', 'func': low_backtrace_increase.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_turtle_trade', 'cn': '海龟交易法则', 'func': turtle_trade.check_enter,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_high_tight_flag', 'cn': '高而窄的旗形', 'func': high_tight_flag.check_high_tight,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_climax_limitdown', 'cn': '放量跌停', 'func': climax_limitdown.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_low_atr', 'cn': '低ATR成长', 'func': low_atr.check_low_increase,
     'columns': _tmp_columns}
]


def get_cols_cn(cols_val):
    data = []
    for v in cols_val:
        data.append(v[1])
    return data


def get_cols_type(cols):
    data = {}
    for k, v in cols.items():
        data[k] = v[0]
    return data
