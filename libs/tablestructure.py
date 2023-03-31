#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlalchemy import DATE, NVARCHAR, FLOAT, SmallInteger
import talib as tl
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
                       'columns': {'date': {'type': DATE, 'cn': '日期'}, 'code': {'type': NVARCHAR(6), 'cn': '代码'},
                                   'name': {'type': NVARCHAR(20), 'cn': '名称'},
                                   'latest_price': {'type': FLOAT, 'cn': '最新价'},
                                   'quote_change': {'type': FLOAT, 'cn': '涨跌幅'},
                                   'ups_downs': {'type': FLOAT, 'cn': '涨跌额'},
                                   'volume': {'type': FLOAT, 'cn': '成交量'},
                                   'turnover': {'type': FLOAT, 'cn': '成交额'},
                                   'amplitude': {'type': FLOAT, 'cn': '振幅'}, 'high': {'type': FLOAT, 'cn': '最高'},
                                   'low': {'type': FLOAT, 'cn': '最低'}, 'open': {'type': FLOAT, 'cn': '今开'},
                                   'closed': {'type': FLOAT, 'cn': '昨收'},
                                   'quantity_ratio': {'type': FLOAT, 'cn': '量比'},
                                   'turnover_rate': {'type': FLOAT, 'cn': '换手率'},
                                   'pe_dynamic': {'type': FLOAT, 'cn': '动态市盈率'},
                                   'pb': {'type': FLOAT, 'cn': '市净率'},
                                   'value_total': {'type': FLOAT, 'cn': '总市值'},
                                   'value_liquidity': {'type': FLOAT, 'cn': '流通市值'},
                                   'speed_increase': {'type': FLOAT, 'cn': '涨速'},
                                   'speed_increase_5': {'type': FLOAT, 'cn': '5分钟涨跌'},
                                   'speed_increase_60': {'type': FLOAT, 'cn': '60日涨跌幅'},
                                   'speed_increase_all': {'type': FLOAT, 'cn': '年初至今涨跌幅'}}}

TABLE_CN_STOCK_TOP = {'name': 'cn_stock_top', 'cn': '龙虎榜',
                      'columns': {'date': {'type': DATE, 'cn': '日期'}, 'code': {'type': NVARCHAR(6), 'cn': '代码'},
                                  'name': {'type': NVARCHAR(20), 'cn': '名称'},
                                  'ranking_times': {'type': FLOAT, 'cn': '上榜次数'},
                                  'sum_buy': {'type': FLOAT, 'cn': '累积购买额'},
                                  'sum_sell': {'type': FLOAT, 'cn': '累积卖出额'},
                                  'net_amount': {'type': FLOAT, 'cn': '净额'},
                                  'buy_seat': {'type': FLOAT, 'cn': '买入席位数'},
                                  'sell_seat': {'type': FLOAT, 'cn': '卖出席位数'}}}

TABLE_CN_STOCK_BLOCKTRADE = {'name': 'cn_stock_blocktrade', 'cn': '大宗交易',
                             'columns': {'date': {'type': DATE, 'cn': '日期'},
                                         'code': {'type': NVARCHAR(6), 'cn': '代码'},
                                         'name': {'type': NVARCHAR(20), 'cn': '名称'},
                                         'quote_change': {'type': FLOAT, 'cn': '涨跌幅'},
                                         'close_price': {'type': FLOAT, 'cn': '收盘价'},
                                         'average_price': {'type': FLOAT, 'cn': '成交均价'},
                                         'overflow_rate': {'type': FLOAT, 'cn': '折溢率'},
                                         'trade_number': {'type': FLOAT, 'cn': '成交笔数'},
                                         'sum_volume': {'type': FLOAT, 'cn': '成交总量'},
                                         'sum_turnover': {'type': FLOAT, 'cn': '成交总额'},
                                         'turnover_market_rate': {'type': FLOAT, 'cn': '成交总额/流通市值'}}}

CN_STOCK_HIST_DATA = {'name': 'stock_zh_a_hist', 'cn': '股票某时间段的日行情数据库',
                      'columns': {'date': {'type': DATE, 'cn': '日期'}, 'open': {'type': FLOAT, 'cn': '开盘价'},
                                  'close': {'type': FLOAT, 'cn': '收盘价'}, 'high': {'type': FLOAT, 'cn': '最高'},
                                  'low': {'type': FLOAT, 'cn': '最低'}, 'volume': {'type': FLOAT, 'cn': '成交量'},
                                  'amount': {'type': FLOAT, 'cn': '成交额'}, 'amplitude': {'type': FLOAT, 'cn': '振幅'},
                                  'quote_change': {'type': FLOAT, 'cn': '涨跌幅'},
                                  'ups_downs': {'type': FLOAT, 'cn': '涨跌额'},
                                  'turnover': {'type': FLOAT, 'cn': '换手率'}}}

TABLE_CN_STOCK_FOREIGN_KEY = {'name': 'cn_stock_foreign_key', 'cn': '股票外键',
                              'columns': {'date': {'type': DATE, 'cn': '日期'},
                                          'code': {'type': NVARCHAR(6), 'cn': '代码'},
                                          'name': {'type': NVARCHAR(20), 'cn': '名称'}}}

TABLE_CN_STOCK_BACKTEST_DATA = {'name': 'cn_stock_backtest_data', 'cn': '股票回归测试数据',
                                'columns': {'rate_%s' % i: {'type': FLOAT, 'cn': '%s日收益率' % i} for i in
                                            range(1, RATE_FIELDS_COUNT + 1, 1)}}

TABLE_CN_STOCK_HIST = {'name': 'cn_stock_hist', 'cn': '股票日行情数据',
                       'columns': TABLE_CN_STOCK_FOREIGN_KEY.copy()}
_tmp_data_ = CN_STOCK_HIST_DATA['columns'].copy()
_tmp_data_.pop('date')
TABLE_CN_STOCK_HIST['columns'].update(_tmp_data_)

STOCK_STATS_DATA = {'name': 'stockstats', 'cn': '股票统计/指标计算助手库',
                    'columns': {'macd': {'type': FLOAT, 'cn': 'dif'}, 'macds': {'type': FLOAT, 'cn': 'macd'},
                                'macdh': {'type': FLOAT, 'cn': 'histogram'},
                                'kdjk': {'type': FLOAT, 'cn': 'kdjk'}, 'kdjd': {'type': FLOAT, 'cn': 'kdjd'},
                                'kdjj': {'type': FLOAT, 'cn': 'kdjj'},
                                'boll_ub': {'type': FLOAT, 'cn': 'boll上轨线'},
                                'boll': {'type': FLOAT, 'cn': 'boll中轨线'},
                                'boll_lb': {'type': FLOAT, 'cn': 'boll下轨线'},
                                'trix': {'type': FLOAT, 'cn': 'trix'}, 'trix_20_sma': {'type': FLOAT, 'cn': 'trma'},
                                'cr': {'type': FLOAT, 'cn': 'cr'}, 'cr-ma1': {'type': FLOAT, 'cn': 'cr-a'},
                                'cr-ma2': {'type': FLOAT, 'cn': 'cr-b'}, 'cr-ma3': {'type': FLOAT, 'cn': 'cr-c '},
                                'rsi_6': {'type': FLOAT, 'cn': 'rsi_6'}, 'rsi_12': {'type': FLOAT, 'cn': 'rsi_12'},
                                'rsi': {'type': FLOAT, 'cn': 'rsi'}, 'rsi_24': {'type': FLOAT, 'cn': 'rsi_24'},
                                'vr': {'type': FLOAT, 'cn': 'vr'}, 'vr_6_sma': {'type': FLOAT, 'cn': 'mavr'},
                                'roc': {'type': FLOAT, 'cn': 'roc'}, 'rocma': {'type': FLOAT, 'cn': 'rocma'},
                                'rocema': {'type': FLOAT, 'cn': 'rocema'},
                                'pdi': {'type': FLOAT, 'cn': 'pdi'}, 'mdi': {'type': FLOAT, 'cn': 'mdi'},
                                'dx': {'type': FLOAT, 'cn': 'dx'},
                                'adx': {'type': FLOAT, 'cn': 'adx'}, 'adxr': {'type': FLOAT, 'cn': 'adxr'},
                                'wr_6': {'type': FLOAT, 'cn': 'wr_6'}, 'wr_10': {'type': FLOAT, 'cn': 'wr_10'},
                                'wr_14': {'type': FLOAT, 'cn': 'wr_14'},
                                'cci': {'type': FLOAT, 'cn': 'cci'}, 'cci_84': {'type': FLOAT, 'cn': 'cci_84'},
                                'tr': {'type': FLOAT, 'cn': 'tr'}, 'atr': {'type': FLOAT, 'cn': 'atr'},
                                'dma': {'type': FLOAT, 'cn': 'dma'}, 'dma_10_sma': {'type': FLOAT, 'cn': 'ama'},
                                'obv': {'type': FLOAT, 'cn': 'obv'}, 'sar': {'type': FLOAT, 'cn': 'sar'},
                                'psy': {'type': FLOAT, 'cn': 'psy'},
                                'br': {'type': FLOAT, 'cn': 'br'}, 'ar': {'type': FLOAT, 'cn': 'ar'},
                                'emv': {'type': FLOAT, 'cn': 'emv'}, 'emva': {'type': FLOAT, 'cn': 'emva'},
                                'bias': {'type': FLOAT, 'cn': 'bias'},
                                'bias_12': {'type': FLOAT, 'cn': 'bias_12'}, 'bias_24': {'type': FLOAT, 'cn': 'bias_24'}
                                }}

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

STOCK_KLINE_PATTERN_DATA = {'name': 'cn_stock_pattern_recognitions', 'cn': 'K线形态',
                            'columns': {
                                'tow_crows': {'type': SmallInteger, 'cn': '两只乌鸦', 'func': tl.CDL2CROWS},
                                'upside_gap_two_crows': {'type': SmallInteger, 'cn': '向上跳空的两只乌鸦',
                                                         'func': tl.CDLUPSIDEGAP2CROWS},
                                'three_black_crows': {'type': SmallInteger, 'cn': '三只乌鸦',
                                                      'func': tl.CDL3BLACKCROWS},
                                'identical_three_crows': {'type': SmallInteger, 'cn': '三胞胎乌鸦',
                                                          'func': tl.CDLIDENTICAL3CROWS},
                                'three_line_strike': {'type': SmallInteger, 'cn': '三线打击',
                                                      'func': tl.CDL3LINESTRIKE},
                                'dark_cloud_cover': {'type': SmallInteger, 'cn': '乌云压顶',
                                                     'func': tl.CDLDARKCLOUDCOVER},
                                'evening_doji_star': {'type': SmallInteger, 'cn': '十字暮星',
                                                      'func': tl.CDLEVENINGDOJISTAR},
                                'doji_Star': {'type': SmallInteger, 'cn': '十字星', 'func': tl.CDLDOJISTAR},
                                'hanging_man': {'type': SmallInteger, 'cn': '上吊线', 'func': tl.CDLHANGINGMAN},
                                'hikkake_pattern': {'type': SmallInteger, 'cn': '陷阱', 'func': tl.CDLHIKKAKE},
                                'modified_hikkake_pattern': {'type': SmallInteger, 'cn': '修正陷阱',
                                                             'func': tl.CDLHIKKAKEMOD},
                                'in_neck_pattern': {'type': SmallInteger, 'cn': '颈内线', 'func': tl.CDLINNECK},
                                'on_neck_pattern': {'type': SmallInteger, 'cn': '颈上线', 'func': tl.CDLONNECK},
                                'thrusting_pattern': {'type': SmallInteger, 'cn': '插入', 'func': tl.CDLTHRUSTING},
                                'shooting_star': {'type': SmallInteger, 'cn': '射击之星',
                                                  'func': tl.CDLSHOOTINGSTAR},
                                'stalled_pattern': {'type': SmallInteger, 'cn': '停顿形态',
                                                    'func': tl.CDLSTALLEDPATTERN},
                                'advance_block': {'type': SmallInteger, 'cn': '大敌当前',
                                                  'func': tl.CDLADVANCEBLOCK},
                                'high_wave_candle': {'type': SmallInteger, 'cn': '风高浪大线',
                                                     'func': tl.CDLHIGHWAVE},
                                'engulfing_pattern': {'type': SmallInteger, 'cn': '吞噬模式',
                                                      'func': tl.CDLENGULFING},
                                'abandoned_baby': {'type': SmallInteger, 'cn': '弃婴',
                                                   'func': tl.CDLABANDONEDBABY},
                                'closing_marubozu': {'type': SmallInteger, 'cn': '收盘缺影线',
                                                     'func': tl.CDLCLOSINGMARUBOZU},
                                'doji': {'type': SmallInteger, 'cn': '十字', 'func': tl.CDLDOJI},
                                'up_down_gap': {'type': SmallInteger, 'cn': '向上/下跳空并列阳线',
                                                'func': tl.CDLGAPSIDESIDEWHITE},
                                'long_legged_doji': {'type': SmallInteger, 'cn': '长脚十字',
                                                     'func': tl.CDLLONGLEGGEDDOJI},
                                'rickshaw_man': {'type': SmallInteger, 'cn': '黄包车夫',
                                                 'func': tl.CDLRICKSHAWMAN},
                                'marubozu': {'type': SmallInteger, 'cn': '光头光脚/缺影线',
                                             'func': tl.CDLMARUBOZU},
                                'three_inside_up_down': {'type': SmallInteger, 'cn': '三内部上涨和下跌',
                                                         'func': tl.CDL3INSIDE},
                                'three_outside_up_down': {'type': SmallInteger, 'cn': '三外部上涨和下跌',
                                                          'func': tl.CDL3OUTSIDE},
                                'three_stars_in_the_south': {'type': SmallInteger, 'cn': '南方三星',
                                                             'func': tl.CDL3STARSINSOUTH},
                                'three_white_soldiers': {'type': SmallInteger, 'cn': '三个白兵',
                                                         'func': tl.CDL3WHITESOLDIERS},
                                'belt_hold': {'type': SmallInteger, 'cn': '捉腰带线', 'func': tl.CDLBELTHOLD},
                                'breakaway': {'type': SmallInteger, 'cn': '脱离', 'func': tl.CDLBREAKAWAY},
                                'concealing_baby_swallow': {'type': SmallInteger, 'cn': '藏婴吞没',
                                                            'func': tl.CDLCONCEALBABYSWALL},
                                'counterattack': {'type': SmallInteger, 'cn': '反击线',
                                                  'func': tl.CDLCOUNTERATTACK},
                                'dragonfly_doji': {'type': SmallInteger, 'cn': '蜻蜓十字/T形十字',
                                                   'func': tl.CDLDRAGONFLYDOJI},
                                'evening_star': {'type': SmallInteger, 'cn': '暮星', 'func': tl.CDLEVENINGSTAR},
                                'gravestone_doji': {'type': SmallInteger, 'cn': '墓碑十字/倒T十字',
                                                    'func': tl.CDLGRAVESTONEDOJI},
                                'hammer': {'type': SmallInteger, 'cn': '锤头', 'func': tl.CDLHAMMER},
                                'harami_pattern': {'type': SmallInteger, 'cn': '母子线', 'func': tl.CDLHARAMI},
                                'harami_cross_pattern': {'type': SmallInteger, 'cn': '十字孕线',
                                                         'func': tl.CDLHARAMICROSS},
                                'homing_pigeon': {'type': SmallInteger, 'cn': '家鸽', 'func': tl.CDLHOMINGPIGEON},
                                'inverted_hammer': {'type': SmallInteger, 'cn': '倒锤头',
                                                    'func': tl.CDLINVERTEDHAMMER},
                                'kicking': {'type': SmallInteger, 'cn': '反冲形态', 'func': tl.CDLKICKING},
                                'kicking_bull_bear': {'type': SmallInteger, 'cn': '由较长缺影线决定的反冲形态',
                                                      'func': tl.CDLKICKINGBYLENGTH},
                                'ladder_bottom': {'type': SmallInteger, 'cn': '梯底', 'func': tl.CDLLADDERBOTTOM},
                                'long_line_candle': {'type': SmallInteger, 'cn': '长蜡烛', 'func': tl.CDLLONGLINE},
                                'matching_low': {'type': SmallInteger, 'cn': '相同低价',
                                                 'func': tl.CDLMATCHINGLOW},
                                'mat_hold': {'type': SmallInteger, 'cn': '铺垫', 'func': tl.CDLMATHOLD},
                                'morning_doji_star': {'type': SmallInteger, 'cn': '十字晨星',
                                                      'func': tl.CDLMORNINGDOJISTAR},
                                'morning_star': {'type': SmallInteger, 'cn': '晨星', 'func': tl.CDLMORNINGSTAR},
                                'piercing_pattern': {'type': SmallInteger, 'cn': '刺透形态',
                                                     'func': tl.CDLPIERCING},
                                'rising_falling_three': {'type': SmallInteger, 'cn': '上升/下降三法',
                                                         'func': tl.CDLRISEFALL3METHODS},
                                'separating_lines': {'type': SmallInteger, 'cn': '分离线',
                                                     'func': tl.CDLSEPARATINGLINES},
                                'short_line_candle': {'type': SmallInteger, 'cn': '短蜡烛',
                                                      'func': tl.CDLSHORTLINE},
                                'spinning_top': {'type': SmallInteger, 'cn': '纺锤', 'func': tl.CDLSPINNINGTOP},
                                'stick_sandwich': {'type': SmallInteger, 'cn': '条形三明治',
                                                   'func': tl.CDLSTICKSANDWICH},
                                'takuri': {'type': SmallInteger, 'cn': '探水竿', 'func': tl.CDLTAKURI},
                                'tasuki_gap': {'type': SmallInteger, 'cn': '跳空并列阴阳线',
                                               'func': tl.CDLTASUKIGAP},
                                'tristar_pattern': {'type': SmallInteger, 'cn': '三星', 'func': tl.CDLTRISTAR},
                                'unique_3_river': {'type': SmallInteger, 'cn': '奇特三河床',
                                                   'func': tl.CDLUNIQUE3RIVER},
                                'upside_downside_gap': {'type': SmallInteger, 'cn': '上升/下降跳空三法',
                                                        'func': tl.CDLXSIDEGAP3METHODS}
                            }}

TABLE_CN_STOCK_KLINE_PATTERN = {'name': 'cn_stock_pattern', 'cn': '股票K线形态',
                                'columns': TABLE_CN_STOCK_FOREIGN_KEY['columns'].copy()}
TABLE_CN_STOCK_KLINE_PATTERN['columns'].update(STOCK_KLINE_PATTERN_DATA['columns'])


def get_field_cn(key, table):
    f = table.get('columns').get(key)
    if f is None:
        return key
    return f.get('cn')


def get_field_cns(cols_val):
    data = []
    for v in cols_val:
        data.append(v['cn'])
    return data


def get_field_types(cols):
    data = {}
    for k, v in cols.items():
        data[k] = v['type']
    return data
