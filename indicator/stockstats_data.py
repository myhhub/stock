#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import stockstats
import numpy as np
import talib as tl

__author__ = 'myh '
__date__ = '2023/3/10 '


def get_indicators(data, end_date=None, threshold=60):
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
    data = data.tail(n=threshold).copy()
    data.loc[:, "volume"] = data["volume"] * 100  # 成交量单位从手变成股。
    data_stat = stockstats.StockDataFrame.retype(data)

    if data_stat is None or len(data_stat.index) == 0:
        return None

    data_stat.loc[:, 'obv'] = tl.OBV(data_stat['close'], data_stat['volume'])
    data_stat.loc[:, 'sar'] = tl.SAR(data_stat['high'], data_stat['low'])

    data_stat.loc[:, 'ext_0'] = data_stat['close'] - data_stat['close'].shift(1)
    data_stat.loc[:, 'ext_1'] = 0
    data_stat.loc[data_stat['ext_0'] > 0, 'ext_1'] = 1
    data_stat.loc[:, 'ext_2'] = data_stat['ext_1'].rolling(window=12).sum()
    data_stat.loc[:, 'psy'] = (data_stat['ext_2'] / 12.0) * 100

    data_stat.loc[:, 'h_o'] = data_stat['high'] - data_stat['open']
    data_stat.loc[:, 'o_l'] = data_stat['open'] - data_stat['low']
    data_stat.loc[:, 'h_o_sum'] = data_stat['h_o'].rolling(window=26).sum()
    data_stat.loc[:, 'o_l_sum'] = data_stat['o_l'].rolling(window=26).sum()
    data_stat.loc[:, 'ar'] = (data_stat['h_o_sum'] / data_stat['o_l_sum']) * 100
    data_stat.loc[:, 'h_c'] = data_stat['high'] - data_stat['close']
    data_stat.loc[:, 'c_l'] = data_stat['close'] - data_stat['low']
    data_stat.loc[:, 'h_c_sum'] = data_stat['h_c'].rolling(window=26).sum()
    data_stat.loc[:, 'c_l_sum'] = data_stat['c_l'].rolling(window=26).sum()
    data_stat.loc[:, 'br'] = (data_stat['h_c_sum'] / data_stat['c_l_sum']) * 100

    data_stat.loc[:, 'emva_a'] = (data_stat['high'] + data_stat['low']) / 2
    data_stat.loc[:, 'emva_b'] = (data_stat['high'].shift(1) + data_stat['low'].shift(1)) / 2
    data_stat.loc[:, 'emva_c'] = data_stat['high'] - data_stat['low']
    data_stat.loc[:, 'emva_em'] = (data_stat['emva_a'] - data_stat['emva_b']) * data_stat['emva_c'] / data_stat[
        'amount']
    data_stat.loc[:, 'emv'] = data_stat['emva_em'].rolling(window=14).sum()
    data_stat.loc[:, 'emva'] = tl.MA(data_stat['emv'], timeperiod=9)

    data_stat.loc[:, 'ma6'] = tl.MA(data_stat['close'], timeperiod=6)
    data_stat.loc[:, 'ma12'] = tl.MA(data_stat['close'], timeperiod=12)
    data_stat.loc[:, 'ma24'] = tl.MA(data_stat['close'], timeperiod=24)
    data_stat.loc[:, 'bias'] = ((data_stat['close'] - data_stat['ma6']) / data_stat['ma6']) * 100
    data_stat.loc[:, 'bias_12'] = ((data_stat['close'] - data_stat['ma12']) / data_stat['ma12']) * 100
    data_stat.loc[:, 'bias_24'] = ((data_stat['close'] - data_stat['ma24']) / data_stat['ma24']) * 100

    data_stat.loc[:, 'roc'] = tl.ROC(data_stat['close'], timeperiod=12)
    data_stat.loc[:, 'rocma'] = tl.MA(data_stat['roc'], timeperiod=6)
    data_stat.loc[:, 'rocema'] = tl.EMA(data_stat['rocma'], timeperiod=9)

    return data_stat


def get_indicators_talib(data, end_date=None, threshold=60):
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
    df = data.tail(n=threshold).copy()
    df.loc[:, "volume"] = data["volume"] * 100  # 成交量单位从手变成股。

    df.loc[:, 'rsi'] = tl.RSI(df['close'], timeperiod=14)
    df.loc[:, 'rsi_6'] = tl.RSI(df['close'], timeperiod=6)
    df.loc[:, 'rsi_12'] = tl.RSI(df['close'], timeperiod=12)
    df.loc[:, 'rsi_24'] = tl.RSI(df['close'], timeperiod=24)

    df.loc[:, 'macd'], df.loc[:, 'macds'], df.loc[:, 'macdh'] = tl.MACD(df['close'], fastperiod=12, slowperiod=26,
                                                                      signalperiod=9)

    df.loc[:, 'kdjk'], df.loc[:, 'kdjd'] = tl.STOCH(df['high'], df['low'], df['close'], fastk_period=5,
                                                    slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    df.loc[:, 'kdjj'] = 3 * df['kdjk'] - 2 * df['kdjd']

    df.loc[:, 'm_price'] = (df['high'] + df['low']) / 2
    df.loc[:, 'h_m'] = df['high'] - df['m_price'].shift(1)
    df.loc[:, 'm_l'] = df['m_price'].shift(1) - df['low']
    df.loc[:, 'h_m_sum'] = df['h_m'].rolling(window=26).sum()
    df.loc[:, 'm_l_sum'] = df['m_l'].rolling(window=26).sum()
    df.loc[:, 'cr'] = (df['h_m_sum'] / df['m_l_sum']) * 100
    df.loc[:, 'cr-ma1'] = tl.MA(df['cr'], timeperiod=5)
    df.loc[:, 'cr-ma2'] = tl.MA(df['cr'], timeperiod=10)
    df.loc[:, 'cr-ma3'] = tl.MA(df['cr'], timeperiod=20)
    df.loc[:, 'boll_ub'], df.loc[:, 'boll'], df.loc[:, 'boll_lb'] = tl.BBANDS(df['close'], timeperiod=20, nbdevup=2,
                                                                              nbdevdn=2, matype=0)
    df.loc[:, 'trix'] = tl.TRIX(df['close'], timeperiod=12)
    df.loc[:, 'trix_20_sma'] = tl.MA(df['trix'], timeperiod=20)

    df.loc[:, 'ma10'] = tl.MA(df['close'], timeperiod=10)
    df.loc[:, 'ma50'] = tl.MA(df['close'], timeperiod=50)
    df.loc[:, 'dma'] = df['ma10'] - df['ma50']
    df.loc[:, 'dma_10_sma'] = tl.MA(df['dma'], timeperiod=10)

    df.loc[:, 'cci'] = tl.CCI(df['high'], df['low'], df['close'], timeperiod=14)
    df.loc[:, 'cci_84'] = tl.CCI(df['high'], df['low'], df['close'], timeperiod=84)

    df.loc[:, 'h_6'] = df['high'].rolling(window=6).max()
    df.loc[:, 'l_6'] = df['low'].rolling(window=6).min()
    df.loc[:, 'wr_6'] = ((df['h_6'] - df['close']) / (df['h_6'] - df['l_6'])) * 100
    df.loc[:, 'h_10'] = df['high'].rolling(window=10).max()
    df.loc[:, 'l_10'] = df['low'].rolling(window=10).min()
    df.loc[:, 'wr_10'] = ((df['h_10'] - df['close']) / (df['h_10'] - df['l_10'])) * 100
    df.loc[:, 'h_14'] = df['high'].rolling(window=14).max()
    df.loc[:, 'l_14'] = df['low'].rolling(window=14).min()
    df.loc[:, 'wr_14'] = ((df['h_14'] - df['close']) / (df['h_14'] - df['l_14'])) * 100

    df.loc[:, 'pdi'] = tl.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
    df.loc[:, 'mdi'] = tl.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
    df.loc[:, 'dx'] = tl.DX(df['high'], df['low'], df['close'], timeperiod=14)
    df.loc[:, 'adx'] = tl.ADX(df['high'], df['low'], df['close'], timeperiod=6)
    df.loc[:, 'adxr'] = tl.ADXR(df['high'], df['low'], df['close'], timeperiod=6)

    df.loc[:, 'vol_5'] = tl.MA(df['volume'], timeperiod=5)
    df.loc[:, 'vol_10'] = tl.MA(df['volume'], timeperiod=10)

    df.loc[:, 'obv'] = tl.OBV(df['close'], df['volume'])
    df.loc[:, 'sar'] = tl.SAR(df['high'], df['low'])

    df.loc[:, 'prev_close'] = df['close'].shift(1)
    df.loc[:, 'ext_0'] = df['close'] - df['prev_close']
    df.loc[:, 'ext_1'] = 0
    df.loc[df['ext_0'] > 0, 'ext_1'] = 1
    df.loc[:, 'ext_2'] = df['ext_1'].rolling(window=12).sum()
    df.loc[:, 'psy'] = (df['ext_2'] / 12.0) * 100

    df.loc[:, 'h_o'] = df['high'] - df['open']
    df.loc[:, 'o_l'] = df['open'] - df['low']
    df.loc[:, 'h_o_sum'] = df['h_o'].rolling(window=26).sum()
    df.loc[:, 'o_l_sum'] = df['o_l'].rolling(window=26).sum()
    df.loc[:, 'ar'] = (df['h_o_sum'] / df['o_l_sum']) * 100
    df.loc[:, 'h_c'] = df['high'] - df['close']
    df.loc[:, 'c_l'] = df['close'] - df['low']
    df.loc[:, 'h_c_sum'] = df['h_c'].rolling(window=26).sum()
    df.loc[:, 'c_l_sum'] = df['c_l'].rolling(window=26).sum()
    df.loc[:, 'br'] = (df['h_c_sum'] / df['c_l_sum']) * 100

    df.loc[:, 'm_hl'] = (df['high'].shift(1) + df['low'].shift(1)) / 2
    df.loc[:, 'h_l'] = df['high'] - df['low']
    df.loc[:, 'emva_em'] = (df['m_price'] - df['m_hl']) * df['h_l'] / df['amount']
    df.loc[:, 'emv'] = df['emva_em'].rolling(window=14).sum()
    df.loc[:, 'emva'] = tl.MA(df['emv'], timeperiod=9)

    df.loc[:, 'ma6'] = tl.MA(df['close'], timeperiod=6)
    df.loc[:, 'ma12'] = tl.MA(df['close'], timeperiod=12)
    df.loc[:, 'ma24'] = tl.MA(df['close'], timeperiod=24)
    df.loc[:, 'bias'] = ((df['close'] - df['ma6']) / df['ma6']) * 100
    df.loc[:, 'bias_12'] = ((df['close'] - df['ma12']) / df['ma12']) * 100
    df.loc[:, 'bias_24'] = ((df['close'] - df['ma24']) / df['ma24']) * 100

    df.loc[:, 'roc'] = tl.ROC(df['close'], timeperiod=12)
    df.loc[:, 'rocma'] = tl.MA(df['roc'], timeperiod=6)
    df.loc[:, 'rocema'] = tl.EMA(df['rocma'], timeperiod=9)

    df.loc[:, 'av'] = df['volume'].where(df['p_change'] > 0, 0, inplace=False)
    df.loc[:, 'avs'] = df['av'].rolling(min_periods=1, window=26, center=False).sum()
    df.loc[:, 'bv'] = df['volume'].where(df['p_change'] < 0, 0, inplace=False)
    df.loc[:, 'bvs'] = df['bv'].rolling(min_periods=1, window=26, center=False).sum()
    df.loc[:, 'cv'] = df['volume'].where(df['p_change'] == 0, 0, inplace=False)
    df.loc[:, 'cvs'] = df['cv'].rolling(min_periods=1, window=26, center=False).sum()
    df.loc[:, 'vr'] = (df['avs'] + df['cvs'] / 2) / (df['bvs'] + df['cvs'] / 2) * 100
    df.loc[:, 'vr_6_sma'] = tl.MA(df['vr'], timeperiod=6)

    df.loc[:, 'h_pc'] = (df['high'] - df['prev_close']).abs()
    df.loc[:, 'l_pc'] = (df['prev_close'] - df['low']).abs()
    df.loc[:, 'tr'] = df.loc[:, ['h_l', 'h_pc', 'l_pc']].T.max()
    df.loc[:, 'atr'] = df['tr'].ewm(ignore_na=False, alpha=1.0 / 14, min_periods=0, adjust=True).mean()

    return df


def get_indicator_tail(code_name, data, stock_column, date=None, threshold=60):
    try:
        if date is None:
            end_date = code_name[0]
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]
        # 设置返回数组。
        stock_data_list = [end_date, code]
        # 增加空判断，如果是空返回 0 数据。
        if len(data.index) == 0:
            for i in range(len(stock_column) - 2):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        stockStat = get_indicators(data, end_date=end_date, threshold=threshold)
        # 增加空判断，如果是空返回 0 数据。
        if stockStat is None:
            for i in range(len(stock_column) - 2):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        # 初始化统计类
        for i in range(len(stock_column) - 2):
            # 将数据的最后一个返回。
            tmp_val = stockStat[stock_column[i + 2]].tail(1).values[0]
            # 解决值中存在INF NaN问题。
            if np.isinf(tmp_val) or np.isnan(tmp_val):
                stock_data_list.append(0)
            else:
                stock_data_list.append(tmp_val)
    except Exception as e:
        logging.debug("{}处理异常：{}代码{}".format('stockstats_data.get_indicator_tail指标计算', code, e))

    return pd.Series(stock_data_list, index=stock_column)
