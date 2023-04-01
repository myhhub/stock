#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import numpy as np
import talib as tl

__author__ = 'myh '
__date__ = '2023/3/10 '


def get_indicators(data, end_date=None, threshold=120, calc_threshold=None):
    isCopy = False
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask]
        isCopy = True
    if calc_threshold is not None:
        data = data.tail(n=calc_threshold)
        isCopy = True

    if isCopy:
        data = data.copy()
    data.loc[:, "volume"] = data["volume"] * 100  # 成交量单位从手变成股。

    # import stockstats
    # test = data.copy()
    # test = stockstats.StockDataFrame.retype(test) #验证计算结果

    # macd
    data.loc[:, 'macd'], data.loc[:, 'macds'], data.loc[:, 'macdh'] = tl.MACD(
        data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    # kdjk
    data.loc[:, 'kdjk'], data.loc[:, 'kdjd'] = tl.STOCH(
        data['high'], data['low'], data['close'], fastk_period=9,
        slowk_period=5, slowk_matype=1, slowd_period=5, slowd_matype=1)
    data.loc[:, 'kdjj'] = 3 * data['kdjk'] - 2 * data['kdjd']
    # boll
    data.loc[:, 'boll_ub'], data.loc[:, 'boll'], data.loc[:, 'boll_lb'] = tl.BBANDS \
        (data['close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    # trix
    data.loc[:, 'trix'] = tl.TRIX(data['close'], timeperiod=12)
    data.loc[:, 'trix_20_sma'] = tl.MA(data['trix'], timeperiod=20)
    # cr
    data.loc[:, 'm_price'] = data['amount'] / data['volume']
    data.loc[:, 'm_price_sf1'] = data['m_price'].shift(1)
    data.loc[:, 'h_m'] = data['high'] - data[['m_price_sf1', 'high']].min(axis=1)
    data.loc[:, 'm_l'] = data['m_price_sf1'] - data[['m_price_sf1', 'low']].min(axis=1)
    data.loc[:, 'h_m_sum'] = data['h_m'].rolling(window=26).sum()
    data.loc[:, 'm_l_sum'] = data['m_l'].rolling(window=26).sum()
    data.loc[:, 'cr'] = (data['h_m_sum'] / data['m_l_sum']) * 100
    data.loc[:, 'cr-ma1'] = tl.MA(data['cr'], timeperiod=5)
    data.loc[:, 'cr-ma2'] = tl.MA(data['cr'], timeperiod=10)
    data.loc[:, 'cr-ma3'] = tl.MA(data['cr'], timeperiod=20)
    # rsi
    data.loc[:, 'rsi'] = tl.RSI(data['close'], timeperiod=14)
    data.loc[:, 'rsi_6'] = tl.RSI(data['close'], timeperiod=6)
    data.loc[:, 'rsi_12'] = tl.RSI(data['close'], timeperiod=12)
    data.loc[:, 'rsi_24'] = tl.RSI(data['close'], timeperiod=24)
    # vr
    data.loc[:, 'av'] = data['volume'].where(data['p_change'] > 0, 0, inplace=False)
    data.loc[:, 'avs'] = data['av'].rolling(min_periods=1, window=26, center=False).sum()
    data.loc[:, 'bv'] = data['volume'].where(data['p_change'] < 0, 0, inplace=False)
    data.loc[:, 'bvs'] = data['bv'].rolling(min_periods=1, window=26, center=False).sum()
    data.loc[:, 'cv'] = data['volume'].where(data['p_change'] == 0, 0, inplace=False)
    data.loc[:, 'cvs'] = data['cv'].rolling(min_periods=1, window=26, center=False).sum()
    data.loc[:, 'vr'] = ((data['avs'] + data['cvs'] / 2) / (data['bvs'] + data['cvs'] / 2)).replace(np.nan, 0).replace(np.inf, 0) * 100
    data.loc[:, 'vr_6_sma'] = tl.MA(data['vr'], timeperiod=6)
    # DMI 计算方法和结果和stockstats不同，stockstats采用每天均价计算
    data.loc[:, 'pdi'] = tl.PLUS_DI(data['high'], data['low'], data['close'], timeperiod=14)
    data.loc[:, 'mdi'] = tl.MINUS_DI(data['high'], data['low'], data['close'], timeperiod=14)
    data.loc[:, 'dx'] = tl.DX(data['high'], data['low'], data['close'], timeperiod=14)
    data.loc[:, 'adx'] = tl.ADX(data['high'], data['low'], data['close'], timeperiod=6)
    data.loc[:, 'adxr'] = tl.ADXR(data['high'], data['low'], data['close'], timeperiod=6)
    # wr
    data.loc[:, 'h_6'] = data['high'].rolling(window=6).max()
    data.loc[:, 'l_6'] = data['low'].rolling(window=6).min()
    data.loc[:, 'wr_6'] = ((data['h_6'] - data['close']) / (data['h_6'] - data['l_6'])) * 100
    data.loc[:, 'h_10'] = data['high'].rolling(window=10).max()
    data.loc[:, 'l_10'] = data['low'].rolling(window=10).min()
    data.loc[:, 'wr_10'] = ((data['h_10'] - data['close']) / (data['h_10'] - data['l_10'])) * 100
    data.loc[:, 'h_14'] = data['high'].rolling(window=14).max()
    data.loc[:, 'l_14'] = data['low'].rolling(window=14).min()
    data.loc[:, 'wr_14'] = ((data['h_14'] - data['close']) / (data['h_14'] - data['l_14'])) * 100
    # cci
    data.loc[:, 'cci'] = tl.CCI(data['high'], data['low'], data['close'], timeperiod=14)
    data.loc[:, 'cci_84'] = tl.CCI(data['high'], data['low'], data['close'], timeperiod=84)
    # atr
    data.loc[:, 'prev_close'] = data['close'].shift(1)
    data.loc[:, 'h_l'] = data['high'] - data['low']
    data.loc[:, 'h_pc'] = (data['high'] - data['prev_close']).abs()
    data.loc[:, 'l_pc'] = (data['prev_close'] - data['low']).abs()
    data.loc[:, 'tr'] = data.loc[:, ['h_l', 'h_pc', 'l_pc']].T.max()
    data.loc[:, 'atr'] = data['tr'].ewm(ignore_na=False, alpha=1.0 / 14, min_periods=0, adjust=True).mean()
    # dma
    data.loc[:, 'ma10'] = tl.MA(data['close'], timeperiod=10)
    data.loc[:, 'ma50'] = tl.MA(data['close'], timeperiod=50)
    data.loc[:, 'dma'] = data['ma10'] - data['ma50']
    data.loc[:, 'dma_10_sma'] = tl.MA(data['dma'], timeperiod=10)
    # ----------stockstats没有以下指标-----------------
    # roc
    data.loc[:, 'roc'] = tl.ROC(data['close'], timeperiod=12)
    data.loc[:, 'rocma'] = tl.MA(data['roc'], timeperiod=6)
    data.loc[:, 'rocema'] = tl.EMA(data['rocma'], timeperiod=9)
    # obv
    data.loc[:, 'obv'] = tl.OBV(data['close'], data['volume'])
    # sar
    data.loc[:, 'sar'] = tl.SAR(data['high'], data['low'])
    # psy
    data.loc[:, 'price_up'] = 0
    data.loc[data['close'] > data['prev_close'], 'price_up'] = 1
    data.loc[:, 'price_up_sum'] = data['price_up'].rolling(window=12).sum()
    data.loc[:, 'psy'] = (data['price_up_sum'] / 12.0) * 100
    # BRAR
    data.loc[:, 'h_o'] = data['high'] - data['open']
    data.loc[:, 'o_l'] = data['open'] - data['low']
    data.loc[:, 'h_o_sum'] = data['h_o'].rolling(window=26).sum()
    data.loc[:, 'o_l_sum'] = data['o_l'].rolling(window=26).sum()
    data.loc[:, 'ar'] = (data['h_o_sum'] / data['o_l_sum']) * 100
    data.loc[:, 'h_c'] = data['high'] - data['close']
    data.loc[:, 'c_l'] = data['close'] - data['low']
    data.loc[:, 'h_c_sum'] = data['h_c'].rolling(window=26).sum()
    data.loc[:, 'c_l_sum'] = data['c_l'].rolling(window=26).sum()
    data.loc[:, 'br'] = (data['h_c_sum'] / data['c_l_sum']) * 100
    # EMV
    data.loc[:, 'm_hl'] = (data['high'].shift(1) + data['low'].shift(1)) / 2
    data.loc[:, 'emva_em'] = (data['m_price'] - data['m_hl']) * data['h_l'] / data['amount']
    data.loc[:, 'emv'] = data['emva_em'].rolling(window=14).sum()
    data.loc[:, 'emva'] = tl.MA(data['emv'], timeperiod=9)
    # BIAS
    data.loc[:, 'ma6'] = tl.MA(data['close'], timeperiod=6)
    data.loc[:, 'ma12'] = tl.MA(data['close'], timeperiod=12)
    data.loc[:, 'ma24'] = tl.MA(data['close'], timeperiod=24)
    data.loc[:, 'bias'] = ((data['close'] - data['ma6']) / data['ma6']) * 100
    data.loc[:, 'bias_12'] = ((data['close'] - data['ma12']) / data['ma12']) * 100
    data.loc[:, 'bias_24'] = ((data['close'] - data['ma24']) / data['ma24']) * 100
    # VOL
    data.loc[:, 'vol_5'] = tl.MA(data['volume'], timeperiod=5)
    data.loc[:, 'vol_10'] = tl.MA(data['volume'], timeperiod=10)
    # MA
    data.loc[:, 'ma20'] = tl.MA(data['close'], timeperiod=20)
    data.loc[:, 'ma200'] = tl.MA(data['close'], timeperiod=200)

    if threshold is not None:
        data = data.tail(n=threshold).copy()

    return data


def get_indicator(code_name, data, stock_column, date=None, calc_threshold=250):
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

        stockStat = get_indicators(data, end_date=end_date, threshold=1, calc_threshold=calc_threshold)
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
