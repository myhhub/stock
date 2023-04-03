#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import numpy as np
import talib as tl

__author__ = 'myh '
__date__ = '2023/3/10 '


def get_indicators(data, end_date=None, threshold=120, calc_threshold=None):
    try:
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
        data["volume"] = data["volume"] * 100  # 成交量单位从手变成股。

        # import stockstats
        # test = data.copy()
        # test = stockstats.StockDataFrame.retype(test)  # 验证计算结果

        # macd
        data.loc[:, 'macd'], data.loc[:, 'macds'], data.loc[:, 'macdh'] = tl.MACD(
            data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        data['macd'] = data['macd'].fillna(0.0)
        data['macds'] = data['macds'].fillna(0.0)
        data['macdh'] = data['macdh'].fillna(0.0)
        # kdjk
        data.loc[:, 'kdjk'], data.loc[:, 'kdjd'] = tl.STOCH(
            data['high'], data['low'], data['close'], fastk_period=9,
            slowk_period=5, slowk_matype=1, slowd_period=5, slowd_matype=1)
        data['kdjk'] = data['kdjk'].fillna(0.0)
        data['kdjd'] = data['kdjd'].fillna(0.0)
        data.loc[:, 'kdjj'] = 3 * data['kdjk'] - 2 * data['kdjd']
        # boll 计算结果和stockstats不同boll_ub,boll_lb
        data.loc[:, 'boll_ub'], data.loc[:, 'boll'], data.loc[:, 'boll_lb'] = tl.BBANDS \
            (data['close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        data['boll_ub'] = data['boll_ub'].fillna(0.0)
        data['boll'] = data['boll'].fillna(0.0)
        data['boll_lb'] = data['boll_lb'].fillna(0.0)
        # trix
        data.loc[:, 'trix'] = tl.TRIX(data['close'], timeperiod=12).fillna(0.0)
        data.loc[:, 'trix_20_sma'] = tl.MA(data['trix'], timeperiod=20).fillna(0.0)
        # cr
        data.loc[:, 'm_price'] = data['amount'] / data['volume']
        data.loc[:, 'm_price_sf1'] = data['m_price'].shift(1)
        data.loc[:, 'h_m'] = data['high'] - data[['m_price_sf1', 'high']].min(axis=1)
        data.loc[:, 'm_l'] = data['m_price_sf1'] - data[['m_price_sf1', 'low']].min(axis=1)
        data.loc[:, 'h_m_sum'] = data['h_m'].rolling(window=26).sum()
        data.loc[:, 'm_l_sum'] = data['m_l'].rolling(window=26).sum()
        data.loc[:, 'cr'] = (data['h_m_sum'] / data['m_l_sum']).fillna(0.0).replace(np.inf, 0.0) * 100
        data.loc[:, 'cr-ma1'] = tl.MA(data['cr'], timeperiod=5).fillna(0.0)
        data.loc[:, 'cr-ma2'] = tl.MA(data['cr'], timeperiod=10).fillna(0.0)
        data.loc[:, 'cr-ma3'] = tl.MA(data['cr'], timeperiod=20).fillna(0.0)
        # rsi
        data.loc[:, 'rsi'] = tl.RSI(data['close'], timeperiod=14).fillna(0.0)
        data.loc[:, 'rsi_6'] = tl.RSI(data['close'], timeperiod=6).fillna(0.0)
        data.loc[:, 'rsi_12'] = tl.RSI(data['close'], timeperiod=12).fillna(0.0)
        data.loc[:, 'rsi_24'] = tl.RSI(data['close'], timeperiod=24).fillna(0.0)
        # vr
        data.loc[:, 'av'] = data['volume'].where(data['p_change'] > 0, 0, inplace=False)
        data.loc[:, 'avs'] = data['av'].rolling(min_periods=1, window=26, center=False).sum()
        data.loc[:, 'bv'] = data['volume'].where(data['p_change'] < 0, 0, inplace=False)
        data.loc[:, 'bvs'] = data['bv'].rolling(min_periods=1, window=26, center=False).sum()
        data.loc[:, 'cv'] = data['volume'].where(data['p_change'] == 0, 0, inplace=False)
        data.loc[:, 'cvs'] = data['cv'].rolling(min_periods=1, window=26, center=False).sum()
        data.loc[:, 'vr'] = ((data['avs'] + data['cvs'] / 2) / (data['bvs'] + data['cvs'] / 2)).fillna(0.0).replace(
            np.inf, 0.0) * 100
        data.loc[:, 'vr_6_sma'] = tl.MA(data['vr'], timeperiod=6).fillna(0.0)
        # atr
        data.loc[:, 'prev_close'] = data['close'].shift(1)
        data.loc[:, 'h_l'] = data['high'] - data['low']
        data.loc[:, 'h_pc'] = (data['high'] - data['prev_close']).abs()
        data.loc[:, 'l_pc'] = (data['prev_close'] - data['low']).abs()
        data.loc[:, 'tr'] = data.loc[:, ['h_l', 'h_pc', 'l_pc']].T.max().fillna(0.0)
        data.loc[:, 'atr'] = tl.ATR(data['high'], data['low'], data['close'], timeperiod=14).fillna(0.0)
        # DMI
        # talib计算公式和stockstats不同
        # data.loc[:, 'pdi'] = tl.PLUS_DI(data['high'], data['low'], data['close'], timeperiod=14).fillna(0.0)
        # data.loc[:, 'mdi'] = tl.MINUS_DI(data['high'], data['low'], data['close'], timeperiod=14).fillna(0.0)
        # data.loc[:, 'dx'] = tl.DX(data['high'], data['low'], data['close'], timeperiod=14).fillna(0.0)
        # data.loc[:, 'adx'] = tl.ADX(data['high'], data['low'], data['close'], timeperiod=6).fillna(0.0)
        # data.loc[:, 'adxr'] = tl.ADXR(data['high'], data['low'], data['close'], timeperiod=6).fillna(0.0)
        # stockstats计算公式
        data.loc[:, 'high_delta'] = data['high'].diff(1)
        data.loc[:, 'high_m'] = (data['high_delta'] + data['high_delta'].abs()) / 2
        data.loc[:, 'low_delta'] = -data['low'].diff(1)
        data.loc[:, 'low_m'] = (data['low_delta'] + data['low_delta'].abs()) / 2
        data.loc[:, 'pdm'] = tl.EMA(data['high_m'].where(data['high_m'] > data['low_m'], 0), timeperiod=14).fillna(0.0)
        data.loc[:, 'pdi'] = (data['pdm'] / data['atr']).fillna(0.0).replace(np.inf, 0.0) * 100
        data.loc[:, 'mdm'] = tl.EMA(data['low_m'].where(data['low_m'] > data['high_m'], 0), timeperiod=14).fillna(0.0)
        data.loc[:, 'mdi'] = (data['mdm'] / data['atr']).fillna(0.0).replace(np.inf, 0.0) * 100
        data.loc[:, 'dx'] = ((data['pdi'] - data['mdi']).abs() / (data['pdi'] + data['mdi'])).fillna(0.0) * 100
        data.loc[:, 'adx'] = tl.EMA(data['dx'], timeperiod=6).fillna(0.0)
        data.loc[:, 'adxr'] = tl.EMA(data['adx'], timeperiod=6).fillna(0.0)
        # wr
        data.loc[:, 'h_6'] = data['high'].rolling(window=6).max()
        data.loc[:, 'l_6'] = data['low'].rolling(window=6).min()
        data.loc[:, 'wr_6'] = ((data['h_6'] - data['close']) / (data['h_6'] - data['l_6'])).fillna(0.0).replace(np.inf,
                                                                                                                0.0) * 100
        data.loc[:, 'h_10'] = data['high'].rolling(window=10).max()
        data.loc[:, 'l_10'] = data['low'].rolling(window=10).min()
        data.loc[:, 'wr_10'] = ((data['h_10'] - data['close']) / (data['h_10'] - data['l_10'])).fillna(0.0).replace(
            np.inf, 0.0) * 100
        data.loc[:, 'h_14'] = data['high'].rolling(window=14).max()
        data.loc[:, 'l_14'] = data['low'].rolling(window=14).min()
        data.loc[:, 'wr_14'] = ((data['h_14'] - data['close']) / (data['h_14'] - data['l_14'])).fillna(0.0).replace(
            np.inf, 0.0) * 100
        # cci 计算方法和结果和stockstats不同，stockstats典型价采用均价(总额/成交量)计算
        data.loc[:, 'cci'] = tl.CCI(data['high'], data['low'], data['close'], timeperiod=14).fillna(0.0)
        data.loc[:, 'cci_84'] = tl.CCI(data['high'], data['low'], data['close'], timeperiod=84).fillna(0.0)
        # dma
        data.loc[:, 'ma10'] = tl.MA(data['close'], timeperiod=10).fillna(0.0)
        data.loc[:, 'ma50'] = tl.MA(data['close'], timeperiod=50).fillna(0.0)
        data.loc[:, 'dma'] = data['ma10'] - data['ma50']
        data.loc[:, 'dma_10_sma'] = tl.MA(data['dma'], timeperiod=10).fillna(0.0)
        # tema
        data.loc[:, 'single5'] = tl.EMA(data['close'], timeperiod=5).fillna(0.0)
        data.loc[:, 'double5'] = tl.EMA(data['single5'], timeperiod=5).fillna(0.0)
        data.loc[:, 'triple5'] = tl.EMA(data['double5'], timeperiod=5).fillna(0.0)
        data.loc[:, 'tema'] = (3 * data['single5'] - 3 * data['double5'] + data['triple5']).fillna(0.0)
        # mfi 计算方法和结果和stockstats不同，stockstats典型价采用均价(总额/成交量)计算
        data.loc[:, 'mfi'] = tl.MFI(data['high'], data['low'], data['close'], data['volume'], timeperiod=14).fillna(0.0)
        # vwma
        data.loc[:, 'tpv_14'] = data['amount'].rolling(window=14).sum()
        data.loc[:, 'vol_14'] = data['volume'].rolling(window=14).sum()
        data.loc[:, 'vwma'] = (data['tpv_14'] / data['vol_14']).fillna(0.0).replace(np.inf, 0.0)
        # ppo
        data.loc[:, 'ppo_short'] = tl.EMA(data['close'], timeperiod=12).fillna(0.0)
        data.loc[:, 'ppo_long'] = tl.EMA(data['close'], timeperiod=26).fillna(0.0)
        data.loc[:, 'ppo'] = ((data['ppo_short'] - data['ppo_long']) / data['ppo_long']).fillna(0.0).replace(np.inf,
                                                                                                             0.0) * 100
        data.loc[:, 'ppos'] = tl.EMA(data['ppo'], timeperiod=9).fillna(0.0)
        data.loc[:, 'ppoh'] = data['ppo'] - data['ppos']
        # stochrsi
        data.loc[:, 'rsi_min'] = data['rsi'].rolling(window=14).min()
        data.loc[:, 'rsi_max'] = data['rsi'].rolling(window=14).max()
        data.loc[:, 'stochrsi'] = ((data['rsi'] - data['rsi_min']) / (data['rsi_max'] - data['rsi_min'])).fillna(
            0.0) * 100
        # wt
        data.loc[:, 'esa'] = tl.EMA(data['m_price'], timeperiod=10).fillna(0.0)
        data.loc[:, 'esa_d'] = tl.EMA((data['m_price'] - data['esa']).abs(), timeperiod=10).fillna(0.0)
        data.loc[:, 'esa_ci'] = ((data['m_price'] - data['esa']) / (0.015 * data['esa_d'])).fillna(0.0).replace(np.inf,
                                                                                                                0.0)
        data.loc[:, 'wt1'] = tl.EMA(data['esa_ci'], timeperiod=21).fillna(0.0)
        data.loc[:, 'wt2'] = tl.MA(data['wt1'], timeperiod=4).fillna(0.0)
        # Supertrend
        data.loc[:, 'm_atr'] = data['atr'] * 3
        data.loc[:, 'hl_avg'] = (data['high'] + data['low']) / 2.0
        data.loc[:, 'b_ub'] = data['hl_avg'] + data['m_atr']
        data.loc[:, 'b_lb'] = data['hl_avg'] - data['m_atr']
        size = len(data.index)
        ub = np.empty(size, dtype=np.float64)
        lb = np.empty(size, dtype=np.float64)
        st = np.empty(size, dtype=np.float64)
        for i in range(size):
            if i == 0:
                ub[i] = data['b_ub'].iloc[i]
                lb[i] = data['b_lb'].iloc[i]
                if data['close'].iloc[i] <= ub[i]:
                    st[i] = ub[i]
                else:
                    st[i] = lb[i]
                continue

            last_close = data['close'].iloc[i - 1]
            curr_close = data['close'].iloc[i]
            last_ub = ub[i - 1]
            last_lb = lb[i - 1]
            last_st = st[i - 1]
            curr_b_ub = data['b_ub'].iloc[i]
            curr_b_lb = data['b_lb'].iloc[i]

            # calculate current upper band
            if curr_b_ub < last_ub or last_close > last_ub:
                ub[i] = curr_b_ub
            else:
                ub[i] = last_ub

            # calculate current lower band
            if curr_b_lb > last_lb or last_close < last_lb:
                lb[i] = curr_b_lb
            else:
                lb[i] = last_lb

            # calculate supertrend
            if last_st == last_ub:
                if curr_close <= ub[i]:
                    st[i] = ub[i]
                else:
                    st[i] = lb[i]
            elif last_st == last_lb:
                if curr_close > lb[i]:
                    st[i] = lb[i]
                else:
                    st[i] = ub[i]

        data.loc[:, 'supertrend_ub'] = ub
        data.loc[:, 'supertrend_lb'] = lb
        data.loc[:, 'supertrend'] = st
        data = data.copy()
        # ----------stockstats没有以下指标-----------------
        # roc
        data.loc[:, 'roc'] = tl.ROC(data['close'], timeperiod=12).fillna(0.0)
        data.loc[:, 'rocma'] = tl.MA(data['roc'], timeperiod=6).fillna(0.0)
        data.loc[:, 'rocema'] = tl.EMA(data['rocma'], timeperiod=9).fillna(0.0)
        # obv
        data.loc[:, 'obv'] = tl.OBV(data['close'], data['volume']).fillna(0.0)
        # sar
        data.loc[:, 'sar'] = tl.SAR(data['high'], data['low']).fillna(0.0)
        # psy
        data.loc[:, 'price_up'] = 0
        data.loc[data['close'] > data['prev_close'], 'price_up'] = 1
        data.loc[:, 'price_up_sum'] = data['price_up'].rolling(window=12).sum()
        data.loc[:, 'psy'] = (data['price_up_sum'] / 12.0).fillna(0.0) * 100
        # BRAR
        data.loc[:, 'h_o'] = data['high'] - data['open']
        data.loc[:, 'o_l'] = data['open'] - data['low']
        data.loc[:, 'h_o_sum'] = data['h_o'].rolling(window=26).sum()
        data.loc[:, 'o_l_sum'] = data['o_l'].rolling(window=26).sum()
        data.loc[:, 'ar'] = (data['h_o_sum'] / data['o_l_sum']).fillna(0.0).replace(np.inf, 0.0) * 100
        data.loc[:, 'h_c'] = data['high'] - data['close']
        data.loc[:, 'c_l'] = data['close'] - data['low']
        data.loc[:, 'h_c_sum'] = data['h_c'].rolling(window=26).sum()
        data.loc[:, 'c_l_sum'] = data['c_l'].rolling(window=26).sum()
        data.loc[:, 'br'] = (data['h_c_sum'] / data['c_l_sum']).fillna(0.0).replace(np.inf, 0.0) * 100
        # EMV
        data.loc[:, 'prev_high'] = data['high'].shift(1)
        data.loc[:, 'prev_low'] = data['low'].shift(1)
        data.loc[:, 'm_hl'] = (data['prev_high'] + data['prev_low']) / 2
        data.loc[:, 'emva_em'] = (data['m_price'] - data['m_hl']) * data['h_l'] / data['amount']
        data.loc[:, 'emv'] = data['emva_em'].rolling(window=14).sum().fillna(0.0)
        data.loc[:, 'emva'] = tl.MA(data['emv'], timeperiod=9).fillna(0.0)
        # BIAS
        data.loc[:, 'ma6'] = tl.MA(data['close'], timeperiod=6).fillna(0.0)
        data.loc[:, 'ma12'] = tl.MA(data['close'], timeperiod=12).fillna(0.0)
        data.loc[:, 'ma24'] = tl.MA(data['close'], timeperiod=24).fillna(0.0)
        data.loc[:, 'bias'] = ((data['close'] - data['ma6']) / data['ma6']) * 100
        data.loc[:, 'bias_12'] = ((data['close'] - data['ma12']) / data['ma12']).fillna(0.0).replace(np.inf, 0.0) * 100
        data.loc[:, 'bias_24'] = ((data['close'] - data['ma24']) / data['ma24']).fillna(0.0).replace(np.inf, 0.0) * 100
        # DPO
        data.loc[:, 'dpo'] = data['close'].fillna(0.0) - (tl.MA(data['close'], timeperiod=11)).shift(1).fillna(0.0)
        data.loc[:, 'madpo'] = tl.MA(data['dpo'], timeperiod=6).fillna(0.0)
        # VHF
        data.loc[:, 'hcp_lcp'] = tl.MAX(data['close'], timeperiod=28) - tl.MIN(data['close'], timeperiod=28)
        data.loc[:, 'vhf'] = data['hcp_lcp'].div(
            tl.MA((data['close'] - data['prev_close']).abs(), timeperiod=28) * 28).fillna(0.0)
        # RVI
        data.loc[:, 'rvi_x'] = data['close'] - data['open'] + 2 * (data['prev_close'] - data['open'].shift(1)) + 2 * (
                data['close'].shift(2) - data['open'].shift(2)) * (data['close'].shift(3) - data['open'].shift(3)) / 6
        data.loc[:, 'rvi_y'] = data['high'] - data['low'] + 2 * (data['prev_high'] - data['prev_low']) + 2 * (
                data['high'].shift(2) - data['low'].shift(2)) * (data['high'].shift(3) - data['low'].shift(3)) / 6
        data.loc[:, 'rvi'] = (tl.MA(data['rvi_x'], timeperiod=10) * 10 / tl.MA(data['rvi_y'], timeperiod=10)).fillna(
            0.0) * 10
        # FI
        data.loc[:, 'fi'] = data['close'].diff(1) * data['volume'].fillna(0.0)
        data.loc[:, 'force_2'] = tl.EMA(data['fi'], timeperiod=2).fillna(0.0)
        data.loc[:, 'force_13'] = tl.EMA(data['fi'], timeperiod=13).fillna(0.0)
        # VOL
        data.loc[:, 'vol_5'] = tl.MA(data['volume'], timeperiod=5).fillna(0.0)
        data.loc[:, 'vol_10'] = tl.MA(data['volume'], timeperiod=10).fillna(0.0)
        # MA
        data.loc[:, 'ma20'] = tl.MA(data['close'], timeperiod=20).fillna(0.0)
        data.loc[:, 'ma200'] = tl.MA(data['close'], timeperiod=200).fillna(0.0)

        if threshold is not None:
            data = data.tail(n=threshold).copy()
        return data
    except Exception as e:
        logging.debug("{}处理异常：{}代码{}".format('calculate_indicator.get_indicators', data['code'], e))
    return None


def get_indicator(code_name, data, stock_column, date=None, calc_threshold=90):
    try:
        if date is None:
            end_date = code_name[0]
        else:
            end_date = date.strftime("%Y-%m-%d")

        code = code_name[1]
        # 设置返回数组。
        stock_data_list = [end_date, code]
        columns_num = len(stock_column) - 2
        # 增加空判断，如果是空返回 0 数据。
        if len(data.index) == 0:
            for i in range(columns_num):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        idr_data = get_indicators(data, end_date=end_date, threshold=1, calc_threshold=calc_threshold)

        # 增加空判断，如果是空返回 0 数据。
        if idr_data is None:
            for i in range(columns_num):
                stock_data_list.append(0)
            return pd.Series(stock_data_list, index=stock_column)

        # 初始化统计类
        for i in range(columns_num):
            # 将数据的最后一个返回。
            tmp_val = idr_data[stock_column[i + 2]].tail(1).values[0]
            # 解决值中存在INF NaN问题。
            if np.isinf(tmp_val) or np.isnan(tmp_val):
                stock_data_list.append(0)
            else:
                stock_data_list.append(tmp_val)

        return pd.Series(stock_data_list, index=stock_column)
    except Exception as e:
        logging.debug("{}处理异常：{}代码{}".format('calculate_indicator.get_indicator', code, e))
    return None
