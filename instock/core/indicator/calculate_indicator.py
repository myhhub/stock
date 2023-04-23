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

        # import stockstats
        # test = data.copy()
        # test = stockstats.StockDataFrame.retype(test)  # 验证计算结果

        with np.errstate(divide='ignore', invalid='ignore'):

            # macd
            data.loc[:, 'macd'], data.loc[:, 'macds'], data.loc[:, 'macdh'] = tl.MACD(
                data['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            data['macd'].values[np.isnan(data['macd'].values)] = 0.0
            data['macds'].values[np.isnan(data['macds'].values)] = 0.0
            data['macdh'].values[np.isnan(data['macdh'].values)] = 0.0

            # kdjk
            data.loc[:, 'kdjk'], data.loc[:, 'kdjd'] = tl.STOCH(
                data['high'].values, data['low'].values, data['close'].values, fastk_period=9,
                slowk_period=5, slowk_matype=1, slowd_period=5, slowd_matype=1)
            data['kdjk'].values[np.isnan(data['kdjk'].values)] = 0.0
            data['kdjd'].values[np.isnan(data['kdjd'].values)] = 0.0
            data.loc[:, 'kdjj'] = 3 * data['kdjk'].values - 2 * data['kdjd'].values

            # boll 计算结果和stockstats不同boll_ub,boll_lb
            data.loc[:, 'boll_ub'], data.loc[:, 'boll'], data.loc[:, 'boll_lb'] = tl.BBANDS \
                (data['close'].values, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
            data['boll_ub'].values[np.isnan(data['boll_ub'].values)] = 0.0
            data['boll'].values[np.isnan(data['boll'].values)] = 0.0
            data['boll_lb'].values[np.isnan(data['boll_lb'].values)] = 0.0

            # trix
            data.loc[:, 'trix'] = tl.TRIX(data['close'].values, timeperiod=12)
            data['trix'].values[np.isnan(data['trix'].values)] = 0.0
            data.loc[:, 'trix_20_sma'] = tl.MA(data['trix'].values, timeperiod=20)
            data['trix_20_sma'].values[np.isnan(data['trix_20_sma'].values)] = 0.0

            # cr
            data.loc[:, 'm_price'] = data['amount'].values / data['volume'].values
            data.loc[:, 'm_price_sf1'] = data['m_price'].shift(1, fill_value=0.0).values
            data.loc[:, 'h_m'] = data['high'].values - data[['m_price_sf1', 'high']].values.min(axis=1)
            data.loc[:, 'm_l'] = data['m_price_sf1'].values - data[['m_price_sf1', 'low']].values.min(axis=1)
            data.loc[:, 'h_m_sum'] = tl.SUM(data['h_m'].values, timeperiod=26)
            data.loc[:, 'm_l_sum'] = tl.SUM(data['m_l'].values, timeperiod=26)
            data.loc[:, 'cr'] = data['h_m_sum'].values / data['m_l_sum'].values
            data['cr'].values[np.isnan(data['cr'].values)] = 0.0
            data['cr'].values[np.isinf(data['cr'].values)] = 0.0
            data['cr'] = data['cr'].values * 100
            data.loc[:, 'cr-ma1'] = tl.MA(data['cr'].values, timeperiod=5)
            data['cr-ma1'].values[np.isnan(data['cr-ma1'].values)] = 0.0
            data.loc[:, 'cr-ma2'] = tl.MA(data['cr'].values, timeperiod=10)
            data['cr-ma2'].values[np.isnan(data['cr-ma2'].values)] = 0.0
            data.loc[:, 'cr-ma3'] = tl.MA(data['cr'].values, timeperiod=20)
            data['cr-ma3'].values[np.isnan(data['cr-ma3'].values)] = 0.0

            # rsi
            data.loc[:, 'rsi'] = tl.RSI(data['close'].values, timeperiod=14)
            data['rsi'].values[np.isnan(data['rsi'].values)] = 0.0
            data.loc[:, 'rsi_6'] = tl.RSI(data['close'].values, timeperiod=6)
            data['rsi_6'].values[np.isnan(data['rsi_6'].values)] = 0.0
            data.loc[:, 'rsi_12'] = tl.RSI(data['close'].values, timeperiod=12)
            data['rsi_12'].values[np.isnan(data['rsi_12'].values)] = 0.0
            data.loc[:, 'rsi_24'] = tl.RSI(data['close'].values, timeperiod=24)
            data['rsi_24'].values[np.isnan(data['rsi_24'].values)] = 0.0

            # vr
            data.loc[:, 'av'] = np.where(data['p_change'].values > 0, data['volume'].values, 0)
            data.loc[:, 'avs'] = tl.SUM(data['av'].values, timeperiod=26)
            data.loc[:, 'bv'] = np.where(data['p_change'].values < 0, data['volume'].values, 0)
            data.loc[:, 'bvs'] = tl.SUM(data['bv'].values, timeperiod=26)
            data.loc[:, 'cv'] = np.where(data['p_change'].values == 0, data['volume'].values, 0)
            data.loc[:, 'cvs'] = tl.SUM(data['cv'].values, timeperiod=26)
            data.loc[:, 'vr'] = (data['avs'].values + data['cvs'].values / 2) / (data['bvs'].values + data['cvs'].values / 2)
            data['vr'].values[np.isnan(data['vr'].values)] = 0.0
            data['vr'].values[np.isinf(data['vr'].values)] = 0.0
            data['vr'] = data['vr'].values * 100
            data.loc[:, 'vr_6_sma'] = tl.MA(data['vr'].values, timeperiod=6)
            data['vr_6_sma'].values[np.isnan(data['vr_6_sma'].values)] = 0.0

            # atr
            data.loc[:, 'prev_close'] = data['close'].shift(1, fill_value=0.0).values
            data.loc[:, 'h_l'] = data['high'].values - data['low'].values
            data.loc[:, 'h_cy'] = data['high'].values - data['prev_close'].values
            data.loc[:, 'cy_l'] = data['prev_close'].values - data['low'].values
            data.loc[:, 'h_cy_a'] = abs(data['h_cy'].values)
            data.loc[:, 'cy_l_a'] = abs(data['cy_l'].values)
            data.loc[:, 'tr'] = data.loc[:, ['h_l', 'h_cy_a', 'cy_l_a']].T.max().values
            data['tr'].values[np.isnan(data['tr'].values)] = 0.0
            data.loc[:, 'atr'] = tl.ATR(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            data['atr'].values[np.isnan(data['atr'].values)] = 0.0

            # DMI
            # talib计算公式和stockstats不同
            # talib计算公式
            # data.loc[:, 'pdi'] = tl.PLUS_DI(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            # data['pdi'].values[np.isnan(data['pdi'].values)] = 0.0
            # data.loc[:, 'mdi'] = tl.MINUS_DI(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            # data['mdi'].values[np.isnan(data['mdi'].values)] = 0.0
            # data.loc[:, 'dx'] = tl.DX(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            # data['dx'].values[np.isnan(data['dx'].values)] = 0.0
            # data.loc[:, 'adx'] = tl.ADX(data['high'].values, data['low'].values, data['close'].values, timeperiod=6)
            # data['adx'].values[np.isnan(data['adx'].values)] = 0.0
            # data.loc[:, 'adxr'] = tl.ADXR(data['high'].values, data['low'].values, data['close'].values, timeperiod=6)
            # data['adxr'].values[np.isnan(data['adxr'].values)] = 0.0
            # stockstats计算公式
            data.loc[:, 'high_delta'] = np.insert(np.diff(data['high'].values), 0, 0.0)
            data.loc[:, 'high_m'] = (data['high_delta'].values + abs(data['high_delta'].values)) / 2
            data.loc[:, 'low_delta'] = np.insert(-np.diff(data['low'].values), 0, 0.0)
            data.loc[:, 'low_m'] = (data['low_delta'].values + abs(data['low_delta'].values)) / 2
            data.loc[:, 'pdm'] = tl.EMA(np.where(data['high_m'].values > data['low_m'].values, data['high_m'].values, 0), timeperiod=14)
            data['pdm'].values[np.isnan(data['pdm'].values)] = 0.0
            data.loc[:, 'pdi'] = data['pdm'].values / data['atr'].values
            data['pdi'].values[np.isnan(data['pdi'].values)] = 0.0
            data['pdi'].values[np.isinf(data['pdi'].values)] = 0.0
            data['pdi'] = data['pdi'].values * 100
            data.loc[:, 'mdm'] = tl.EMA(np.where(data['low_m'].values > data['high_m'].values, data['low_m'].values, 0), timeperiod=14)
            data['mdm'].values[np.isnan(data['mdm'].values)] = 0.0
            data.loc[:, 'mdi'] = data['mdm'].values / data['atr'].values
            data['mdi'].values[np.isnan(data['mdi'].values)] = 0.0
            data['mdi'].values[np.isinf(data['mdi'].values)] = 0.0
            data['mdi'] = data['mdi'].values * 100
            data.loc[:, 'dx'] = abs(data['pdi'].values - data['mdi'].values) / (data['pdi'].values + data['mdi'].values)
            data['dx'].values[np.isnan(data['dx'].values)] = 0.0
            data['dx'].values[np.isinf(data['dx'].values)] = 0.0
            data['dx'] = data['dx'].values * 100
            data.loc[:, 'adx'] = tl.EMA(data['dx'].values, timeperiod=6)
            data['adx'].values[np.isnan(data['adx'].values)] = 0.0
            data.loc[:, 'adxr'] = tl.EMA(data['adx'].values, timeperiod=6)
            data['adxr'].values[np.isnan(data['adxr'].values)] = 0.0

            # wr
            data.loc[:, 'wr_6'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=6)
            data['wr_6'].values[np.isnan(data['wr_6'].values)] = 0.0
            data.loc[:, 'wr_10'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=10)
            data['wr_10'].values[np.isnan(data['wr_10'].values)] = 0.0
            data.loc[:, 'wr_14'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            data['wr_14'].values[np.isnan(data['wr_14'].values)] = 0.0

            # cci 计算方法和结果和stockstats不同，stockstats典型价采用均价(总额/成交量)计算
            data.loc[:, 'cci'] = tl.CCI(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            data['cci'].values[np.isnan(data['cci'].values)] = 0.0
            data.loc[:, 'cci_84'] = tl.CCI(data['high'].values, data['low'].values, data['close'].values, timeperiod=84)
            data['cci_84'].values[np.isnan(data['cci_84'].values)] = 0.0

            # dma
            data.loc[:, 'ma10'] = tl.MA(data['close'].values, timeperiod=10)
            data['ma10'].values[np.isnan(data['ma10'].values)] = 0.0
            data.loc[:, 'ma50'] = tl.MA(data['close'].values, timeperiod=50)
            data['ma50'].values[np.isnan(data['ma50'].values)] = 0.0
            data.loc[:, 'dma'] = data['ma10'].values - data['ma50'].values
            data.loc[:, 'dma_10_sma'] = tl.MA(data['dma'].values, timeperiod=10)
            data['dma_10_sma'].values[np.isnan(data['dma_10_sma'].values)] = 0.0

            # tema
            data.loc[:, 'tema'] = tl.TEMA(data['close'].values, timeperiod=14)
            data['tema'].values[np.isnan(data['tema'].values)] = 0.0

            # mfi 计算方法和结果和stockstats不同，stockstats典型价采用均价(总额/成交量)计算
            data.loc[:, 'mfi'] = tl.MFI(data['high'].values, data['low'].values, data['close'].values, data['volume'].values, timeperiod=14)
            data['mfi'].values[np.isnan(data['mfi'].values)] = 0.0
            data.loc[:, 'mfisma'] = tl.MA(data['mfi'].values, timeperiod=6)

            # vwma
            data.loc[:, 'tpv_14'] = tl.SUM(data['amount'].values, timeperiod=14)
            data.loc[:, 'vol_14'] = tl.SUM(data['volume'].values, timeperiod=14)
            data.loc[:, 'vwma'] = data['tpv_14'].values / data['vol_14'].values
            data['vwma'].values[np.isnan(data['vwma'].values)] = 0.0
            data['vwma'].values[np.isinf(data['vwma'].values)] = 0.0
            data.loc[:, 'mvwma'] = tl.MA(data['vwma'].values, timeperiod=6)

            # ppo
            data.loc[:, 'ppo'] = tl.PPO(data['close'].values, fastperiod=12, slowperiod=26, matype=1)
            data['ppo'].values[np.isnan(data['ppo'].values)] = 0.0
            data.loc[:, 'ppos'] = tl.EMA(data['ppo'].values, timeperiod=9)
            data['ppos'].values[np.isnan(data['ppos'].values)] = 0.0
            data.loc[:, 'ppoh'] = data['ppo'].values - data['ppos'].values

            # stochrsi
            # talib计算公式和stockstats不同
            # talib计算公式
            # data.loc[:, 'stochrsi_k'], data.loc[:, 'stochrsi_d'] = tl.STOCHRSI(data['close'].values, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
            data.loc[:, 'rsi_min'] = tl.MIN(data['rsi'].values, timeperiod=14)
            data.loc[:, 'rsi_max'] = tl.MAX(data['rsi'].values, timeperiod=14)
            data.loc[:, 'stochrsi_k'] = (data['rsi'].values - data['rsi_min'].values) / (data['rsi_max'].values - data['rsi_min'].values)
            data['stochrsi_k'].values[np.isnan(data['stochrsi_k'].values)] = 0.0
            data['stochrsi_k'].values[np.isinf(data['stochrsi_k'].values)] = 0.0
            data['stochrsi_k'] = data['stochrsi_k'].values * 100
            data.loc[:, 'stochrsi_d'] = tl.MA(data['stochrsi_k'].values, timeperiod=3)

            # wt
            data.loc[:, 'esa'] = tl.EMA(data['m_price'].values, timeperiod=10)
            data['esa'].values[np.isnan(data['esa'].values)] = 0.0
            data.loc[:, 'esa_d'] = tl.EMA(abs(data['m_price'].values - data['esa'].values), timeperiod=10)
            data.loc[:, 'esa_ci'] = (data['m_price'].values - data['esa'].values) / (0.015 * data['esa_d'].values)
            data['esa_ci'].values[np.isnan(data['esa_ci'].values)] = 0.0
            data['esa_ci'].values[np.isinf(data['esa_ci'].values)] = 0.0
            data.loc[:, 'wt1'] = tl.EMA(data['esa_ci'].values, timeperiod=21)
            data['wt1'].values[np.isnan(data['wt1'].values)] = 0.0
            data.loc[:, 'wt2'] = tl.MA(data['wt1'].values, timeperiod=4)
            data['wt2'].values[np.isnan(data['wt2'].values)] = 0.0

            # Supertrend
            data.loc[:, 'm_atr'] = data['atr'].values * 3
            data.loc[:, 'hl_avg'] = (data['high'].values + data['low'].values) / 2.0
            data.loc[:, 'b_ub'] = data['hl_avg'].values + data['m_atr'].values
            data.loc[:, 'b_lb'] = data['hl_avg'].values - data['m_atr'].values
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
            data.loc[:, 'roc'] = tl.ROC(data['close'].values, timeperiod=12)
            data['roc'].values[np.isnan(data['roc'].values)] = 0.0
            data.loc[:, 'rocma'] = tl.MA(data['roc'].values, timeperiod=6)
            data['rocma'].values[np.isnan(data['rocma'].values)] = 0.0
            data.loc[:, 'rocema'] = tl.EMA(data['roc'].values, timeperiod=9)
            data['rocema'].values[np.isnan(data['rocema'].values)] = 0.0

            # obv
            data.loc[:, 'obv'] = tl.OBV(data['close'].values, data['volume'].values)
            data['obv'].values[np.isnan(data['obv'].values)] = 0.0

            # sar
            data.loc[:, 'sar'] = tl.SAR(data['high'].values, data['low'].values)
            data['sar'].values[np.isnan(data['sar'].values)] = 0.0

            # psy
            data.loc[:, 'price_up'] = 0.0
            data.loc[data['close'].values > data['prev_close'].values, 'price_up'] = 1.0
            data.loc[:, 'price_up_sum'] = tl.SUM(data['price_up'].values, timeperiod=12)
            data.loc[:, 'psy'] = data['price_up_sum'].values / 12.0
            data['psy'].values[np.isnan(data['psy'].values)] = 0.0
            data['psy'] = data['psy'].values * 100
            data.loc[:, 'psyma'] = tl.MA(data['psy'].values, timeperiod=6)

            # BRAR
            data.loc[:, 'h_o'] = data['high'].values - data['open'].values
            data.loc[:, 'o_l'] = data['open'].values - data['low'].values
            data.loc[:, 'h_o_sum'] = tl.SUM(data['h_o'].values, timeperiod=26)
            data.loc[:, 'o_l_sum'] = tl.SUM(data['o_l'].values, timeperiod=26)
            data.loc[:, 'ar'] = data['h_o_sum'] .values / data['o_l_sum'].values
            data['ar'].values[np.isnan(data['ar'].values)] = 0.0
            data['ar'].values[np.isinf(data['ar'].values)] = 0.0
            data['ar'] = data['ar'].values * 100
            data.loc[:, 'h_cy_sum'] = tl.SUM(data['h_cy'].values, timeperiod=26)
            data.loc[:, 'cy_l_sum'] = tl.SUM(data['cy_l'].values, timeperiod=26)
            data.loc[:, 'br'] = data['h_cy_sum'].values / data['cy_l_sum'].values
            data['br'].values[np.isnan(data['br'].values)] = 0.0
            data['br'].values[np.isinf(data['br'].values)] = 0.0
            data['br'] = data['br'].values * 100

            # EMV
            data.loc[:, 'prev_high'] = data['high'].shift(1, fill_value=0.0).values
            data.loc[:, 'prev_low'] = data['low'].shift(1, fill_value=0.0).values
            data.loc[:, 'phl_avg'] = (data['prev_high'].values + data['prev_low'].values) / 2.0
            data.loc[:, 'emva_em'] = (data['hl_avg'].values - data['phl_avg'].values) * data['h_l'].values / data['amount'].values
            data.loc[:, 'emv'] = tl.SUM(data['emva_em'].values, timeperiod=14)
            data['emv'].values[np.isnan(data['emv'].values)] = 0.0
            data.loc[:, 'emva'] = tl.MA(data['emv'].values, timeperiod=9)
            data['emva'].values[np.isnan(data['emva'].values)] = 0.0

            # BIAS
            data.loc[:, 'ma6'] = tl.MA(data['close'].values, timeperiod=6)
            data['ma6'].values[np.isnan(data['ma6'].values)] = 0.0
            data.loc[:, 'ma12'] = tl.MA(data['close'].values, timeperiod=12)
            data['ma12'].values[np.isnan(data['ma12'].values)] = 0.0
            data.loc[:, 'ma24'] = tl.MA(data['close'].values, timeperiod=24)
            data['ma24'].values[np.isnan(data['ma24'].values)] = 0.0
            data.loc[:, 'bias'] = ((data['close'].values - data['ma6'].values) / data['ma6'].values)
            data['bias'].values[np.isnan(data['bias'].values)] = 0.0
            data['bias'].values[np.isinf(data['bias'].values)] = 0.0
            data['bias'] = data['bias'].values * 100
            data.loc[:, 'bias_12'] = (data['close'].values - data['ma12'].values) / data['ma12'].values
            data['bias_12'].values[np.isnan(data['bias_12'].values)] = 0.0
            data['bias_12'].values[np.isinf(data['bias_12'].values)] = 0.0
            data['bias_12'] = data['bias_12'].values * 100
            data.loc[:, 'bias_24'] = (data['close'].values - data['ma24'].values) / data['ma24'].values
            data['bias_24'].values[np.isnan(data['bias_24'].values)] = 0.0
            data['bias_24'].values[np.isinf(data['bias_24'].values)] = 0.0
            data['bias_24'] = data['bias_24'].values * 100

            # DPO
            data.loc[:, 'c_m_11'] = tl.MA(data['close'].values, timeperiod=11)
            data.loc[:, 'dpo'] = data['close'].values - data['c_m_11'].shift(1, fill_value=0.0).values
            data['dpo'].values[np.isnan(data['dpo'].values)] = 0.0
            data.loc[:, 'madpo'] = tl.MA(data['dpo'].values, timeperiod=6)
            data['madpo'].values[np.isnan(data['madpo'].values)] = 0.0

            # VHF
            data.loc[:, 'hcp_lcp'] = tl.MAX(data['close'].values, timeperiod=28) - tl.MIN(data['close'].values, timeperiod=28)
            data['hcp_lcp'].values[np.isnan(data['hcp_lcp'].values)] = 0.0
            data.loc[:, 'vhf'] = np.divide(data['hcp_lcp'].values, tl.SUM(abs(data['close'].values - data['prev_close'].values), timeperiod=28))
            data['vhf'].values[np.isnan(data['vhf'].values)] = 0.0

            # RVI
            data.loc[:, 'rvi_x'] = ((data['close'].values - data['open'].values) +
                                    2 * (data['prev_close'].values - data['open'].shift(1, fill_value=0.0).values) +
                                    2 * (data['close'].shift(2, fill_value=0.0).values - data['open'].shift(2, fill_value=0.0).values) +
                                    (data['close'].shift(3, fill_value=0.0).values - data['open'].shift(3, fill_value=0.0).values)) / 6
            data.loc[:, 'rvi_y'] = ((data['high'].values - data['low'].values) +
                                    2 * (data['prev_high'].values - data['prev_low'].values) +
                                    2 * (data['high'].shift(2, fill_value=0.0).values - data['low'].shift(2, fill_value=0.0).values) +
                                    (data['high'].shift(3, fill_value=0.0).values - data['low'].shift(3, fill_value=0.0).values)) / 6
            data.loc[:, 'rvi'] = tl.MA(data['rvi_x'].values, timeperiod=10) / tl.MA(data['rvi_y'].values, timeperiod=10)
            data['rvi'].values[np.isnan(data['rvi'].values)] = 0.0
            data['rvi'].values[np.isinf(data['rvi'].values)] = 0.0
            data.loc[:, 'rvis'] = (data['rvi'].values +
                                   2 * data['rvi'].shift(1, fill_value=0.0).values +
                                   2 * data['rvi'].shift(2, fill_value=0.0).values +
                                   data['rvi'].shift(3, fill_value=0.0).values) / 6

            # FI
            data.loc[:, 'fi'] = np.insert(np.diff(data['close'].values), 0, 0.0) * data['volume'].values
            data.loc[:, 'force_2'] = tl.EMA(data['fi'].values, timeperiod=2)
            data['force_2'].values[np.isnan(data['force_2'].values)] = 0.0
            data.loc[:, 'force_13'] = tl.EMA(data['fi'].values, timeperiod=13)
            data['force_13'].values[np.isnan(data['force_13'].values)] = 0.0

            # ENE
            data.loc[:, 'ene_ue'] = (1 + 11 / 100) * data['ma10'].values
            data.loc[:, 'ene_le'] = (1 - 9 / 100) * data['ma10'].values
            data.loc[:, 'ene'] = (data['ene_ue'].values + data['ene_le'].values) / 2

            # VOL
            data.loc[:, 'vol_5'] = tl.MA(data['volume'].values, timeperiod=5)
            data['vol_5'].values[np.isnan(data['vol_5'].values)] = 0.0
            data.loc[:, 'vol_10'] = tl.MA(data['volume'].values, timeperiod=10)
            data['vol_10'].values[np.isnan(data['vol_10'].values)] = 0.0

            # MA
            data.loc[:, 'ma20'] = tl.MA(data['close'].values, timeperiod=20)
            data['ma20'].values[np.isnan(data['ma20'].values)] = 0.0
            data.loc[:, 'ma200'] = tl.MA(data['close'].values, timeperiod=200)
            data['ma200'].values[np.isnan(data['ma200'].values)] = 0.0

        if threshold is not None:
            data = data.tail(n=threshold).copy()
        return data
    except Exception as e:
        logging.error(f"calculate_indicator.get_indicators处理异常：{data['code']}代码{e}")
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
        if len(data.index) <= 1:
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
        logging.error(f"calculate_indicator.get_indicator处理异常：{code}代码{e}")
    return None
