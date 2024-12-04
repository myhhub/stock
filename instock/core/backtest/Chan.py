import os
import random
import sys
from datetime import datetime, timedelta

import backtrader as bt
import pandas as pd
import matplotlib

from instock.core.backtest.base_strategy import BaseStrategy
from instock.core.singleton_stock import stock_hist_data, stock_data
from instock.lib.singleton_type import singleton_type
import instock.core.tablestructure as tbs

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

class ChanKline:
    def __init__(self, open, high, low, close, date):
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.date = date

class Segment:
    def __init__(self, start, end, direction):
        self.start = start
        self.end = end
        self.direction = direction  # 1 for up, -1 for down

class Pivot:
    def __init__(self, kline, type):
        self.kline = kline
        self.type = type  # 'high' or 'low'

class CentralZone:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.high = max(start.high, end.high)
        self.low = min(start.low, end.low)

class ChanIndicator(bt.Indicator):
    lines = ('buy_signal', 'sell_signal')
    params = (('period', 20),
              ('stop_loss', 0.65),  # 10% 止损
              )

    def __init__(self):
        self.addminperiod(self.params.period)
        self.merged_klines = []
        self.segments = []
        self.pivots = []
        self.central_zones = []
        self.stop_loss_price = {}  # 用于记录每个数据源的止损价格

    def next(self):
        current_kline = ChanKline(self.data.open[0], self.data.high[0], self.data.low[0], self.data.close[0], self.data.datetime.date(0))
        self.merge_klines(current_kline)
        self.identify_pivots()
        self.identify_segments()
        self.identify_central_zones()

        self.lines.buy_signal[0] = self.is_buy_point()
        self.lines.sell_signal[0] = self.is_sell_point()

    def merge_klines(self, new_kline):
        if not self.merged_klines:
            self.merged_klines.append(new_kline)
            return

        last_kline = self.merged_klines[-1]
        if (new_kline.high > last_kline.high and new_kline.low < last_kline.low) or \
           (new_kline.high < last_kline.high and new_kline.low > last_kline.low):
            merged_kline = ChanKline(
                last_kline.open,
                max(last_kline.high, new_kline.high),
                min(last_kline.low, new_kline.low),
                new_kline.close,
                new_kline.date
            )
            self.merged_klines[-1] = merged_kline
        else:
            self.merged_klines.append(new_kline)

    def identify_pivots(self):
        if len(self.merged_klines) < 3:
            return

        for i in range(1, len(self.merged_klines) - 1):
            prev, curr, next = self.merged_klines[i-1:i+2]
            if curr.high > prev.high and curr.high > next.high:
                self.pivots.append(Pivot(curr, 'high'))
            elif curr.low < prev.low and curr.low < next.low:
                self.pivots.append(Pivot(curr, 'low'))

    def identify_segments(self):
        if len(self.pivots) < 2:
            return

        for i in range(len(self.pivots) - 1):
            start, end = self.pivots[i], self.pivots[i+1]
            if start.type != end.type:
                direction = 1 if start.type == 'low' else -1
                self.segments.append(Segment(start.kline, end.kline, direction))

    def identify_central_zones(self):
        if len(self.segments) < 3:
            return

        for i in range(len(self.segments) - 2):
            seg1, seg2, seg3 = self.segments[i:i+3]
            if seg1.direction != seg3.direction:
                overlap_high = min(seg1.end.high, seg2.end.high, seg3.start.high)
                overlap_low = max(seg1.end.low, seg2.end.low, seg3.start.low)
                if overlap_high > overlap_low:
                    self.central_zones.append(CentralZone(seg1.end, seg3.start))

    def is_buy_point(self):
        if not self.central_zones:
            return 0

        last_zone = self.central_zones[-1]
        last_kline = self.merged_klines[-1]
        prev_kline = self.merged_klines[-2]

        # 第一次突破中枢
        if prev_kline.close <= last_zone.high and last_kline.close > last_zone.high:
            return 1

        # 回调后收盘价高于中枢
        if self.lines.buy_signal[-1] == 1 and last_kline.close > last_zone.high:
            return 2

        # 回调跌破中枢
        if self.lines.buy_signal[-1] == 1 and last_kline.close <= last_zone.high:
            return 3

        return 0

    def is_sell_point(self):
        if not self.central_zones:
            return 0

        last_zone = self.central_zones[-1]
        last_kline = self.merged_klines[-1]

        # 第一类卖点：向上离开中枢后回调不升破中枢
        # if last_kline.high < last_zone.high and last_kline.close < last_zone.low:
        #     return 1

        # 第二类卖点：在中枢中震荡后向下突破
        if last_kline.close < last_zone.low and self.merged_klines[-2].close >= last_zone.low:
            return 2

        return 0

    def is_divergence(self, seg1, seg2):
        # 简单的背驰判断，可以根据实际需求进行调整
        price_change1 = abs(seg1.end.close - seg1.start.close)
        price_change2 = abs(seg2.end.close - seg2.start.close)
        volume1 = sum([k.close for k in self.merged_klines[self.merged_klines.index(seg1.start):self.merged_klines.index(seg1.end)+1]])
        volume2 = sum([k.close for k in self.merged_klines[self.merged_klines.index(seg2.start):self.merged_klines.index(seg2.end)+1]])
        return (seg1.direction == seg2.direction) and (price_change2 > price_change1) and (volume2 < volume1)

class ChanStrategy(BaseStrategy):
    params = (
        ('maperiod', 20),
    )

    def __init__(self):
        super().__init__()
        self.chan_indicators = {}
        for d in self.datas:
            self.chan_indicators[d] = ChanIndicator(d, period=self.params.maperiod)

    def next(self):
        for data in self.datas:
            self.process_data(data)
            self.check_sell_strategy(data)

    def process_data(self, data):
        if self.orders.get(data):
            return

        position = self.getposition(data)
        buy_signal = self.chan_indicators[data].buy_signal[0]

        if not position:
            if buy_signal == 2:
                self.log(f'创建买入订单: {data._name} (回调后收盘价高于中枢), 价格: {data.close[0]}')
                self.buy_stock(data)
            elif buy_signal == 3:
                self.log(f'回调跌破中枢: {data._name}, 价格: {data.close[0]}')
                self.orders[data] = 'waiting'
        else:
            if buy_signal == 3 and self.orders.get(data) == 'waiting':
                if data.close[0] < data.low[-1]:
                    self.log(f'创建卖出订单: {data._name} (跌破中枢后创新低), 价格: {data.close[0]}')
                    self.close(data)
                else:
                    self.log(f'继续持有: {data._name} (跌破中枢但未创新低), 价格: {data.close[0]}')
                self.orders[data] = None


