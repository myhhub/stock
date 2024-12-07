import backtrader as bt
import numpy as np
from instock.core.backtest.base_strategy import BaseStrategy


class VolumeBreakStrategy(BaseStrategy):
    params = (
        ('volume_threshold', 1.5),  # 成交量阈值
        ('price_drop_threshold', 0.10),  # 价格下跌阈值
        ('lookback_period', 7),  # 回看天数
    )

    def __init__(self):
        super().__init__()
        self.volume_ma = bt.indicators.SimpleMovingAverage(
            self.data.volume, period=self.params.lookback_period)
        self.high_price = bt.indicators.Highest(
            self.data.high, period=self.params.lookback_period)
        self.low_price = bt.indicators.Lowest(
            self.data.low, period=self.params.lookback_period)
        self.max_volume = bt.indicators.Highest(
            self.data.volume, period=self.params.lookback_period)

    def next(self):
        for data in self.datas:
            self.process_data(data)

    def process_data(self, data):
        if self.orders.get(data):
            return


        position = self.getposition(data)
        if position:
            sell_signal = self.check_sell_strategy(data)
            if sell_signal:
                return
        if self.buy_condition(data):
            self.log(f'买入信号: {data._name}, 价格: {data.close[0]}')
            self.buy_stock(data)
            return


    def buy_condition(self, data):
        # 条件1: 最近七天有一天成交量大于当天-7天成交量均值的150%
        volume_condition = np.any(np.array(data.volume.get(size=self.params.lookback_period)) >
                                  self.volume_ma * self.params.volume_threshold)

        # 条件2: 当天的收盘价低于最近七天的最高价的15%
        price_condition = data.close[0] < self.high_price[0] * (1 - self.params.price_drop_threshold)

        # 条件3: 当天的成交量低于最高日的成交量
        current_volume_condition = data.volume[0] < self.max_volume[0]

        # 条件4: 当天的最低价格高于昨天的最低价
        low_price_condition = data.low[0] > data.low[-1]

        return (volume_condition and price_condition and
                current_volume_condition and low_price_condition)
