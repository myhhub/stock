import backtrader as bt
import numpy as np

class BacktraceMA250(bt.Strategy):
    params = (
        ('period', 90),
    )

    def __init__(self):
        self.ma250 = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.period)
        self.highest = bt.indicators.Highest(self.data.close, period=60)
        self.volume = bt.indicators.SimpleMovingAverage(self.data.volume, period=5)

    def next(self):
        if len(self) < 60:
            return

        if not self.position:
            if (self.data.close[-60] < self.ma250[-60] and self.data.close[0] > self.ma250[0] and
                self.data.close[0] >= self.highest[-1] and
                self.data.volume[0] / self.volume[0] > 2 and
                self.data.close[0] / self.highest[0] < 0.8):
                self.buy()

        elif self.position:
            if self.data.close[0] < self.ma250[0]:
                self.sell()

class BreakthroughPlatform(bt.Strategy):
    params = (
        ('period', 60),
    )

    def __init__(self):
        self.ma60 = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.period)
        self.volume = bt.indicators.SimpleMovingAverage(self.data.volume, period=5)

    def next(self):
        if len(self) < self.params.period:
            return

        if not self.position:
            if (self.data.open[0] < self.ma60[0] <= self.data.close[0] and
                self.data.volume[0] > self.volume[0] * 2):
                for i in range(1, self.params.period):
                    if not -0.05 < (self.ma60[-i] - self.data.close[-i]) / self.ma60[-i] < 0.2:
                        return
                self.buy()

        elif self.position:
            if self.data.close[0] < self.ma60[0]:
                self.sell()

class ClimaxLimitdown(bt.Strategy):
    params = (
        ('volume_times', 4),
        ('amount_threshold', 200000000),
    )

    def __init__(self):
        self.volume_ma5 = bt.indicators.SimpleMovingAverage(self.data.volume, period=5)

    def next(self):
        if not self.position:
            if (self.data.close[0] / self.data.open[0] - 1 < -0.095 and
                self.data.close[0] * self.data.volume[0] >= self.params.amount_threshold and
                self.data.volume[0] >= self.volume_ma5[0] * self.params.volume_times):
                self.buy()

        elif self.position:
            if self.data.close[0] > self.data.open[0]:
                self.sell()


class EnterStrategy(bt.Strategy):
    params = (
        ('volume_times', 2),
        ('amount_threshold', 200000000),
    )

    def __init__(self):
        self.volume_ma5 = bt.indicators.SimpleMovingAverage(self.data.volume, period=5)

    def next(self):
        if not self.position:
            if (self.data.close[0] / self.data.open[0] - 1 > 0.02 and
                self.data.close[0] * self.data.volume[0] >= self.params.amount_threshold and
                self.data.volume[0] >= self.volume_ma5[0] * self.params.volume_times):
                self.buy()

        elif self.position:
            if self.data.close[0] < self.data.open[0]:
                self.sell()


class HighTightFlag(bt.Strategy):
    params = (
        ('lookback', 24),
        ('threshold', 1.9),
    )

    def __init__(self):
        self.order = None

    def next(self):
        if self.order:
            return

        if not self.position:
            lookback_low = min(self.data.low.get(size=self.params.lookback))
            if self.data.close[0] / lookback_low >= self.params.threshold:
                count = 0
                for i in range(1, self.params.lookback + 1):
                    if self.data.close[-i] / self.data.open[-i] - 1 >= 0.095:
                        count += 1
                    else:
                        count = 0
                    if count == 2:
                        self.order = self.buy()
                        break

        elif self.position:
            if self.data.close[0] < self.data.open[0]:
                self.order = self.sell()

class KeepIncreasing(bt.Strategy):
    params = (
        ('period', 30),
        ('threshold', 1.2),
    )

    def __init__(self):
        self.ma30 = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.period)

    def next(self):
        if len(self) < self.params.period:
            return

        if not self.position:
            if (self.ma30[0] > self.ma30[-10] > self.ma30[-20] > self.ma30[-30] and
                self.ma30[0] / self.ma30[-30] > self.params.threshold):
                self.buy()

        elif self.position:
            if self.data.close[0] < self.ma30[0]:
                self.sell()


class LowATRGrowth(bt.Strategy):
    params = (
        ('period', 10),
        ('atr_threshold', 10),
        ('growth_threshold', 1.1),
    )

    def __init__(self):
        self.atr = bt.indicators.ATR(self.data, period=self.params.period)

    def next(self):
        if len(self) < self.params.period:
            return

        if not self.position:
            highest = max(self.data.close.get(size=self.params.period))
            lowest = min(self.data.close.get(size=self.params.period))
            if (self.atr[0] <= self.params.atr_threshold and
                highest / lowest >= self.params.growth_threshold):
                self.buy()

        elif self.position:
            if self.data.close[0] < self.data.open[0]:
                self.sell()


class LowBacktraceIncrease(bt.Strategy):
    params = (
        ('period', 60),
        ('increase_threshold', 0.6),
        ('decline_threshold', -0.07),
        ('cumulative_threshold', -0.1),
    )

    def __init__(self):
        self.order = None

    def next(self):
        if self.order:
            return

        if len(self) < self.params.period:
            return

        if not self.position:
            if self.data.close[0] / self.data.close[-self.params.period] - 1 >= self.params.increase_threshold:
                for i in range(1, self.params.period + 1):
                    if (self.data.close[-i] / self.data.open[-i] - 1 <= self.params.decline_threshold or
                        self.data.close[-i] / self.data.close[-i-1] - 1 <= self.params.cumulative_threshold):
                        return
                self.order = self.buy()

        elif self.position:
            if self.data.close[0] < self.data.open[0]:
                self.order = self.sell()


class ParkingApron(bt.Strategy):
    params = (
        ('lookback', 15),
        ('increase_threshold', 0.095),
        ('consolidation_days', 3),
        ('consolidation_threshold', 0.03),
    )

    def __init__(self):
        self.order = None

    def next(self):
        if self.order:
            return

        if len(self) < self.params.lookback:
            return

        if not self.position:
            for i in range(1, self.params.lookback + 1):
                if self.data.close[-i] / self.data.open[-i] - 1 > self.params.increase_threshold:
                    if i >= self.params.consolidation_days:
                        is_consolidating = True
                        for j in range(1, self.params.consolidation_days + 1):
                            if abs(self.data.close[-j] / self.data.open[-j] - 1) > self.params.consolidation_threshold:
                                is_consolidating = False
                                break
                        if is_consolidating:
                            self.order = self.buy()
                            break

        elif self.position:
            if self.data.close[0] < self.data.open[0]:
                self.order = self.sell()


class TurtleTrade(bt.Strategy):
    params = (
        ('period', 60),
    )

    def __init__(self):
        self.highest = bt.indicators.Highest(self.data.close, period=self.params.period)

    def next(self):
        if len(self) < self.params.period:
            return

        if not self.position:
            if self.data.close[0] >= self.highest[-1]:
                self.buy()

        elif self.position:
            if self.data.close[0] < self.data.open[0]:
                self.sell()

class MACDStrategy(bt.Strategy):
    params = (
        ('fast_period', 12),
        ('slow_period', 26),
        ('signal_period', 9),
    )

    def __init__(self):
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.params.fast_period,
            period_me2=self.params.slow_period,
            period_signal=self.params.signal_period
        )
        self.crossover = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)

    def next(self):
        if not self.position:
            if self.crossover > 0:  # 金叉买入信号
                self.buy()
        else:
            if self.crossover < 0:  # 死叉卖出信号
                self.sell()