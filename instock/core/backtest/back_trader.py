import os
import random
import sys
from datetime import datetime, timedelta

import backtrader as bt
import pandas as pd
import matplotlib
from instock.core.singleton_stock import stock_hist_data, stock_data
from instock.lib.singleton_type import singleton_type
import instock.core.tablestructure as tbs

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)


class CustomStrategy(bt.Strategy):
    params = (
        ('maperiod5', 5),
        ('maperiod13', 13),
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.m5 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod5)
        self.m13 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod13)

    def next(self):
        if self.order:
            return

        if not self.position:
            if self.m5[0] > self.m13[0]:
                self.order = self.buy(price=self.m5[0])
        else:
            if self.dataclose[0] < self.m5[0]:
                self.order = self.sell(price=self.m13[0])

class back_test(metaclass=singleton_type):
    def bt_strategy(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=-1)
        data = self.get_data(date, rand_num=5)
        cerebro = bt.Cerebro()
        cerebro.addstrategy(CustomStrategy)
        cerebro.broker.set_cash(100000.0)
        cerebro.addsizer(bt.sizers.PercentSizer, percents=50)
        cerebro.broker.setcommission(commission=0.001)
        for d in data:
            cerebro.adddata(d)
        cerebro.run()
        cerebro.plot()

    def get_data(self, date, rand_num=100):
        columns_ = stock_data(date).get_data()[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        stocks = [tuple(x) for x in columns_.values]
        random_stocks = random.sample(stocks, rand_num)
        data_list = []
        df = stock_hist_data(date=date, stocks=random_stocks).get_data()
        for idx, row in df.items():
            if len(row) <= 100:
                continue
            row['datetime'] = pd.to_datetime(row['date'])
            row.set_index('datetime', inplace=True)
            data = bt.feeds.PandasData(
                dataname=row
            )
            data_list.append(data)

        return data_list


if __name__ == '__main__':
    test = back_test()
    test.bt_strategy()
