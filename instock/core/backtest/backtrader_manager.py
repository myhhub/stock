import os
import random
import sys
from datetime import datetime, timedelta

import backtrader as bt
import matplotlib
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

import instock.core.tablestructure as tbs
from instock.core.backtest.Chan import ImprovedChanStrategy
from instock.core.backtest.volume_break_strategy import VolumeBreakStrategy
from instock.core.singleton_stock import stock_hist_data, stock_data
from instock.lib.singleton_type import singleton_type

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)


class back_test(metaclass=singleton_type):
    def bt_strategy(self, date=None, strategy=None,rand_num=2):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        if strategy is None:
            raise Exception("策略类为空")
        data = self.get_data(date=date, rand_num=rand_num)
        cerebro = bt.Cerebro()
        cerebro.addstrategy(strategy)
        cerebro.broker.set_cash(100000.0)
        cerebro.addsizer(bt.sizers.PercentSizer, percents=50)
        cerebro.broker.setcommission(commission=0.0001)
        for d in data:
            cerebro.adddata(d)

        cerebro.run()

        # 计算收益率
        initial_value = 100000.0
        final_value = cerebro.broker.getvalue()
        profit = final_value - initial_value
        profit_rate = (profit / initial_value) * 100

        print(f'总收益: ¥{profit:.2f}')
        print(f'收益率: {profit_rate:.2f}%')
        # 设置中文字体
        matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']
        matplotlib.rcParams['axes.unicode_minus'] = False
        figs = cerebro.plot(volume=True, style='candle',
                            barup='red', bardown='green',
                            volup='red', voldown='green',
                            subplot=False,  # 不使用子图
                            voloverlay=False,  # 成交量不覆盖在K线上
                            volscaling=0.3,  # 成交量高度占比
                            volpushup=1.0)
        # 绘制图表，包括K线图
        fig = figs[0][0]
        fig.set_size_inches(40, 20)
        fig.suptitle("策略回测结果", fontsize=16)
        fig.grid(False)
        ax1 = fig.axes[0]  # 获取主要的坐标轴

        # 显示图表
        # 设置x轴日期格式
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
        plt.tight_layout()
        plt.show()

    def bt_strategy_multiple(self, date=None, strategy=None, iterations=10):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        if strategy is None:
            raise Exception("策略类为空")

        results = []
        data = self.get_data(date, rand_num=iterations)
        for i in range(len(data)):
            cerebro = bt.Cerebro()
            cerebro.addstrategy(strategy)
            cerebro.broker.set_cash(100000.0)
            cerebro.addsizer(bt.sizers.PercentSizer, percents=50)
            cerebro.broker.setcommission(commission=0.0001)
            cerebro.adddata(data[i])

            initial_value = cerebro.broker.getvalue()
            cerebro.run()
            final_value = cerebro.broker.getvalue()
            profit = final_value - initial_value
            profit_rate = (profit / initial_value) * 100

            results.append({
                'iteration': i + 1,
                'initial_value': initial_value,
                'final_value': final_value,
                'profit': profit,
                'profit_rate': profit_rate
            })

            print(f'迭代次数 {i + 1}: 利润: {profit:.2f}, 利润率: {profit_rate:.2f}%')


        # 统计结果
        profits = [r['profit'] for r in results]
        profit_rates = [r['profit_rate'] for r in results]
        winning_trades = sum(1 for p in profits if p > 0)

        print("\n总结:")
        print(f"总迭代次数: {iterations}")
        print(f"盈利交易次数: {winning_trades}")
        print(f"胜率: {winning_trades / iterations * 100:.2f}%")
        print(f"平均利润: {np.mean(profits):.2f}")
        print(f"平均利润率: {np.mean(profit_rates):.2f}%")
        print(f"最大利润: {max(profits):.2f}")
        print(f"最小利润: {min(profits):.2f}")
        print(f"利润标准差: {np.std(profits):.2f}")

        return results

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
                dataname=row,
                name=idx[1] + idx[2]
            )
            data_list.append(data)

        return data_list


if __name__ == '__main__':
    test = back_test()
    test.bt_strategy_multiple(strategy=VolumeBreakStrategy,  iterations=200)
    # test.bt_strategy(strategy=VolumeBreakStrategy, rand_num=10)
