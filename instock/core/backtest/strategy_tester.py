import backtrader as bt
import pandas as pd
from datetime import datetime, timedelta
import random
from instock.core.singleton_stock import stock_hist_data, stock_data
import instock.core.tablestructure as tbs
from instock.core.backtest.strategy_group import *

class StrategyTester:
    def __init__(self):
        self.strategies = [
            BacktraceMA250, BreakthroughPlatform, ClimaxLimitdown,
            EnterStrategy, HighTightFlag, KeepIncreasing, LowATRGrowth,
            LowBacktraceIncrease, ParkingApron, TurtleTrade, MACDStrategy
        ]

    def get_data(self, date, rand_num=10):
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

    def run_backtest(self, strategy, data):
        cerebro = bt.Cerebro()
        cerebro.addstrategy(strategy)
        cerebro.adddata(data)
        cerebro.broker.setcash(100000.0)
        cerebro.broker.setcommission(commission=0.001)
        cerebro.addsizer(bt.sizers.PercentSizer, percents=95)

        # 添加交易分析器
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")

        initial_value = cerebro.broker.getvalue()
        results = cerebro.run()
        final_value = cerebro.broker.getvalue()

        return results[0], initial_value, final_value

    def evaluate_strategy(self, strategy, data):
        try:
            strategy_results, initial_value, final_value = self.run_backtest(strategy, data)

            # 获取交易分析结果
            trade_analyzer = strategy_results.analyzers.trades.get_analysis()

            # 计算总交易次数和盈利交易次数
            total_trades = trade_analyzer.total.closed
            won_trades = trade_analyzer.won.total if hasattr(trade_analyzer, 'won') else 0

            max_profit = final_value - initial_value
            win_rate = won_trades / total_trades if total_trades > 0 else 0

            return {
                'strategy': strategy.__name__,
                'max_profit': max_profit,
                'win_rate': win_rate
            }
        except Exception as e:
            print(f"Error evaluating strategy {strategy.__name__}: {str(e)}")
            return None

    def run_all_strategies(self, date=None, iterations=5):
        if date is None:
            date = datetime.now() - timedelta(days=1)

        all_results = []

        for _ in range(iterations):
            data_list = self.get_data(date)
            for data in data_list:
                for strategy in self.strategies:
                    result = self.evaluate_strategy(strategy, data)
                    if result:
                        all_results.append(result)

        return all_results

    def find_best_strategy(self, results):
        strategy_performance = {}
        for result in results:
            strategy_name = result['strategy']
            if strategy_name not in strategy_performance:
                strategy_performance[strategy_name] = {
                    'total_profit': 0,
                    'total_win_rate': 0,
                    'count': 0
                }
            strategy_performance[strategy_name]['total_profit'] += result['max_profit']
            strategy_performance[strategy_name]['total_win_rate'] += result['win_rate']
            strategy_performance[strategy_name]['count'] += 1

        for strategy, performance in strategy_performance.items():
            performance['avg_profit'] = performance['total_profit'] / performance['count']
            performance['avg_win_rate'] = performance['total_win_rate'] / performance['count']

        best_strategy = max(strategy_performance.items(), key=lambda x: x[1]['avg_profit'])
        return best_strategy

if __name__ == '__main__':
    tester = StrategyTester()
    results = tester.run_all_strategies(iterations=5)

    print("All Strategy Results:")
    for result in results:
        print(f"Strategy: {result['strategy']}, Max Profit: {result['max_profit']:.2f}, Win Rate: {result['win_rate']:.2f}")

    best_strategy, performance = tester.find_best_strategy(results)
    print(f"\nBest Strategy: {best_strategy}")
    print(f"Average Profit: {performance['avg_profit']:.2f}")
    print(f"Average Win Rate: {performance['avg_win_rate']:.2f}")
