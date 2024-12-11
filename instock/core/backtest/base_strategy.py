import backtrader as bt


class BaseStrategy(bt.Strategy):
    params = (
        ('stop_loss', 0.06),
        ('take_profit', 0.30),
    )

    def __init__(self):
        self.orders = {}
        self.position_value = {}
        self.buyprice = {}
        self.buycomm = {}
        self.stop_loss_price = {}
        self.take_profit_price = {}

    def buy_stock(self, data, size=None):
        if size is None:
            size = self.calculate_buy_size(data)
        if size > 0:
            limit_up_price = self.calculate_limit_up_price(data)
            if data.close[0] >= limit_up_price:
                self.log(f'涨停无法买入: {data._name}, 价格: {data.close[0]}, 涨停价: {limit_up_price}')
                return

            if data.close[0] > 0:
                self.stop_loss_price[data] = max(data.close[0] * (1 - self.params.stop_loss), 0.01)
                self.take_profit_price[data] = max(data.close[0] * (1 + self.params.take_profit), 0.01)
            else:
                self.log(f'警告: {data._name} 的收盘价为0，无法设置止损和止盈')
                return

            self.orders[data] = self.buy(data=data, size=size)
            self.position_value[data] = size * data.close[0]
        else:
            self.log(f'可用资金不足，无法买入: {data._name}, 可用资金: {self.broker.getvalue() - sum(self.position_value.values())}')

    def calculate_buy_size(self, data, ratio=None):
        available_cash = self.broker.getvalue() - sum(self.position_value.values())
        if available_cash <= 0:
            return 0
        if ratio is None:
            if self.position:
                ratio = 0.2
            else:
                ratio = 0.5
        return int(round(available_cash * ratio / data.close[0] / 100) * 100)

    def check_sell_strategy(self, data):
        position = self.getposition(data)
        if position:
            if data in self.stop_loss_price and data.close[0] <= self.stop_loss_price[data]:
                self.log(f'触发止损: {data._name}, 价格: {data.close[0]}')
                self.close(data)
                return True
            if data in self.take_profit_price and data.close[0] >= self.take_profit_price[data]:
                if data.low[0] < data.low[-1] and data.high[0] < data.high[-1]:
                    self.log(f'触发最低点低于昨日最低点卖出: {data._name}, 价格: {data.close[0]}')
                    self.close(data)
                    return True
        if data.low[0] < data.low[-1] < data.low[-2] and data.high[0] < data.high[-1] < data.high[-2]:
            if position:
                self.log(f'触发最高和最低点低于昨日卖出: {data._name}, 价格: {data.close[0]}')
                self.close(data)
            return True

        if data.volume[0] == max(data.volume.get(size=7) or [0]) and data.close[0] < data.low[-1]:
            if position:
                self.log(f'触发成交量和价格条件卖出: {data._name}, 价格: {data.close[0]}')
                self.close(data)
            return True
        return False

    @staticmethod
    def calculate_limit_up_price(data):
        previous_close = data.close[-1]
        stock_code = data._name[:6]
        limit_up_ratio = 0.20 if stock_code.startswith('300') else 0.10
        base_limit_up_price = round(previous_close * (1 + limit_up_ratio), 2)
        return base_limit_up_price * 1.001

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}, {txt}')

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.buyprice[order.data] = order.executed.price
                self.buycomm[order.data] = order.executed.comm
                self.log(
                    f'买入执行: {order.data._name}, 价格: {order.executed.price:.2f}, '
                    f'成本: {order.executed.value:.2f}, 佣金: {order.executed.comm:.2f}, '
                    f'数量: {order.executed.size}, '
                    f'止损价: {self.stop_loss_price.get(order.data, 0):.2f}, '
                    f'止盈价: {self.take_profit_price.get(order.data, 0):.2f}'
                )
            else:
                buyprice = self.buyprice[order.data]
                buycomm = self.buycomm[order.data]
                sellprice = order.executed.price
                sellcomm = order.executed.comm
                profit = (sellprice - buyprice) * order.executed.size * -1 - buycomm - sellcomm
                profit_pct = (profit / (buyprice * order.executed.size)) * 100 * -1
                self.log(
                    f'卖出执行: {order.data._name}, 价格: {sellprice:.2f}, '
                    f'成本: {order.executed.value:.2f}, 佣金: {sellcomm:.2f}, '
                    f'数量: {order.executed.size}, 利润: {profit:.2f}, '
                    f'利润率: {profit_pct:.2f}%'
                )
                self.position_value[order.data] = 0
                self.stop_loss_price.pop(order.data, None)
                self.take_profit_price.pop(order.data, None)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单被取消/保证金不足/拒绝: {order.data._name}')

        self.orders[order.data] = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'交易利润: {trade.data._name}, 毛利: {trade.pnl:.2f}, 净利: {trade.pnlcomm:.2f}')

    def stop(self):
        self.log(f'最终投资组合价值: {self.broker.getvalue():.2f}')
        for data in self.datas:
            position = self.getposition(data)
            if position.size != 0:
                unrealized_pnl = position.size * (data.close[0] - self.buyprice[data]) - self.buycomm[data]
                unrealized_pnl_pct = (unrealized_pnl / (self.buyprice[data] * position.size)) * 100
                self.log(
                    f'最终持仓 {data._name}: {position.size} 股, '
                    f'价值: {position.size * data.close[0]:.2f}, '
                    f'未实现盈亏: {unrealized_pnl:.2f}, '
                    f'未实现盈亏率: {unrealized_pnl_pct:.2f}%'
                )
