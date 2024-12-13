#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import datetime as dt
import random
from typing import List, Tuple

from dateutil import tz
from instock.trade.robot.infrastructure.default_handler import DefaultLogHandler
from instock.trade.robot.infrastructure.strategy_template import StrategyTemplate
import instock.lib.database as mdb
import pandas as pd
import instock.core.tablestructure as tbs

class Strategy(StrategyTemplate):

    def init(self):
        self.buy_orders = []
        self.check_interval = dt.timedelta(minutes=5)
        self.last_check_time = dt.datetime.now()

    def buy_strategy(self):
        self.user.refresh()
        balance = pd.DataFrame([self.user.balance])
        if balance.empty:
            return

        cash = balance['可用金额'].values[0]
        if cash < 10000:
            return

        prepare_buy = self.get_stocks_to_buy()
        if not prepare_buy:
            return

        for code, price, amount in prepare_buy:
            order = self.user.buy(code, price=price, amount=amount)
            self.buy_orders.append(order)

        self.log.info('检查持仓')
        self.log.info(self.user.balance)
        self.log.info('\n')

    def get_stocks_to_buy(self) -> List[Tuple[str, float, int]]:
        yesterday = dt.datetime.now() - dt.timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')
        
        fetch = mdb.executeSqlFetch(
            f"SELECT * FROM `{tbs.TABLE_CN_STOCK_BUY_DATA['name']}` WHERE `date`='{date_str}'")
        pd_result = pd.DataFrame(fetch, columns=list(tbs.TABLE_CN_STOCK_BUY_DATA['columns']))
        
        if pd_result.empty:
            return []

        random_row = pd_result.sample(n=1)
        price = random_row['close'].values[0]
        cash = self.user.balance['可用金额']
        amount = int((cash / 2 / price) // 100) * 100
        return [(random_row['code'].values[0], float(price), amount)]

    def check_buy_orders(self):
        current_time = dt.datetime.now()
        if current_time - self.last_check_time >= self.check_interval:
            self.last_check_time = current_time
            completed_orders = []
            for order in self.buy_orders:
                if order.is_completed():
                    completed_orders.append(order)
                    self.log.info(f"买入订单完成: {order}")
            
            for completed_order in completed_orders:
                self.buy_orders.remove(completed_order)
            
            if completed_orders:
                self.user.refresh()
                self.log.info("更新后的持仓信息:")
                self.log.info(self.user.position)

    def clock(self, event):
        if event.data.clock_event in ('open', 'continue', 'close'):
            self.buy_strategy()
        self.check_buy_orders()

    def log_handler(self):
        cpath_current = os.path.dirname(os.path.dirname(__file__))
        cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
        log_filepath = os.path.join(cpath, 'log', f'{self.name}.log')
        return DefaultLogHandler(self.name, log_type='file', filepath=log_filepath)

    def shutdown(self):
        self.log.info("关闭前保存策略数据")
