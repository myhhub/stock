#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import datetime as dt
import random

from dateutil import tz
from instock.trade.robot.infrastructure.default_handler import DefaultLogHandler
from instock.trade.robot.infrastructure.strategy_template import StrategyTemplate
import instock.lib.database as mdb
import pandas as pd
import instock.core.tablestructure as tbs
__author__ = 'myh '
__date__ = '2023/4/10 '

# 更新持仓
class Strategy(StrategyTemplate):

    def init(self):
        # 注册盘后事件
        # after_trading_moment = dt.time(15, 0, 0, tzinfo=tz.tzlocal())
        # self.clock_engine.register_moment(self.name, after_trading_moment)
        return

    def strategy(self):
        # [{'参考市值': 21642.0,
        #   '可用资金': 28494.21,
        #   '币种': '0',
        #   '总资产': 50136.21,
        #   '股份参考盈亏': -90.21,
        #   '资金余额': 28494.21,
        #   '资金帐号': 'xxx'}]
        self.user.refresh()
        balance = pd.DataFrame([self.user.balance])
        if len(balance) <  1:
            return

        # 测试
        # 查询昨日生产的可以买入股票
        yeasterDay = dt.datetime.now() - dt.timedelta(days=1)
        datetime = yeasterDay.strftime('%Y-%m-%d')
        prepare_buy = []
        fetch = mdb.executeSqlFetch(
            f"SELECT * FROM `{tbs.TABLE_CN_STOCK_BUY_DATA['name']}` WHERE `date`='{datetime}'")
        pd_result = pd.DataFrame(fetch, columns=list(tbs.TABLE_CN_STOCK_BUY_DATA['columns']))
        cash = balance['可用金额']
        if cash < 10000:
            return
        if not pd_result.empty:
            # 随机选一个
            random_row = pd_result.sample(n=1)
            price = random_row['close'].values[0]
            amount = int((cash / 2 / price) // 100) * 100
            prepare_buy = [(random_row['code'].values[0], float(price), amount)]

        if not prepare_buy or  len(prepare_buy) < 1:
            return
        # --------写交易策略---------

        # --------写交易策略---------
        for buy in prepare_buy:
            self.user.buy(buy[0], price=buy[1], amount=buy[3])

        self.log.info('检查持仓')
        self.log.info(self.user.balance)
        self.log.info('\n')


    def clock(self, event):
        """在交易时间会定时推送 clock 事件"""
        if event.data.clock_event in ('open','continue', 'close'):
            self.strategy()

    def log_handler(self):
        """自定义 log 记录方式"""
        cpath_current = os.path.dirname(os.path.dirname(__file__))
        cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
        log_filepath = os.path.join(cpath, 'log', f'{self.name}.log')
        return DefaultLogHandler(self.name, log_type='file', filepath=log_filepath)

    def shutdown(self):
        """关闭进程前的调用"""
        self.log.info("假装在关闭前保存了策略数据")





