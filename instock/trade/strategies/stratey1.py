#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import datetime as dt
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

        # 注册时钟间隔事件, 不在交易阶段也会触发, clock_type == minute_interval
        minute_interval = 3
        self.clock_engine.register_interval(minute_interval, trading=True)
        return

    def save_position_to_db(self):
        positions = self.user.position
        if not positions:
            self.log.info("没有持仓信息")
            return
        # Convert positions data to English and map columns
        positions_en = [
            {
                'market': 'Shanghai A-share' if p['交易市场'] == '上海Ａ股' else '深圳 A股',
                'frozen_quantity': p['冻结数量'],
                'unit_quantity': p['单位数量'],
                'available_balance': p['可用余额'],
                'actual_quantity': p['实际数量'],
                'market_price': p['市价'],
                'market_value': p['市值'],
                'market_code': p['市场代码'],
                'opening_date': p['开仓日期'],
                'cost_price': p['成本价'],
                'cost_amount': p['成本金额'],
                'break_even_price': p['摊薄保本价'],
                'circulation_type': 'Circulating' if p['流通类型'] == '流通' else p['流通类型'],
                'profit_loss': p['盈亏'],
                'profit_loss_ratio': p['盈亏比例(%)'],
                'account': p['股东帐户'].strip('="'),
                'stock_balance': p['股票余额'],
                'stock_category': p['股票类别'],
                'code': p['证券代码'].strip('="'),
                'name': p['证券名称']
            } for p in positions
        ]

        # Convert to DataFrame
        df = pd.DataFrame(positions_en)

        df = pd.DataFrame(positions)
        data = df.rename(columns={
            '证券代码': 'code',
            '证券名称': 'name',
            '成本价': 'cost_price',
            '可用余额': 'available_shares',
            '市价': 'market_price',
            '市值': 'market_value',
            '买入冻结' : 'freeze',
        })

        table_name = tbs.TABLE_CN_STOCK_POSITION['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}`"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_POSITION['columns'])

        df['date'] = dt.date.today()
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        self.log.info(f"持仓信息{data}已保存到数据库表 {table_name}")

    def save_account_to_db(self):
        balance = self.user.balance
        if not balance:
            self.log.info("没有用户信息")
            return
        df = pd.DataFrame([balance])
        data = df.rename(columns={
            '参考市值': 'market_cap',
            '可用资金': 'available_funds',
            '币种': 'cost_price',
            '总资产': 'total_assets',
            '股份参考盈亏': 'profit_loss',
            '资金余额': 'balance',
            '资金帐号': 'account_id',
        })

        table_name = tbs.TABLE_CN_STOCK_ACCOUNT['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}`"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_ACCOUNT['columns'])

        df['date'] = dt.date.today()
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`account_id`")
        self.log.info(f"账户信息{data}已保存到数据库表 {table_name}")

    def clock(self, event):
        """在交易时间会定时推送 clock 事件"""
        self.save_account_to_db()
        self.save_position_to_db()

    def log_handler(self):
        """自定义 log 记录方式"""
        cpath_current = os.path.dirname(os.path.dirname(__file__))
        cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
        log_filepath = os.path.join(cpath, 'log', f'{self.name}.log')
        return DefaultLogHandler(self.name, log_type='file', filepath=log_filepath)

    def shutdown(self):
        """关闭进程前的调用"""
        self.log.info("假装在关闭前保存了策略数据")
