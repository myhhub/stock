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


def test_buy():
    yesterday = dt.datetime.now() - dt.timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')

    fetch = mdb.executeSqlFetch(
        f"SELECT * FROM `{tbs.TABLE_CN_STOCK_BUY_DATA['name']}` WHERE `date`='{date_str}'")
    pd_result = pd.DataFrame(fetch, columns=list(tbs.TABLE_CN_STOCK_BUY_DATA['columns']))

    if pd_result.empty:
        return []
    cash = 100000
    random_row = pd_result.sample(n=1)
    price = random_row['close'].values[0]
    amount = int((cash / 2 / price) // 100) * 100
    return [(random_row['code'].values[0], float(price), amount)]

if __name__ == '__main__':
    buys = test_buy()
    for buy in buys:
        print(f'buy: {buy[0]} price:{buy[1]} amount: {buy[2]}')
