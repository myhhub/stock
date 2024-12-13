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

if __name__ == '__main__':
    positions = [{'Unnamed: 24': '', '交易市场': '上海Ａ股', '冻结数量': 0, '单位数量': 1, '历史成交': '', '可用余额': 1000, '实际数量': 1000, '市价': 18.9, '市值': 18900.0, '市场代码': 2, '开仓日期': 20241209, '当日买入成交数量': 0, '当日卖出成交数量': 0, '成本价': 18.775, '成本金额': 18775.19, '摊薄保本价': 18.79, '流通类型': '流通', '盈亏': 110.17, '盈亏比例(%)': 0.587, '股东帐户': '="A811175096"', '股票余额': 1000, '股票类别': 'A0', '证券代码': '="603078"', '证券名称': '江化微', '资讯': ''}, {'Unnamed: 24': '', '交易市场': '深圳Ａ股', '冻结数量': 0, '单位数量': 1, '历史成交': '', '可用余额': 500, '实际数量': 500, '市价': 41.07, '市值': 20535.0, '市场代码': 1, '开仓日期': 20241210, '当日买入成交数量': 0, '当日卖出成交数量': 0, '成本价': 41.01, '成本金额': 20505.0, '摊薄保本价': 41.04, '流通类型': '流通', '盈亏': 14.73, '盈亏比例(%)': 0.072, '股东帐户': '="0241355428"', '股票余额': 500, '股票类别': 'A0', '证券代码': '="001229"', '证券名称': '魅视科技', '资讯': ''}, {'Unnamed: 24': '', '交易市场': '深圳Ａ股', '冻结数量': 0, '单位数量': 1, '历史成交': '', '可用余额': 400, '实际数量': 400, '市价': 60.28, '市值': 24112.0, '市场代码': 1, '开仓日期': 20241120, '当日买入成交数量': 0, '当日卖出成交数量': 0, '成本价': 58.665, '成本金额': 23466.0, '摊薄保本价': 58.708, '流通类型': '流通', '盈亏': 628.26, '盈亏比例(%)': 2.677, '股东帐户': '="0241355428"', '股票余额': 400, '股票类别': 'CR', '证券代码': '="300917"', '证券名称': '特发服务', '资讯': ''}]

    positions_en = [
        {
            'market': '上海Ａ股' if p['交易市场'] == '上海Ａ股' else '深圳A股',
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
    table_name = tbs.TABLE_CN_STOCK_POSITION['name']
    # 删除老数据。
    if mdb.checkTableIsExist(table_name):
        del_sql = f"DELETE FROM `{table_name}`"
        mdb.executeSql(del_sql)
        cols_type = None
    else:
        cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_POSITION['columns'])

    df['date'] = dt.date.today()
    mdb.insert_db_from_df(df, table_name, cols_type, False, "`date`,`code`")
