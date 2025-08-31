import datetime
import instock.lib.trade_time as trd
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.lib.database_factory import execute_sql, insert_db_from_df

def get_stock_exchange(code):
    """
    根据A股6位代码判断交易所
    
    Args:
        code (str): 6位股票代码
        
    Returns:
        str: 'sh' - 上海交易所, 'sz' - 深圳交易所, 'bj' - 北京交易所, None - 无法识别
    """
    if not code or len(code) != 6:
        return None
        
    # 上海交易所：600、601、603、605开头的A股，688科创板，900B股
    if code.startswith(('600', '601', '603', '605', '688', '900')):
        return 'sh'
    
    # 深圳交易所：000、001、002、003主板，300、301创业板，200B股
    elif code.startswith(('000', '001', '002', '003', '300', '301', '200')):
        return 'sz'
    
    # 北京交易所：430、83、87开头
    elif code.startswith(('430', '83', '87')) or (code.startswith('8') and len(code) == 6):
        return 'bj'
    
    # 无法识别的代码
    else:
        return None

def get_history_table_name_by_code(date, code, base_name="cn_stock_history"):
    """
    根据日期获取历史表名
    
    Args:
        base_name: 基础表名
        date: 日期对象或字符串（格式：YYYY-MM-DD）
    
    Returns:
        str: 历史表名，格式为 base_name_YYYYMMDD
    """
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
    # 数据是每5年一张表
    start_year = (date.year // 5) * 5
    end_year = start_year + 4
    market = get_stock_exchange(code)
    if market is None:
        return None
    table_name = f"{base_name}_{market}_{start_year}_{end_year}"
    return table_name

def check_and_delete_old_data_for_realtime_data(table_object, data, date, cols_type_only=False, save_to_history=True, use_batch=True):
    """
    检查并删除实时数据的旧数据，并可选择保存到历史数据库
    
    Args:
        table_object: 表对象
        data: 数据DataFrame
        date: 日期
        cols_type_only: 仅返回列类型
        save_to_history: 是否保存到历史数据库
        use_batch: 是否使用批量处理（推荐）
    
    Returns:
        cols_type if cols_type_only else None
    """
    table_name = table_object['name']
    # 删除老数据(仅实时)
    now = datetime.datetime.now()
    if trd.is_trade_date(now) and mdb.checkTableIsExist(table_name):
        del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
        execute_sql(del_sql)
        cols_type = None
    else:
        cols_type = tbs.get_field_types(table_object['columns'])
    
    if cols_type_only:
        return cols_type
    
    # 插入到实时表
    insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    
    # 保存到历史数据库
    if save_to_history and 'columns_to_history_db' in table_object:
        try:
            mdb.save_batch_realtime_data_to_history(data, table_object)
        except Exception as e:
            import logging
            logging.error(f"保存到历史数据库失败：{e}")
    
    return None

