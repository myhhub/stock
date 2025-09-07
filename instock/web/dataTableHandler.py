#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import json
import traceback
from abc import ABC
from decimal import Decimal
from tornado import gen
# import logging
import datetime
import instock.lib.trade_time as trd
import instock.core.singleton_stock_web_module_data as sswmd
import instock.web.base as webBase
import pandas as pd
# 引入数据库工厂
from instock.lib.database_factory import get_database, DatabaseType, db_config

__author__ = 'myh '
__date__ = '2023/3/10 '


class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, bytes):
            return "✅" if ord(obj) == 1 else "❌"
        elif isinstance(obj, Decimal):
            # 处理 Decimal 类型，转换为 float
            return float(obj)
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            # 返回标准的ISO日期格式 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS
            return obj.isoformat()
        elif isinstance(obj, (int, float)) and obj > 1e10:  # 可能是纳秒级时间戳
            try:
                # 尝试转换为datetime
                dt = pd.to_datetime(obj, unit='ns')
                return dt.isoformat()
            except:
                pass
        return json.JSONEncoder.default(self, obj)


# 获得页面数据。
class GetStockHtmlHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        name = self.get_argument("table_name", default=None, strip=False)
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        run_date, run_date_nph = trd.get_trade_date_last()
        if web_module_data.is_realtime:
            date_now_str = run_date_nph.strftime("%Y-%m-%d")
        else:
            date_now_str = run_date.strftime("%Y-%m-%d")
        
        # 将 column_names 序列化为 JSON 字符串，以便在模板中安全使用
        column_names_json = json.dumps(web_module_data.column_names, cls=MyEncoder, ensure_ascii=False)
        self.render("stock_web.html", web_module_data=web_module_data, 
                   column_names_json=column_names_json,
                   date_now=date_now_str,
                   leftMenu=webBase.GetLeftMenu(self.request.uri))


# 获得股票数据内容。
class GetStockDataHandler(webBase.BaseHandler, ABC):
    def get(self):
        name = self.get_argument("name", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        if date is None:
            run_date, run_date_nph = trd.get_trade_date_last()
            date = run_date_nph.strftime("%Y-%m-%d")
        # 分页参数
        page = int(self.get_argument("page", default=1, strip=False))
        page_size = int(self.get_argument("page_size", default=100, strip=False))
        # 搜索参数
        search_keyword = self.get_argument("search", default=None, strip=False)
        # 数据库类型参数（可选）
        db_type_param = self.get_argument("db_type", default=None, strip=False)
        
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        self.set_header('Content-Type', 'application/json;charset=UTF-8')
        
        # 获取数据库实例
        try:
            # 如果请求中指定了数据库类型，临时切换
            original_db_type = db_config.db_type
            if db_type_param and db_type_param.lower() in ['mysql', 'clickhouse']:
                from instock.lib.database_factory import switch_database_type
                switch_database_type(db_type_param.lower())
            
            db = get_database()
            
            # 构建WHERE条件
            where_conditions = []
            params = []
            
            # 日期条件
            if date is not None:
                if db_config.db_type == DatabaseType.CLICKHOUSE:
                    where_conditions.append("toDate(date) = %s")
                else:
                    where_conditions.append("`date` = %s")
                params.append(date)
            
            # 搜索条件
            if search_keyword is not None and search_keyword.strip():
                # 搜索股票代码和名称字段
                search_conditions = []
                search_value = f"%{search_keyword.strip()}%"
                
                # 根据表结构添加搜索字段
                searchable_fields = []
                for col_info in web_module_data.column_names:
                    if isinstance(col_info, dict):
                        field_name = col_info.get('value', '')
                        caption = col_info.get('caption', '').lower()
                        # 搜索包含"代码"、"名称"、"股票"等关键词的字段
                        if any(keyword in caption for keyword in ['代码', '名称', '股票', 'code', 'name']):
                            searchable_fields.append(field_name)
                            
                # 如果没找到特定字段，使用常见的字段名
                if not searchable_fields:
                    common_fields = ['code', 'name', 'stock_code', 'stock_name', '股票代码', '股票名称']
                    for field in common_fields:
                        searchable_fields.append(field)
                
                # 为每个可搜索字段创建LIKE条件
                for field in searchable_fields:
                    if db_config.db_type == DatabaseType.CLICKHOUSE:
                        search_conditions.append(f"{field} LIKE %s")
                    else:
                        search_conditions.append(f"`{field}` LIKE %s")
                    params.append(search_value)
                
                if search_conditions:
                    where_conditions.append(f"({' OR '.join(search_conditions)})")

            # 构建完整的WHERE子句
            where = ""
            if where_conditions:
                where = " WHERE " + " AND ".join(where_conditions)

            # 构建ORDER BY子句
            order_by = ""
            if web_module_data.order_by is not None:
                order_by = f" ORDER BY {web_module_data.order_by}"

            # 构建额外列
            order_columns = ""
            if web_module_data.order_columns is not None and db_config.db_type == DatabaseType.MYSQL:
                # ClickHouse不支持这种复杂的子查询作为选择列，跳过
                order_columns = f",{web_module_data.order_columns}"

            # 先查询总数
            count_sql = f"SELECT COUNT(*) as total FROM {web_module_data.table_name}{where}"
            try:
                if params:
                    total_result = db.query(count_sql, *params)
                else:
                    total_result = db.query(count_sql)
                total = total_result[0]['total'] if total_result else 0
            except Exception as e:
                traceback.print_exc()
                total = 0

            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 分页查询
            if total > 0:
                sql = f"SELECT *{order_columns} FROM {web_module_data.table_name}{where}{order_by} LIMIT %s OFFSET %s"
                print(sql, params + [page_size, offset])
                try:
                    query_params = params + [page_size, offset]
                    data = db.query(sql, *query_params)
                except Exception as e:
                    traceback.print_exc()
                    print(f"数据查询错误: {e}")
                    data = []
            else:
                data = []
            
            # 返回分页数据
            result = {
                'data': data,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total': total,
                    'total_pages': (total + page_size - 1) // page_size if total > 0 else 0
                },
                'meta': {
                    'database_type': db_config.db_type.value,
                    'table_name': web_module_data.table_name
                }
            }

        except Exception as e:
            print(f"数据库操作错误: {e}")
            result = {
                'data': [],
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total': 0,
                    'total_pages': 0
                },
                'error': str(e),
                'meta': {
                    'database_type': db_config.db_type.value,
                    'table_name': web_module_data.table_name if web_module_data else 'unknown'
                }
            }
        
        finally:
            # 恢复原始数据库类型（如果曾经切换过）
            if db_type_param and db_type_param.lower() in ['mysql', 'clickhouse']:
                if original_db_type != db_config.db_type:
                    from instock.lib.database_factory import switch_database_type
                    switch_database_type(original_db_type)
        self.write(json.dumps(result, cls=MyEncoder))
