#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os.path
import sys
from decimal import Decimal
from typing import Any

import pandas as pd  # type: ignore[reportMissingImports]

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.lib.hot_concept_cli import parse_trade_date, positive_int, validate_snapshot_time

__author__ = 'myh '
__date__ = '2026/6/30 '


CONCEPT_ORDER_COLUMNS = ['score', 'total_deal_amount', 'avg_change_rate', 'concept_name']
CONCEPT_IDENTITY_COLUMNS = ['trade_date', 'snapshot_time', 'concept_type', 'concept_name', 'config_hash']
HISTORY_CONCEPT_IDENTITY_COLUMNS = ['trade_date', 'concept_type', 'concept_name', 'config_hash']


class HotConceptQueryError(RuntimeError):
    pass


class NoHotConceptRowsError(HotConceptQueryError):
    pass


def build_query_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Query hot concepts and nested Top20 stocks from stored intraday or historical tables.',
        allow_abbrev=False,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--trade-date', type=parse_trade_date, metavar='YYYY-MM-DD')
    parser.add_argument('--snapshot-time', type=validate_snapshot_time, metavar='HHMM')
    parser.add_argument('--start-date', type=parse_trade_date, metavar='YYYY-MM-DD')
    parser.add_argument('--end-date', type=parse_trade_date, metavar='YYYY-MM-DD')
    parser.add_argument('--top-concepts', type=positive_int, metavar='INT')
    parser.add_argument('--top-stocks', default=20, type=positive_int, metavar='INT')
    parser.add_argument('--format', default='table', choices=('json', 'table'))
    return parser


def _query_mode(args: argparse.Namespace) -> str:
    intraday_values = (args.trade_date, args.snapshot_time)
    history_values = (args.start_date, args.end_date)
    has_intraday = any(value is not None for value in intraday_values)
    has_history = any(value is not None for value in history_values)
    if has_intraday and has_history:
        raise HotConceptQueryError('invalid-query-mode: use either intraday or history arguments, not both')
    if has_intraday:
        if not all(value is not None for value in intraday_values):
            raise HotConceptQueryError('invalid-query-mode: intraday query requires --trade-date and --snapshot-time')
        return 'intraday'
    if has_history:
        if not all(value is not None for value in history_values):
            raise HotConceptQueryError('invalid-query-mode: history query requires --start-date and --end-date')
        if args.start_date > args.end_date:
            raise HotConceptQueryError('invalid-date-range: --start-date must be on or before --end-date')
        return 'history'
    raise HotConceptQueryError('invalid-query-mode: provide intraday or history query arguments')


def _read_table(sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
    try:
        return pd.read_sql(sql=sql, con=mdb.engine(), params=params)
    except Exception as exc:
        raise HotConceptQueryError(f'db-query-failed: {exc}') from exc


def _limit_clause(limit: int | None) -> str:
    if limit is None:
        return ''
    return f' LIMIT {limit}'


def _load_intraday_rows(
    trade_date: dt.date,
    snapshot_time: str,
    top_concepts: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    concept_table = tbs.TABLE_CN_HOT_CONCEPT_SNAPSHOT['name']
    stock_table = tbs.TABLE_CN_HOT_CONCEPT_TOP_STOCK['name']
    concepts = _read_table(
        f'SELECT * FROM `{concept_table}` '
        'WHERE `trade_date` = %s AND `snapshot_time` = %s '
        'ORDER BY `score` DESC, `total_deal_amount` DESC, `avg_change_rate` DESC, `concept_name` ASC'
        f'{_limit_clause(top_concepts)}',
        (trade_date, snapshot_time),
    )
    stocks = _read_table(
        f'SELECT * FROM `{stock_table}` '
        'WHERE `trade_date` = %s AND `snapshot_time` = %s '
        'ORDER BY `concept_type` ASC, `concept_name` ASC, `config_hash` ASC, `rank` ASC',
        (trade_date, snapshot_time),
    )
    return concepts, stocks


def _load_history_rows(
    start_date: dt.date,
    end_date: dt.date,
    top_concepts: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    concept_table = tbs.TABLE_CN_HOT_CONCEPT_HISTORY['name']
    stock_table = tbs.TABLE_CN_HOT_CONCEPT_HISTORY_TOP_STOCK['name']
    concepts = _read_table(
        f'SELECT * FROM `{concept_table}` '
        'WHERE `trade_date` >= %s AND `trade_date` <= %s '
        'ORDER BY `score` DESC, `total_deal_amount` DESC, `avg_change_rate` DESC, `concept_name` ASC'
        f'{_limit_clause(top_concepts)}',
        (start_date, end_date),
    )
    stocks = _read_table(
        f'SELECT * FROM `{stock_table}` '
        'WHERE `trade_date` >= %s AND `trade_date` <= %s '
        'ORDER BY `trade_date` ASC, `concept_type` ASC, `concept_name` ASC, `config_hash` ASC, `rank` ASC',
        (start_date, end_date),
    )
    return concepts, stocks


def _json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if value.time() == dt.time.min:
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    item_method = getattr(value, 'item', None)
    if callable(item_method):
        return _json_value(item_method())
    return value


def _record_from_row(row: pd.Series) -> dict[str, Any]:
    return {column: _json_value(row[column]) for column in row.index}


def _concept_sort_key(record: dict[str, Any]) -> tuple[float, float, float, str]:
    return (
        -float(record.get('score') or 0),
        -float(record.get('total_deal_amount') or 0),
        -float(record.get('avg_change_rate') or 0),
        str(record.get('concept_name') or ''),
    )


def _concept_key(record: dict[str, Any], history: bool) -> tuple[Any, ...]:
    columns = HISTORY_CONCEPT_IDENTITY_COLUMNS if history else CONCEPT_IDENTITY_COLUMNS
    return tuple(record.get(column) for column in columns)


def build_result(concepts: pd.DataFrame, stocks: pd.DataFrame, top_stocks: int, history: bool) -> dict[str, Any]:
    if concepts.empty:
        raise NoHotConceptRowsError('no hot concept rows found')

    stock_records = [_record_from_row(row) for _, row in stocks.iterrows()]
    grouped_stocks: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for stock in stock_records:
        grouped_stocks.setdefault(_concept_key(stock, history), []).append(stock)

    concept_records = [_record_from_row(row) for _, row in concepts.iterrows()]
    concept_records.sort(key=_concept_sort_key)
    for concept in concept_records:
        nested_stocks = grouped_stocks.get(_concept_key(concept, history), [])
        nested_stocks.sort(key=lambda stock: int(stock.get('rank') or 0))
        concept['stocks'] = nested_stocks[:top_stocks]
    return {'concepts': concept_records}


def format_json(result: dict[str, Any]) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2)


def format_table(result: dict[str, Any]) -> str:
    lines: list[str] = []
    for index, concept in enumerate(result['concepts'], start=1):
        snapshot = concept.get('snapshot_time')
        date_label = concept.get('trade_date') if snapshot is None else f"{concept.get('trade_date')} {snapshot}"
        lines.append(
            f"{index}. {date_label} {concept.get('concept_type')} {concept.get('concept_name')} "
            f"score={concept.get('score')} total_deal_amount={concept.get('total_deal_amount')} "
            f"avg_change_rate={concept.get('avg_change_rate')} stock_count={concept.get('stock_count')} "
            f"config_hash={concept.get('config_hash')} membership_as_of_date={concept.get('membership_as_of_date')}"
        )
        for stock in concept['stocks']:
            lines.append(
                f"   #{stock.get('rank')} {stock.get('code')} {stock.get('name')} "
                f"change_rate={stock.get('change_rate')} deal_amount={stock.get('deal_amount')} "
                f"score={stock.get('score')}"
            )
    return '\n'.join(lines)


def run_query(args: argparse.Namespace) -> dict[str, Any]:
    mode = _query_mode(args)
    if mode == 'intraday':
        concepts, stocks = _load_intraday_rows(args.trade_date, args.snapshot_time, args.top_concepts)
        return build_result(concepts, stocks, args.top_stocks, history=False)
    concepts, stocks = _load_history_rows(args.start_date, args.end_date, args.top_concepts)
    return build_result(concepts, stocks, args.top_stocks, history=True)


def main() -> None:
    parser = build_query_parser()
    args = parser.parse_args()
    try:
        result = run_query(args)
        if args.format == 'json':
            print(format_json(result))
        else:
            print(format_table(result))
    except HotConceptQueryError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        logging.exception('hot concept query failed')
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == '__main__':
    main()
