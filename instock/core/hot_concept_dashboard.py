#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import datetime as dt
import math
from decimal import Decimal
from typing import Any

import instock.core.tablestructure as tbs
import instock.lib.database as mdb

__author__ = 'myh '
__date__ = '2026/7/2 '


ALLOWED_METRICS = {
    'score',
    'avg_change_rate',
    'weighted_change_rate',
    'rise_ratio',
    'total_deal_amount',
    'limit_up_count',
}
ALLOWED_CONCEPT_TYPES = {'ALL', 'CONCEPT', 'STYLE'}
DEFAULT_CONCEPT_TOP_N = 20
DEFAULT_STOCK_TOP_N = 20
MAX_TOP_N = 50
NO_DATA_MESSAGE = '暂无热门概念数据'

SNAPSHOT_TABLE = tbs.TABLE_CN_HOT_CONCEPT_SNAPSHOT['name']
TOP_STOCK_TABLE = tbs.TABLE_CN_HOT_CONCEPT_TOP_STOCK['name']

CONCEPT_COLUMNS = [
    'trade_date',
    'snapshot_time',
    'captured_at',
    'membership_as_of_date',
    'concept_type',
    'concept_name',
    'stock_count',
    'up_count',
    'rise_ratio',
    'avg_change_rate',
    'weighted_change_rate',
    'total_deal_amount',
    'limit_up_count',
    'score',
    'config_hash',
]
STOCK_COLUMNS = [
    'trade_date',
    'snapshot_time',
    'captured_at',
    'membership_as_of_date',
    'concept_type',
    'concept_name',
    'rank',
    'code',
    'name',
    'new_price',
    'change_rate',
    'deal_amount',
    'stock_count',
    'score',
    'config_hash',
]


class HotConceptDashboardError(RuntimeError):
    pass


class HotConceptDashboardValidationError(HotConceptDashboardError):
    pass


def validate_dashboard_request(
    trade_date: str | dt.date | None = None,
    config_hash: str | None = None,
    metric: str = 'score',
    concept_type: str | None = 'CONCEPT',
    metric_min: int | float | str | None = None,
    metric_max: int | float | str | None = None,
    sort_metric: str | None = None,
    sort_order: str | None = 'desc',
    concept_top_n: int | str | None = DEFAULT_CONCEPT_TOP_N,
    stock_top_n: int | str | None = DEFAULT_STOCK_TOP_N,
    selected_concept: str | None = None,
    selected_concept_type: str | None = None,
) -> dict[str, Any]:
    """Validate dashboard inputs and return normalized values safe for SQL helpers."""
    metric_value = _validate_metric(metric)
    concept_type_value = _validate_concept_type(concept_type)
    metric_min_value, metric_max_value = _validate_metric_range(metric_min, metric_max)
    return {
        'trade_date': _validate_trade_date(trade_date),
        'config_hash': _validate_config_hash(config_hash),
        'metric': metric_value,
        'concept_type': concept_type_value,
        'metric_min': metric_min_value,
        'metric_max': metric_max_value,
        'sort_metric': _validate_sort_metric(sort_metric, metric_value),
        'sort_order': _validate_sort_order(sort_order),
        'concept_top_n': _validate_top_n(concept_top_n, DEFAULT_CONCEPT_TOP_N, 'concept_top_n'),
        'stock_top_n': _validate_top_n(stock_top_n, DEFAULT_STOCK_TOP_N, 'stock_top_n'),
        'selected_concept': _clean_optional_text(selected_concept),
        'selected_concept_type': _validate_selected_concept_type(selected_concept_type),
    }


def build_hot_concept_dashboard(
    trade_date: str | dt.date | None = None,
    config_hash: str | None = None,
    metric: str = 'score',
    concept_type: str | None = 'CONCEPT',
    metric_min: int | float | str | None = None,
    metric_max: int | float | str | None = None,
    sort_metric: str | None = None,
    sort_order: str | None = 'desc',
    concept_top_n: int | str | None = DEFAULT_CONCEPT_TOP_N,
    stock_top_n: int | str | None = DEFAULT_STOCK_TOP_N,
    selected_concept: str | None = None,
    selected_concept_type: str | None = None,
) -> dict[str, Any]:
    """Load and shape one-day intraday hot concept dashboard data.

    Canonical chart keys are `concept_trend_rows`, `concept_series`,
    `heatmap_matrix`, `selected_concept_metrics`, and `stock_trend_rows`.
    Compatibility aliases `concept_records`, `concept_trends`, `stock_records`,
    `stock_trends`, and `top_stocks` are kept for callers; stock trend aliases
    keep all snapshots while `top_stocks` is latest-snapshot-only.
    """
    request = validate_dashboard_request(
        trade_date=trade_date,
        config_hash=config_hash,
        metric=metric,
        concept_type=concept_type,
        metric_min=metric_min,
        metric_max=metric_max,
        sort_metric=sort_metric,
        sort_order=sort_order,
        concept_top_n=concept_top_n,
        stock_top_n=stock_top_n,
        selected_concept=selected_concept,
        selected_concept_type=selected_concept_type,
    )
    selected_date = request['trade_date'] or load_latest_trade_date()
    if selected_date is None:
        return build_no_data(request=request)

    selected_hash = request['config_hash']
    if selected_hash is None:
        selected_hash = load_latest_config_hash(selected_date)
    elif not config_hash_exists(selected_date, selected_hash):
        raise HotConceptDashboardValidationError('invalid-config-hash: config_hash does not exist for trade_date')
    if selected_hash is None:
        return build_no_data(request={**request, 'trade_date': selected_date})

    concept_rows = load_concept_trend_rows(selected_date, selected_hash, request['concept_type'])
    if not concept_rows:
        return build_no_data(request={**request, 'trade_date': selected_date, 'config_hash': selected_hash})

    latest_snapshot_time = max(row['snapshot_time'] for row in concept_rows if row.get('snapshot_time') is not None)
    latest_concepts = [row for row in concept_rows if row.get('snapshot_time') == latest_snapshot_time]
    available_metric_min, available_metric_max = _metric_value_bounds(latest_concepts, request['metric'])
    effective_metric_min, effective_metric_max, range_is_default = _resolve_effective_metric_range(
        latest_concepts,
        request['metric'],
        request['metric_min'],
        request['metric_max'],
        available_metric_min,
        available_metric_max,
    )
    filtered_latest_concepts = _filter_concepts_by_metric_range(
        latest_concepts,
        request['metric'],
        effective_metric_min,
        effective_metric_max,
    )
    metric_ranked_concepts = sorted(
        filtered_latest_concepts,
        key=lambda row: _concept_order_key(row, request['metric'], 'desc'),
    )
    top_metric_concept = metric_ranked_concepts[0] if metric_ranked_concepts else None
    displayed_concepts = metric_ranked_concepts[:request['concept_top_n']]
    displayed_concepts.sort(
        key=lambda row: _concept_order_key(row, request['sort_metric'], request['sort_order'])
    )

    selected_name = request['selected_concept']
    selected_type = request['selected_concept_type']
    if selected_name is None and displayed_concepts:
        selected_name = displayed_concepts[0]['concept_name']
        selected_type = displayed_concepts[0]['concept_type']
    selected_metrics = _find_selected_concept(filtered_latest_concepts, selected_name, selected_type)
    if selected_metrics is not None:
        selected_type = selected_metrics['concept_type']

    stock_rows = []
    if selected_metrics is not None:
        stock_rows = load_selected_concept_stock_rows(
            selected_date,
            selected_hash,
            selected_metrics['concept_type'],
            selected_metrics['concept_name'],
            request['stock_top_n'],
        )
    latest_top_stocks = _latest_stock_rows(stock_rows)

    filtered_concept_rows = _filter_concept_rows_for_display(concept_rows, displayed_concepts)
    concept_series = build_concept_series(filtered_concept_rows, displayed_concepts, request['metric'])
    heatmap_matrix = build_heatmap_matrix(filtered_concept_rows, displayed_concepts, request['metric'])

    return {
        'has_data': True,
        'message': '',
        'trade_date': _json_value(selected_date),
        'config_hash': selected_hash,
        'metric': request['metric'],
        'concept_type': request['concept_type'],
        'metric_min': request['metric_min'],
        'metric_max': request['metric_max'],
        'effective_metric_min': effective_metric_min,
        'effective_metric_max': effective_metric_max,
        'range_is_default': range_is_default,
        'sort_metric': request['sort_metric'],
        'sort_order': request['sort_order'],
        'available_metric_min': available_metric_min,
        'available_metric_max': available_metric_max,
        'latest_snapshot_time': latest_snapshot_time,
        'top_metric_concept': top_metric_concept,
        'selected_concept': selected_name,
        'selected_concept_type': selected_type,
        'selected_concept_key': _concept_identity(selected_type, selected_name),
        'summary_cards': build_summary_cards(filtered_latest_concepts),
        'concept_trend_rows': filtered_concept_rows,
        'concept_series': concept_series,
        'heatmap_matrix': heatmap_matrix,
        'concepts': displayed_concepts,
        'selected_concept_metrics': selected_metrics,
        'stock_trend_rows': stock_rows,
        'latest_top_stocks': latest_top_stocks,
        'concept_records': filtered_concept_rows,
        'concept_trends': filtered_concept_rows,
        'stock_records': stock_rows,
        'stock_trends': stock_rows,
        'top_stocks': latest_top_stocks,
    }


def build_no_data(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the standard empty-state dashboard object."""
    normalized_request = request or {}
    return {
        'has_data': False,
        'message': NO_DATA_MESSAGE,
        'trade_date': _json_value(normalized_request.get('trade_date')),
        'config_hash': normalized_request.get('config_hash'),
        'metric': normalized_request.get('metric', 'score'),
        'concept_type': normalized_request.get('concept_type', 'CONCEPT'),
        'metric_min': normalized_request.get('metric_min'),
        'metric_max': normalized_request.get('metric_max'),
        'effective_metric_min': normalized_request.get('effective_metric_min'),
        'effective_metric_max': normalized_request.get('effective_metric_max'),
        'range_is_default': normalized_request.get('range_is_default', True),
        'sort_metric': normalized_request.get('sort_metric', normalized_request.get('metric', 'score')),
        'sort_order': normalized_request.get('sort_order', 'desc'),
        'available_metric_min': normalized_request.get('available_metric_min'),
        'available_metric_max': normalized_request.get('available_metric_max'),
        'latest_snapshot_time': None,
        'top_metric_concept': None,
        'selected_concept': normalized_request.get('selected_concept'),
        'selected_concept_type': normalized_request.get('selected_concept_type'),
        'selected_concept_key': _concept_identity(
            normalized_request.get('selected_concept_type'), normalized_request.get('selected_concept')
        ),
        'summary_cards': [],
        'concept_trend_rows': [],
        'concept_series': [],
        'heatmap_matrix': [],
        'concepts': [],
        'selected_concept_metrics': None,
        'stock_trend_rows': [],
        'latest_top_stocks': [],
        'concept_records': [],
        'concept_trends': [],
        'stock_records': [],
        'stock_trends': [],
        'top_stocks': [],
    }


def load_latest_trade_date() -> dt.date | None:
    """Return the latest trade_date from hot concept snapshots."""
    rows = _fetch_all(f'SELECT MAX(`trade_date`) FROM `{SNAPSHOT_TABLE}`', ())
    if not rows or rows[0][0] is None:
        return None
    return _date_value(rows[0][0])


def load_latest_config_hash(trade_date: dt.date) -> str | None:
    """Return the latest config hash for a trade date by MAX(captured_at)."""
    rows = _fetch_all(
        f'SELECT `config_hash` FROM `{SNAPSHOT_TABLE}` '
        'WHERE `trade_date` = %s '
        'GROUP BY `config_hash` '
        'ORDER BY MAX(`captured_at`) DESC, `config_hash` ASC '
        'LIMIT 1',
        (trade_date,),
    )
    if not rows:
        return None
    return str(rows[0][0])


def config_hash_exists(trade_date: dt.date, config_hash: str) -> bool:
    """Return True when a config hash exists for the selected trade date."""
    rows = _fetch_all(
        f'SELECT 1 FROM `{SNAPSHOT_TABLE}` WHERE `trade_date` = %s AND `config_hash` = %s LIMIT 1',
        (trade_date, config_hash),
    )
    return bool(rows)


def load_concept_trend_rows(trade_date: dt.date, config_hash: str, concept_type: str = 'ALL') -> list[dict[str, Any]]:
    """Load all intraday concept rows for the selected date/config/type."""
    where_sql = 'WHERE `trade_date` = %s AND `config_hash` = %s'
    params: list[Any] = [trade_date, config_hash]
    if concept_type != 'ALL':
        where_sql += ' AND `concept_type` = %s'
        params.append(concept_type)
    rows = _fetch_all(
        f'SELECT {_select_columns(CONCEPT_COLUMNS)} FROM `{SNAPSHOT_TABLE}` '
        f'{where_sql} '
        'ORDER BY `snapshot_time` ASC, `score` DESC, `total_deal_amount` DESC, '
        '`avg_change_rate` DESC, `concept_name` ASC',
        tuple(params),
    )
    return _rows_to_dicts(rows, CONCEPT_COLUMNS)


def load_selected_concept_stock_rows(
    trade_date: dt.date,
    config_hash: str,
    concept_type: str,
    concept_name: str,
    stock_top_n: int = DEFAULT_STOCK_TOP_N,
) -> list[dict[str, Any]]:
    """Load selected concept TopN stock rows across all intraday snapshots."""
    top_n = _validate_top_n(stock_top_n, DEFAULT_STOCK_TOP_N, 'stock_top_n')
    rows = _fetch_all(
        f'SELECT {_select_columns(STOCK_COLUMNS)} FROM `{TOP_STOCK_TABLE}` '
        'WHERE `trade_date` = %s AND `config_hash` = %s '
        'AND `concept_type` = %s AND `concept_name` = %s AND `rank` <= %s '
        'ORDER BY `snapshot_time` ASC, `rank` ASC, `change_rate` DESC, `deal_amount` DESC, `code` ASC',
        (trade_date, config_hash, concept_type, concept_name, top_n),
    )
    return _rows_to_dicts(rows, STOCK_COLUMNS)


def build_summary_cards(latest_concepts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build top-level summary cards from latest snapshot concepts."""
    if not latest_concepts:
        return []
    total_concepts = len(latest_concepts)
    avg_score = _average(row.get('score') for row in latest_concepts)
    avg_rise_ratio = _average(row.get('rise_ratio') for row in latest_concepts)
    total_deal_amount = sum(_number(row.get('total_deal_amount')) for row in latest_concepts)
    limit_up_count = sum(int(_number(row.get('limit_up_count'))) for row in latest_concepts)
    return [
        {'key': 'concept_count', 'label': '热门概念数', 'value': total_concepts},
        {'key': 'avg_score', 'label': '平均热度分数', 'value': _round_float(avg_score)},
        {'key': 'avg_rise_ratio', 'label': '平均上涨比例', 'value': _round_float(avg_rise_ratio)},
        {'key': 'total_deal_amount', 'label': '概念总成交额', 'value': _round_float(total_deal_amount)},
        {'key': 'limit_up_count', 'label': '涨停家数', 'value': limit_up_count},
    ]


def build_concept_series(
    concept_rows: list[dict[str, Any]],
    displayed_concepts: list[dict[str, Any]],
    metric: str,
) -> list[dict[str, Any]]:
    """Build per-concept intraday series for the selected metric."""
    metric_value = _validate_metric(metric)
    displayed_keys = {(row['concept_type'], row['concept_name']) for row in displayed_concepts}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in concept_rows:
        key = (row['concept_type'], row['concept_name'])
        if key in displayed_keys:
            grouped.setdefault(key, []).append(row)
    series = []
    for concept in displayed_concepts:
        key = (concept['concept_type'], concept['concept_name'])
        points = [
            {'snapshot_time': row['snapshot_time'], 'value': row.get(metric_value)}
            for row in sorted(grouped.get(key, []), key=lambda item: item['snapshot_time'])
        ]
        series.append(
            {
                'concept_type': key[0],
                'concept_name': key[1],
                'concept_key': _concept_identity(key[0], key[1]),
                'concept_label': _concept_label(key[0], key[1]),
                'metric': metric_value,
                'points': points,
            }
        )
    return series


def build_heatmap_matrix(
    concept_rows: list[dict[str, Any]],
    displayed_concepts: list[dict[str, Any]],
    metric: str,
) -> list[dict[str, Any]]:
    """Build concept x snapshot heatmap cells for the selected metric."""
    metric_value = _validate_metric(metric)
    displayed_keys = {(row['concept_type'], row['concept_name']) for row in displayed_concepts}
    cells = []
    for row in concept_rows:
        key = (row['concept_type'], row['concept_name'])
        if key in displayed_keys:
            cells.append(
                {
                    'snapshot_time': row['snapshot_time'],
                    'concept_type': row['concept_type'],
                    'concept_name': row['concept_name'],
                    'concept_key': _concept_identity(row['concept_type'], row['concept_name']),
                    'concept_label': _concept_label(row['concept_type'], row['concept_name']),
                    'metric': metric_value,
                    'value': row.get(metric_value),
                    'score': row.get('score'),
                    'total_deal_amount': row.get('total_deal_amount'),
                }
            )
    return cells


def _validate_metric(metric: str) -> str:
    if metric not in ALLOWED_METRICS:
        raise HotConceptDashboardValidationError(f'invalid-metric: metric must be one of {sorted(ALLOWED_METRICS)}')
    return metric


def _validate_sort_metric(sort_metric: str | None, default_metric: str) -> str:
    if sort_metric is None or sort_metric == '':
        return default_metric
    try:
        return _validate_metric(str(sort_metric))
    except HotConceptDashboardValidationError as exc:
        raise HotConceptDashboardValidationError('invalid metric') from exc


def _validate_sort_order(sort_order: str | None) -> str:
    value = str(sort_order or 'desc').lower()
    if value not in {'asc', 'desc'}:
        raise HotConceptDashboardValidationError('invalid sort order')
    return value


def _validate_metric_range(
    metric_min: int | float | str | None,
    metric_max: int | float | str | None,
) -> tuple[float | None, float | None]:
    min_value = _validate_range_bound(metric_min)
    max_value = _validate_range_bound(metric_max)
    if min_value is not None and max_value is not None and min_value > max_value:
        raise HotConceptDashboardValidationError('invalid range')
    return min_value, max_value


def _validate_range_bound(value: int | float | str | None) -> float | None:
    if value is None or value == '':
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise HotConceptDashboardValidationError('invalid range') from exc
    if not math.isfinite(number):
        raise HotConceptDashboardValidationError('invalid range')
    return number


def _validate_concept_type(concept_type: str | None) -> str:
    value = (concept_type or 'CONCEPT').upper()
    if value not in ALLOWED_CONCEPT_TYPES:
        raise HotConceptDashboardValidationError(
            f'invalid-concept-type: concept_type must be one of {sorted(ALLOWED_CONCEPT_TYPES)}'
        )
    return value


def _validate_selected_concept_type(concept_type: str | None) -> str | None:
    if concept_type is None or concept_type == '':
        return None
    value = str(concept_type).upper()
    if value not in {'CONCEPT', 'STYLE'}:
        raise HotConceptDashboardValidationError("invalid-selected-concept-type: must be 'CONCEPT' or 'STYLE'")
    return value


def _validate_top_n(value: int | str | None, default: int, name: str) -> int:
    if value is None or value == '':
        top_n = default
    else:
        try:
            top_n = int(value)
        except (TypeError, ValueError) as exc:
            raise HotConceptDashboardValidationError(f'invalid-{name}: must be an integer') from exc
    if top_n < 1 or top_n > MAX_TOP_N:
        raise HotConceptDashboardValidationError(f'invalid-{name}: must be between 1 and {MAX_TOP_N}')
    return top_n


def _validate_trade_date(value: str | dt.date | None) -> dt.date | None:
    if value is None or value == '':
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.datetime.strptime(str(value), '%Y-%m-%d').date()
    except ValueError as exc:
        raise HotConceptDashboardValidationError('invalid-trade-date: expected YYYY-MM-DD') from exc


def _validate_config_hash(value: str | None) -> str | None:
    cleaned = _clean_optional_text(value)
    if cleaned is None:
        return None
    if len(cleaned) > 64:
        raise HotConceptDashboardValidationError('invalid-config-hash: length must be <= 64')
    return cleaned


def _clean_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _fetch_all(sql: str, params: tuple[Any, ...]) -> tuple[tuple[Any, ...], ...]:
    try:
        rows = mdb.executeSqlFetch(sql, params)
    except Exception as exc:
        raise HotConceptDashboardError(f'db-query-failed: {exc}') from exc
    return tuple(rows or ())


def _select_columns(columns: list[str]) -> str:
    return ', '.join(f'`{column}`' for column in columns)


def _rows_to_dicts(rows: tuple[tuple[Any, ...], ...], columns: list[str]) -> list[dict[str, Any]]:
    records = [{column: _json_value(value) for column, value in zip(columns, row)} for row in rows]
    if {'concept_type', 'concept_name'}.issubset(columns):
        for record in records:
            concept_key = _concept_identity(record.get('concept_type'), record.get('concept_name'))
            record['concept_key'] = concept_key
            record['concept_label'] = _concept_label(record.get('concept_type'), record.get('concept_name'))
    return records


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=' ')
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    item_method = getattr(value, 'item', None)
    if callable(item_method):
        return _json_value(item_method())
    return value


def _date_value(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.datetime.strptime(str(value), '%Y-%m-%d').date()


def _concept_order_key(row: dict[str, Any], metric: str, sort_order: str = 'desc') -> tuple[float, float, float, str]:
    primary_value = _number(row.get(metric))
    if sort_order != 'asc':
        primary_value = -primary_value
    return (
        primary_value,
        -_number(row.get('score')),
        -_number(row.get('total_deal_amount')),
        str(row.get('concept_name') or ''),
    )


def _metric_value_bounds(latest_concepts: list[dict[str, Any]], metric: str) -> tuple[float | None, float | None]:
    values = _metric_values(latest_concepts, metric)
    if not values:
        return None, None
    return min(values), max(values)


def _resolve_effective_metric_range(
    latest_concepts: list[dict[str, Any]],
    metric: str,
    metric_min: float | None,
    metric_max: float | None,
    available_metric_min: float | None,
    available_metric_max: float | None,
) -> tuple[float | None, float | None, bool]:
    if metric_min is not None or metric_max is not None:
        return (
            metric_min if metric_min is not None else available_metric_min,
            metric_max if metric_max is not None else available_metric_max,
            False,
        )
    values = _metric_values(latest_concepts, metric)
    if not values:
        return None, None, True
    values.sort()
    min_index = max(0, math.ceil(0.8 * len(values)) - 1)
    return values[min_index], values[-1], True


def _filter_concepts_by_metric_range(
    latest_concepts: list[dict[str, Any]],
    metric: str,
    metric_min: float | None,
    metric_max: float | None,
) -> list[dict[str, Any]]:
    filtered = []
    for concept in latest_concepts:
        value = _optional_number(concept.get(metric))
        if value is None:
            continue
        if metric_min is not None and value < metric_min:
            continue
        if metric_max is not None and value > metric_max:
            continue
        filtered.append(concept)
    return filtered


def _filter_concept_rows_for_display(
    concept_rows: list[dict[str, Any]], displayed_concepts: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    displayed_keys = {(row['concept_type'], row['concept_name']) for row in displayed_concepts}
    return [row for row in concept_rows if (row['concept_type'], row['concept_name']) in displayed_keys]


def _latest_stock_rows(stock_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not stock_rows:
        return []
    latest_snapshot_time = max(row['snapshot_time'] for row in stock_rows if row.get('snapshot_time') is not None)
    latest_rows = [row for row in stock_rows if row.get('snapshot_time') == latest_snapshot_time]
    return sorted(latest_rows, key=lambda row: (_number(row.get('rank')), str(row.get('code') or '')))


def _metric_values(rows: list[dict[str, Any]], metric: str) -> list[float]:
    values = []
    for row in rows:
        value = _optional_number(row.get(metric))
        if value is not None:
            values.append(value)
    return values


def _find_selected_concept(
    latest_concepts: list[dict[str, Any]], selected_name: str | None, selected_type: str | None = None
) -> dict[str, Any] | None:
    if selected_name is None:
        return None
    for concept in latest_concepts:
        if selected_type is not None and concept.get('concept_type') != selected_type:
            continue
        if concept.get('concept_name') == selected_name:
            return concept
    return None


def _concept_identity(concept_type: Any, concept_name: Any) -> str | None:
    if concept_type is None or concept_name is None:
        return None
    return f'{concept_type}:{concept_name}'


def _concept_label(concept_type: Any, concept_name: Any) -> str:
    if concept_name is None:
        return ''
    return f'{concept_type} {concept_name}'.strip() if concept_type is not None else str(concept_name)


def _average(values: Any) -> float:
    numbers = [_number(value) for value in values if value is not None]
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number):
        return 0.0
    return number


def _optional_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _round_float(value: float) -> float:
    return round(value, 4)
