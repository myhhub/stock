#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
from abc import ABC
from typing import Any
from urllib.parse import urlencode

from tornado import gen  # type: ignore[reportMissingImports]

import instock.core.hot_concept_dashboard as dashboard
import instock.core.hot_concept_visualization as visualization
import instock.core.singleton_stock_web_module_data as sswmd
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2026/7/2 '


OVERVIEW_ROUTE = '/instock/hot_concept'
DETAIL_ROUTE = '/instock/hot_concept/detail'

METRIC_OPTIONS = [
    ('score', '热度分数'),
    ('avg_change_rate', '平均涨跌幅'),
    ('weighted_change_rate', '成交额加权涨跌幅'),
    ('rise_ratio', '上涨比例'),
    ('total_deal_amount', '总成交额'),
    ('limit_up_count', '涨停数'),
]

CONCEPT_TYPE_OPTIONS = [
    ('ALL', '全部'),
    ('CONCEPT', '概念'),
    ('STYLE', '风格'),
]

SORT_ORDER_OPTIONS = [
    ('desc', '降序'),
    ('asc', '升序'),
]

CONCEPT_SORT_COLUMNS = {
    'concept_type': {'label': '类型', 'type': 'text'},
    'concept_name': {'label': '名称', 'type': 'text'},
    'snapshot_time': {'label': '快照', 'type': 'text'},
    'score': {'label': '热度分数', 'type': 'number'},
    'avg_change_rate': {'label': '平均涨跌幅', 'type': 'number'},
    'rise_ratio': {'label': '上涨比例', 'type': 'number'},
    'total_deal_amount': {'label': '总成交额', 'type': 'number'},
    'limit_up_count': {'label': '涨停数', 'type': 'number'},
}

STOCK_SORT_COLUMNS = {
    'snapshot_time': {'label': '快照', 'type': 'text'},
    'rank': {'label': '排名', 'type': 'number'},
    'code': {'label': '代码', 'type': 'text'},
    'name': {'label': '名称', 'type': 'text'},
    'new_price': {'label': '最新价', 'type': 'number'},
    'change_rate': {'label': '涨跌幅', 'type': 'number'},
    'deal_amount': {'label': '成交额', 'type': 'number'},
    'score': {'label': '热度分数', 'type': 'number'},
}


class GetHotConceptHtmlHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        context = _build_hot_concept_context(self)
        self.render('hot_concept.html', **context)


class GetHotConceptDetailHtmlHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        context = _build_hot_concept_context(self, require_selected_concept=True)
        context['back_url'] = _overview_url(context['filters'])
        self.render('hot_concept_detail.html', **context)


def _build_hot_concept_context(handler: webBase.BaseHandler, require_selected_concept: bool = False) -> dict[str, Any]:
    raw_args = _read_query_args(handler)
    error_message = ''
    request: dict[str, Any]

    try:
        request = dashboard.validate_dashboard_request(
            trade_date=raw_args['trade_date'],
            config_hash=raw_args['config_hash'],
            metric=raw_args['metric'] or 'score',
            concept_type=raw_args['concept_type'] or 'CONCEPT',
            metric_min=raw_args['metric_min'],
            metric_max=raw_args['metric_max'],
            sort_metric=_dashboard_sort_metric(raw_args),
            sort_order=_dashboard_sort_order(raw_args),
            concept_top_n=raw_args['top_n'],
            stock_top_n=raw_args['top_stocks'],
            selected_concept=raw_args['concept_name'],
            selected_concept_type=_selected_concept_type(raw_args['concept_type'], raw_args['concept_name']),
        )
        view_model = dashboard.build_hot_concept_dashboard(
            trade_date=request['trade_date'],
            config_hash=request['config_hash'],
            metric=request['metric'],
            concept_type=request['concept_type'],
            metric_min=request['metric_min'],
            metric_max=request['metric_max'],
            sort_metric=request['sort_metric'],
            sort_order=request['sort_order'],
            concept_top_n=request['concept_top_n'],
            stock_top_n=request['stock_top_n'],
            selected_concept=request['selected_concept'],
            selected_concept_type=request['selected_concept_type'],
        )
    except dashboard.HotConceptDashboardValidationError as exc:
        error_message = _validation_error_message(exc)
        request = _fallback_request(raw_args)
        view_model = dashboard.build_no_data(request=request)
    except dashboard.HotConceptDashboardError as exc:
        logging.error(f'dataHotConceptHandler.GetHotConceptHtmlHandler处理异常：{exc}')
        error_message = '热门概念数据加载失败，请稍后重试'
        request = _fallback_request(raw_args)
        view_model = dashboard.build_no_data(request=request)

    if require_selected_concept:
        error_message = _apply_detail_validation(raw_args, view_model, error_message)

    components = visualization.build_hot_concept_components(view_model)
    filters = _build_filter_state(raw_args, request, view_model)
    concepts = _sort_rows(
        _concept_rows_with_urls(view_model.get('concepts', []), filters, view_model),
        filters['concept_sort'],
        filters['concept_sort_order'],
        CONCEPT_SORT_COLUMNS,
    )
    top_stocks = _sort_rows(
        _stock_rows_with_urls(view_model.get('latest_top_stocks', view_model.get('top_stocks', [])), view_model),
        filters['stock_sort'],
        filters['stock_sort_order'],
        STOCK_SORT_COLUMNS,
    )

    return {
        'web_module_data': sswmd.stock_web_module_data().get_data('hot_concept_dashboard'),
        'leftMenu': webBase.GetLeftMenu(handler.request.uri),
        'view_model': view_model,
        'filters': filters,
        'metric_options': METRIC_OPTIONS,
        'concept_type_options': CONCEPT_TYPE_OPTIONS,
        'sort_order_options': SORT_ORDER_OPTIONS,
        'range_state': _range_state(view_model),
        'concept_sort_headers': _concept_sort_headers(filters, view_model),
        'stock_sort_headers': _stock_sort_headers(filters),
        'components': components,
        'concepts': concepts,
        'top_stocks': top_stocks,
        'summary_cards': _summary_cards(view_model),
        'selected_concept_metrics': _selected_concept_metrics(view_model),
        'detail_state': _detail_state(filters, view_model),
        'error_message': error_message,
    }


def _apply_detail_validation(
    raw_args: dict[str, str | None], view_model: dict[str, Any], error_message: str
) -> str:
    selected_name = _text(raw_args.get('concept_name'))
    raw_type = (raw_args.get('concept_type') or '').upper()
    selected_type = _selected_concept_type(raw_args.get('concept_type'), selected_name)
    invalid_identity = bool(selected_name and raw_type not in {'CONCEPT', 'STYLE'})
    if selected_name and not invalid_identity and view_model.get('selected_concept_metrics'):
        return error_message

    if not error_message:
        error_message = '未找到选中的热门概念' if selected_name else '请选择一个概念后查看详情'

    view_model['has_data'] = False
    view_model['selected_concept'] = selected_name or None
    view_model['selected_concept_type'] = selected_type
    view_model['selected_concept_key'] = _concept_identity(selected_type, selected_name) or '__missing__'
    view_model['concept_trend_rows'] = []
    view_model['concept_records'] = []
    view_model['concept_trends'] = []
    view_model['concepts'] = []
    view_model['concept_series'] = []
    view_model['heatmap_matrix'] = []
    view_model['selected_concept_metrics'] = None
    view_model['stock_trend_rows'] = []
    view_model['latest_top_stocks'] = []
    view_model['stock_records'] = []
    view_model['stock_trends'] = []
    view_model['top_stocks'] = []
    return error_message


def _read_query_args(handler: webBase.BaseHandler) -> dict[str, str | None]:
    return {
        'trade_date': handler.get_argument('trade_date', default=None, strip=True),
        'metric': handler.get_argument('metric', default='score', strip=True),
        'concept_type': handler.get_argument('concept_type', default='CONCEPT', strip=True),
        'concept_name': handler.get_argument('concept_name', default=None, strip=True),
        'metric_min': handler.get_argument('metric_min', default=None, strip=True),
        'metric_max': handler.get_argument('metric_max', default=None, strip=True),
        'sort_metric': handler.get_argument('sort_metric', default=None, strip=True),
        'sort_order': handler.get_argument('sort_order', default='desc', strip=True),
        'concept_sort': handler.get_argument('concept_sort', default=None, strip=True),
        'concept_sort_order': handler.get_argument('concept_sort_order', default=None, strip=True),
        'stock_sort': handler.get_argument('stock_sort', default=None, strip=True),
        'stock_sort_order': handler.get_argument('stock_sort_order', default=None, strip=True),
        'top_n': handler.get_argument('top_n', default=str(dashboard.DEFAULT_CONCEPT_TOP_N), strip=True),
        'top_stocks': handler.get_argument('top_stocks', default=str(dashboard.DEFAULT_STOCK_TOP_N), strip=True),
        'config_hash': handler.get_argument('config_hash', default=None, strip=True),
    }


def _validation_error_message(exc: dashboard.HotConceptDashboardValidationError) -> str:
    message = str(exc)
    if message.startswith('invalid-metric:'):
        return message.replace('invalid-metric', 'invalid metric', 1)
    return message


def _selected_concept_type(concept_type: str | None, concept_name: str | None) -> str | None:
    value = (concept_type or '').upper()
    if concept_name and value in {'CONCEPT', 'STYLE'}:
        return value
    return None


def _dashboard_sort_metric(raw_args: dict[str, str | None]) -> str:
    concept_sort = raw_args.get('concept_sort')
    if concept_sort in dashboard.ALLOWED_METRICS:
        return concept_sort
    legacy_sort_metric = raw_args.get('sort_metric')
    if legacy_sort_metric in dashboard.ALLOWED_METRICS:
        return legacy_sort_metric
    metric = raw_args.get('metric') or 'score'
    return metric if metric in dashboard.ALLOWED_METRICS else 'score'


def _dashboard_sort_order(raw_args: dict[str, str | None]) -> str:
    if raw_args.get('concept_sort') in dashboard.ALLOWED_METRICS:
        return _normalized_sort_order(raw_args.get('concept_sort_order'), 'desc')
    if raw_args.get('sort_metric') in dashboard.ALLOWED_METRICS:
        return _normalized_sort_order(raw_args.get('sort_order'), 'desc')
    return 'desc'


def _normalized_sort_order(value: str | None, default: str) -> str:
    sort_order = (value or default).lower()
    return sort_order if sort_order in {'asc', 'desc'} else default


def _fallback_request(raw_args: dict[str, str | None]) -> dict[str, Any]:
    metric = raw_args.get('metric') or 'score'
    if metric not in dashboard.ALLOWED_METRICS:
        metric = 'score'
    concept_type = (raw_args.get('concept_type') or 'CONCEPT').upper()
    if concept_type not in dashboard.ALLOWED_CONCEPT_TYPES:
        concept_type = 'CONCEPT'
    sort_metric = raw_args.get('sort_metric') or metric
    if sort_metric not in dashboard.ALLOWED_METRICS:
        sort_metric = metric
    sort_order = (raw_args.get('sort_order') or 'desc').lower()
    if sort_order not in {'asc', 'desc'}:
        sort_order = 'desc'
    return {
        'trade_date': raw_args.get('trade_date'),
        'config_hash': raw_args.get('config_hash'),
        'metric': metric,
        'concept_type': concept_type,
        'metric_min': raw_args.get('metric_min'),
        'metric_max': raw_args.get('metric_max'),
        'sort_metric': sort_metric,
        'sort_order': sort_order,
        'selected_concept': raw_args.get('concept_name'),
        'selected_concept_type': _selected_concept_type(concept_type, raw_args.get('concept_name')),
    }


def _build_filter_state(
    raw_args: dict[str, str | None], request: dict[str, Any], view_model: dict[str, Any]
) -> dict[str, str]:
    metric = _first_text(view_model.get('metric'), request.get('metric'), raw_args.get('metric'), 'score')
    legacy_concept_sort = _first_text(raw_args.get('sort_metric'), metric)
    concept_sort = _normalized_sort_column(
        raw_args.get('concept_sort') or legacy_concept_sort,
        CONCEPT_SORT_COLUMNS,
        'score',
    )
    stock_sort = _normalized_sort_column(raw_args.get('stock_sort'), STOCK_SORT_COLUMNS, 'rank')
    return {
        'trade_date': _first_text(view_model.get('trade_date'), request.get('trade_date'), raw_args.get('trade_date')),
        'metric': metric,
        'concept_type': _first_text(view_model.get('concept_type'), request.get('concept_type'), raw_args.get('concept_type'), 'CONCEPT'),
        'concept_name': _first_text(view_model.get('selected_concept'), request.get('selected_concept'), raw_args.get('concept_name')),
        'metric_min': _first_text(view_model.get('metric_min'), request.get('metric_min'), raw_args.get('metric_min')),
        'metric_max': _first_text(view_model.get('metric_max'), request.get('metric_max'), raw_args.get('metric_max')),
        'sort_metric': _first_text(view_model.get('sort_metric'), request.get('sort_metric'), raw_args.get('sort_metric'), metric),
        'sort_order': _first_text(view_model.get('sort_order'), request.get('sort_order'), raw_args.get('sort_order'), 'desc'),
        'concept_sort': concept_sort,
        'concept_sort_order': _normalized_sort_order(
            raw_args.get('concept_sort_order') or raw_args.get('sort_order'),
            'desc',
        ),
        'stock_sort': stock_sort,
        'stock_sort_order': _normalized_sort_order(raw_args.get('stock_sort_order'), 'asc'),
        'top_n': _first_text(request.get('concept_top_n'), raw_args.get('top_n'), dashboard.DEFAULT_CONCEPT_TOP_N),
        'top_stocks': _first_text(request.get('stock_top_n'), raw_args.get('top_stocks'), dashboard.DEFAULT_STOCK_TOP_N),
        'config_hash': _first_text(view_model.get('config_hash'), request.get('config_hash'), raw_args.get('config_hash')),
    }


def _concept_rows_with_urls(
    concepts: list[dict[str, Any]], filters: dict[str, str], view_model: dict[str, Any]
) -> list[dict[str, Any]]:
    rows = []
    for concept in concepts:
        row = dict(concept)
        row['url'] = _detail_url(filters, {
            'trade_date': _text(view_model.get('trade_date')) or filters.get('trade_date', ''),
            'config_hash': _text(view_model.get('config_hash')) or filters.get('config_hash', ''),
            'concept_type': _text(concept.get('concept_type')),
            'concept_name': _text(concept.get('concept_name')),
        })
        row['concept_key'] = f"{concept.get('concept_type')}:{concept.get('concept_name')}"
        row['display_total_deal_amount'] = _format_chinese_amount(concept.get('total_deal_amount'))
        rows.append(row)
    return rows


def _normalized_sort_column(value: str | None, columns: dict[str, dict[str, str]], default: str) -> str:
    return value if value in columns else default


def _sort_rows(
    rows: list[dict[str, Any]],
    sort_column: str,
    sort_order: str,
    columns: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    if sort_column not in columns:
        return rows
    is_number = columns[sort_column]['type'] == 'number'
    reverse = sort_order == 'desc'

    def has_value(row: dict[str, Any]) -> bool:
        value = row.get(sort_column)
        return value is not None and value != ''

    def sort_key(row: dict[str, Any]) -> tuple[float | str, str]:
        value = row.get(sort_column)
        if is_number:
            return (_number(value), _text(row.get('code') or row.get('concept_name')))
        return (_text(value), _text(row.get('code') or row.get('concept_name')))

    valued_rows = [row for row in rows if has_value(row)]
    empty_rows = [row for row in rows if not has_value(row)]
    return sorted(valued_rows, key=sort_key, reverse=reverse) + empty_rows


def _concept_sort_headers(filters: dict[str, str], view_model: dict[str, Any]) -> list[dict[str, str]]:
    headers = []
    for column, config in CONCEPT_SORT_COLUMNS.items():
        next_order = _next_sort_order(filters['concept_sort'], filters['concept_sort_order'], column)
        url = OVERVIEW_ROUTE + '?' + urlencode(
            _query_from_filters(
                filters,
                include_concept_name=False,
                overrides={
                    'trade_date': _text(view_model.get('trade_date')) or filters.get('trade_date', ''),
                    'config_hash': _text(view_model.get('config_hash')) or filters.get('config_hash', ''),
                    'concept_sort': column,
                    'concept_sort_order': next_order,
                },
            )
        )
        headers.append(_sort_header(config['label'], column, filters['concept_sort'], filters['concept_sort_order'], url))
    return headers


def _stock_sort_headers(filters: dict[str, str]) -> list[dict[str, str]]:
    headers = []
    for column, config in STOCK_SORT_COLUMNS.items():
        next_order = _next_sort_order(filters['stock_sort'], filters['stock_sort_order'], column)
        url = DETAIL_ROUTE + '?' + urlencode(
            _query_from_filters(
                filters,
                include_concept_name=True,
                overrides={'stock_sort': column, 'stock_sort_order': next_order},
            )
        )
        headers.append(_sort_header(config['label'], column, filters['stock_sort'], filters['stock_sort_order'], url))
    return headers


def _next_sort_order(current_column: str, current_order: str, column: str) -> str:
    if current_column == column and current_order == 'desc':
        return 'asc'
    return 'desc'


def _sort_header(label: str, column: str, current_column: str, current_order: str, url: str) -> dict[str, str]:
    active = column == current_column
    return {
        'label': label,
        'url': url,
        'active': '1' if active else '',
        'indicator': '↓' if active and current_order == 'desc' else ('↑' if active else ''),
    }


def _detail_url(filters: dict[str, str], overrides: dict[str, str]) -> str:
    return DETAIL_ROUTE + '?' + urlencode(_query_from_filters(filters, include_concept_name=True, overrides=overrides))


def _overview_url(filters: dict[str, str]) -> str:
    query = _query_from_filters(filters, include_concept_name=False)
    if not query:
        return OVERVIEW_ROUTE
    return OVERVIEW_ROUTE + '?' + urlencode(query)


def _query_from_filters(
    filters: dict[str, str], include_concept_name: bool, overrides: dict[str, str] | None = None
) -> dict[str, str]:
    query: dict[str, str] = {}
    for key in (
        'trade_date',
        'config_hash',
        'concept_type',
        'concept_name',
        'metric',
        'metric_min',
        'metric_max',
        'sort_metric',
        'sort_order',
        'concept_sort',
        'concept_sort_order',
        'stock_sort',
        'stock_sort_order',
        'top_n',
        'top_stocks',
    ):
        if key == 'concept_name' and not include_concept_name:
            continue
        value = _text((overrides or {}).get(key, filters.get(key, '')))
        if value:
            query[key] = value
    return query


def _stock_rows_with_urls(stocks: list[dict[str, Any]], view_model: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    trade_date = _text(view_model.get('trade_date'))
    for stock in stocks:
        row = dict(stock)
        row['display_deal_amount'] = _format_chinese_amount(stock.get('deal_amount'))
        code = _text(stock.get('code'))
        if code:
            query = {
                'code': code,
                'date': trade_date,
                'name': _text(stock.get('name')),
            }
            row['indicator_url'] = '/instock/data/indicators?' + urlencode(query)
        else:
            row['indicator_url'] = ''
        rows.append(row)
    return rows


def _detail_state(filters: dict[str, str], view_model: dict[str, Any]) -> dict[str, str]:
    selected_type = _first_text(view_model.get('selected_concept_type'), filters.get('concept_type'))
    selected_name = _first_text(view_model.get('selected_concept'), filters.get('concept_name'))
    return {
        'metric_label': _metric_label(_first_text(view_model.get('metric'), filters.get('metric'), 'score')),
        'selected_identity': _concept_identity(selected_type, selected_name),
        'selected_type': selected_type,
        'selected_name': selected_name,
    }


def _selected_concept_metrics(view_model: dict[str, Any]) -> dict[str, str] | None:
    metrics = view_model.get('selected_concept_metrics')
    if not metrics:
        return None
    return {
        'concept_type': _text(metrics.get('concept_type')),
        'concept_name': _text(metrics.get('concept_name')),
        'snapshot_time': _text(metrics.get('snapshot_time')),
        'score': _text(metrics.get('score')),
        'avg_change_rate': _text(metrics.get('avg_change_rate')),
        'weighted_change_rate': _text(metrics.get('weighted_change_rate')),
        'rise_ratio': _text(metrics.get('rise_ratio')),
        'total_deal_amount': _format_chinese_amount(metrics.get('total_deal_amount')),
        'limit_up_count': _text(metrics.get('limit_up_count')),
    }


def _summary_cards(view_model: dict[str, Any]) -> list[dict[str, str]]:
    if not view_model.get('has_data'):
        return []
    concepts = view_model.get('concepts') or []
    trend_rows = view_model.get('concept_trend_rows') or []
    snapshot_times = sorted({row.get('snapshot_time') for row in trend_rows if row.get('snapshot_time')})
    hottest = view_model.get('top_metric_concept') or (concepts[0] if concepts else {})
    hottest_label = _text(hottest.get('concept_name'))
    if hottest.get('concept_type'):
        hottest_label = f"{hottest.get('concept_type')} {hottest_label}".strip()
    total_deal_amount = sum(_number(concept.get('total_deal_amount')) for concept in concepts)
    return [
        {'label': '交易日期', 'value': _text(view_model.get('trade_date'))},
        {'label': '快照数', 'value': _text(len(snapshot_times))},
        {'label': '概念数', 'value': _text(len(concepts))},
        {'label': '配置哈希', 'value': _short_hash(view_model.get('config_hash'))},
        {'label': '总成交额', 'value': _format_chinese_amount(total_deal_amount)},
        {'label': '当前热度最高', 'value': hottest_label},
    ]


def _range_state(view_model: dict[str, Any]) -> dict[str, str]:
    metric = _text(view_model.get('metric')) or 'score'
    metric_label = _metric_label(metric)
    return {
        'metric_label': metric_label,
        'current_range_label': f"当前范围：{metric_label} {_range_label(view_model.get('effective_metric_min'), view_model.get('effective_metric_max'))}",
        'available_range_label': f"可用范围：{metric_label} {_range_label(view_model.get('available_metric_min'), view_model.get('available_metric_max'))}",
        'mode_label': '默认前20%' if view_model.get('range_is_default', True) else '用户定义',
        'sort_label': f"排序：{_metric_label(_text(view_model.get('sort_metric')) or metric)} {_sort_order_label(view_model.get('sort_order'))}",
    }


def _metric_label(metric: str) -> str:
    labels = dict(METRIC_OPTIONS)
    return labels.get(metric, metric)


def _range_label(min_value: Any, max_value: Any) -> str:
    min_text = _display_metric_value(min_value)
    max_text = _display_metric_value(max_value)
    if min_text == '暂无' and max_text == '暂无':
        return '暂无'
    return f'{min_text} - {max_text}'


def _display_metric_value(value: Any) -> str:
    if value is None or value == '':
        return '暂无'
    try:
        number = float(value)
    except (TypeError, ValueError):
        return _text(value)
    if number.is_integer():
        return str(int(number))
    return f'{number:.4f}'.rstrip('0').rstrip('.')


def _sort_order_label(value: Any) -> str:
    labels = dict(SORT_ORDER_OPTIONS)
    return labels.get(_text(value), '降序')


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number in (float('inf'), float('-inf')):
        return 0.0
    return number


def _format_chinese_amount(value: Any) -> str:
    number = _number(value)
    if number == 0:
        return '0'
    abs_number = abs(number)
    if abs_number >= 100000000:
        return f'{number / 100000000:.2f}亿'
    if abs_number >= 10000:
        return f'{number / 10000:.2f}万'
    if number.is_integer():
        return str(int(number))
    return f'{number:.2f}'


def _short_hash(value: Any) -> str:
    text = _text(value)
    return text[:12] if len(text) > 12 else text


def _concept_identity(concept_type: Any, concept_name: Any) -> str:
    type_text = _text(concept_type)
    name_text = _text(concept_name)
    if not type_text or not name_text:
        return ''
    return f'{type_text}:{name_text}'


def _first_text(*values: Any) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ''


def _text(value: Any) -> str:
    if value is None:
        return ''
    return str(value)
