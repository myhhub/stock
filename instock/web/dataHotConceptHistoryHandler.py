#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import datetime as dt
from abc import ABC
from typing import Any
from urllib.parse import urlencode

from bokeh.embed import components  # type: ignore[reportMissingImports]
from bokeh.models import ColumnDataSource, Div, HoverTool  # type: ignore[reportMissingImports]
from bokeh.plotting import figure  # type: ignore[reportMissingImports]
from tornado import gen  # type: ignore[reportMissingImports]

import instock.core.hot_concept_dashboard as hot_dashboard
import instock.core.singleton_stock_web_module_data as sswmd
import instock.core.tablestructure as tbs
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2026/7/14 '


OVERVIEW_ROUTE = '/instock/hot_concept/history'
HISTORY_TABLE = tbs.TABLE_CN_HOT_CONCEPT_HISTORY['name']
TOP_STOCK_TABLE = tbs.TABLE_CN_HOT_CONCEPT_HISTORY_TOP_STOCK['name']
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
DEFAULT_LOOKBACK_DATES = 5
METRIC_LABELS = {value: label for value, label in METRIC_OPTIONS}
SERIES_COLORS = (
    '#175cd3', '#b42318', '#027a48', '#7a5af8', '#c11574', '#93370d', '#0e7090', '#475467',
    '#1d4ed8', '#15803d', '#9f1239', '#7c3aed',
)


class GetHotConceptHistoryHtmlHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        context = _build_history_context(self)
        self.render('hot_concept_history.html', **context)


def _build_history_context(handler: webBase.BaseHandler) -> dict[str, Any]:
    raw_args = _read_query_args(handler)
    error_message = ''
    default_start_date, default_end_date = _load_default_date_range(handler.db)

    if default_end_date is None:
        return {
            'web_module_data': sswmd.stock_web_module_data().get_data('hot_concept_history_dashboard'),
            'leftMenu': webBase.GetLeftMenu(handler.request.uri),
            'filters': _default_filters(default_start_date, default_end_date),
            'metric_options': METRIC_OPTIONS,
            'concept_type_options': CONCEPT_TYPE_OPTIONS,
            'concepts': [],
            'selected_concept': None,
            'top_stocks': [],
            'summary': {'trade_dates': 0, 'concept_rows': 0, 'config_hashes': 0},
            'error_message': '',
            'has_data': False,
        }

    filters, error_message = _normalize_filters(raw_args, default_start_date, default_end_date)
    concepts = _load_history_concepts(handler.db, filters)
    selected_concept = _resolve_selected_concept(concepts, filters)
    top_stocks = _load_top_stocks(handler.db, selected_concept, filters['top_stocks']) if selected_concept else []
    selected_concept_history = _load_selected_concept_history(handler.db, selected_concept, filters) if selected_concept else []
    _decorate_concepts(concepts, filters)
    _decorate_top_stocks(top_stocks)
    trend_component = _build_topn_trend_component(concepts, filters['metric'])

    return {
        'web_module_data': sswmd.stock_web_module_data().get_data('hot_concept_history_dashboard'),
        'leftMenu': webBase.GetLeftMenu(handler.request.uri),
        'filters': filters,
        'metric_options': METRIC_OPTIONS,
        'concept_type_options': CONCEPT_TYPE_OPTIONS,
        'concepts': concepts,
        'selected_concept': selected_concept,
        'selected_concept_history': selected_concept_history,
        'top_stocks': top_stocks,
        'trend_component': trend_component,
        'summary': _build_summary(concepts),
        'error_message': error_message,
        'has_data': bool(concepts),
    }


def _read_query_args(handler: webBase.BaseHandler) -> dict[str, str | None]:
    return {
        'start_date': handler.get_argument('start_date', default=None, strip=True),
        'end_date': handler.get_argument('end_date', default=None, strip=True),
        'concept_type': handler.get_argument('concept_type', default='ALL', strip=True),
        'metric': handler.get_argument('metric', default='score', strip=True),
        'config_hash': handler.get_argument('config_hash', default=None, strip=True),
        'top_n': handler.get_argument('top_n', default=str(hot_dashboard.DEFAULT_CONCEPT_TOP_N), strip=True),
        'top_stocks': handler.get_argument('top_stocks', default=str(hot_dashboard.DEFAULT_STOCK_TOP_N), strip=True),
        'selected_trade_date': handler.get_argument('selected_trade_date', default=None, strip=True),
        'selected_concept_type': handler.get_argument('selected_concept_type', default=None, strip=True),
        'selected_concept_name': handler.get_argument('selected_concept_name', default=None, strip=True),
        'selected_config_hash': handler.get_argument('selected_config_hash', default=None, strip=True),
    }


def _default_filters(start_date: dt.date | None, end_date: dt.date | None) -> dict[str, Any]:
    return {
        'start_date': _date_text(start_date),
        'end_date': _date_text(end_date),
        'concept_type': 'ALL',
        'metric': 'score',
        'config_hash': '',
        'top_n': hot_dashboard.DEFAULT_CONCEPT_TOP_N,
        'top_stocks': hot_dashboard.DEFAULT_STOCK_TOP_N,
        'selected_trade_date': '',
        'selected_concept_type': '',
        'selected_concept_name': '',
        'selected_config_hash': '',
    }


def _normalize_filters(
    raw_args: dict[str, str | None],
    default_start_date: dt.date | None,
    default_end_date: dt.date | None,
) -> tuple[dict[str, Any], str]:
    error_messages: list[str] = []
    start_date = _parse_date(raw_args.get('start_date'))
    end_date = _parse_date(raw_args.get('end_date'))
    if raw_args.get('start_date') and start_date is None:
        error_messages.append('开始日期格式无效，已回退到默认区间')
    if raw_args.get('end_date') and end_date is None:
        error_messages.append('结束日期格式无效，已回退到默认区间')

    if start_date is None:
        start_date = default_start_date
    if end_date is None:
        end_date = default_end_date
    if start_date is not None and end_date is not None and start_date > end_date:
        start_date, end_date = end_date, start_date
        error_messages.append('开始日期晚于结束日期，已自动交换')

    concept_type = (raw_args.get('concept_type') or 'ALL').upper()
    if concept_type not in hot_dashboard.ALLOWED_CONCEPT_TYPES:
        concept_type = 'ALL'
        error_messages.append('概念类型无效，已回退为全部')

    metric = raw_args.get('metric') or 'score'
    if metric not in hot_dashboard.ALLOWED_METRICS:
        metric = 'score'
        error_messages.append('排序指标无效，已回退为热度分数')

    top_n = _normalize_top_n(raw_args.get('top_n'), hot_dashboard.DEFAULT_CONCEPT_TOP_N)
    top_stocks = _normalize_top_n(raw_args.get('top_stocks'), hot_dashboard.DEFAULT_STOCK_TOP_N)
    config_hash = (raw_args.get('config_hash') or '').strip()

    return {
        'start_date': _date_text(start_date),
        'end_date': _date_text(end_date),
        'concept_type': concept_type,
        'metric': metric,
        'config_hash': config_hash,
        'top_n': top_n,
        'top_stocks': top_stocks,
        'selected_trade_date': (raw_args.get('selected_trade_date') or '').strip(),
        'selected_concept_type': (raw_args.get('selected_concept_type') or '').strip().upper(),
        'selected_concept_name': (raw_args.get('selected_concept_name') or '').strip(),
        'selected_config_hash': (raw_args.get('selected_config_hash') or '').strip(),
    }, '；'.join(error_messages)


def _load_default_date_range(db: Any) -> tuple[dt.date | None, dt.date | None]:
    rows = db.query(
        f'SELECT DISTINCT `trade_date` FROM `{HISTORY_TABLE}` ORDER BY `trade_date` DESC LIMIT %s',
        DEFAULT_LOOKBACK_DATES,
    )
    dates = [coerced for row in rows if (coerced := _coerce_date(row.trade_date)) is not None]
    dates.sort()
    if not dates:
        return None, None
    return dates[0], dates[-1]


def _load_history_concepts(db: Any, filters: dict[str, Any]) -> list[dict[str, Any]]:
    metric = filters['metric']
    params: list[Any] = [filters['start_date'], filters['end_date']]
    where_parts = ['`trade_date` >= %s', '`trade_date` <= %s']
    if filters['concept_type'] != 'ALL':
        where_parts.append('`concept_type` = %s')
        params.append(filters['concept_type'])
    if filters['config_hash']:
        where_parts.append('`config_hash` = %s')
        params.append(filters['config_hash'])
    where_sql = ' AND '.join(where_parts)

    rows = db.query(
        f'SELECT `trade_date`, `captured_at`, `membership_as_of_date`, `concept_type`, `concept_name`, '
        f'`stock_count`, `up_count`, `rise_ratio`, `avg_change_rate`, `weighted_change_rate`, '
        f'`total_deal_amount`, `limit_up_count`, `score`, `config_hash` '
        f'FROM `{HISTORY_TABLE}` '
        f'WHERE {where_sql} '
        f'ORDER BY `trade_date` DESC, `{metric}` DESC, `score` DESC, `total_deal_amount` DESC, `concept_name` ASC',
        *params,
    )

    concepts: list[dict[str, Any]] = []
    date_counts: dict[str, int] = {}
    for row in rows:
        trade_date_text = _date_text(_coerce_date(row.trade_date))
        if not trade_date_text:
            continue
        date_counts.setdefault(trade_date_text, 0)
        if date_counts[trade_date_text] >= filters['top_n']:
            continue
        date_counts[trade_date_text] += 1
        concepts.append(
            {
                'trade_date': trade_date_text,
                'captured_at': _datetime_text(row.captured_at),
                'membership_as_of_date': _date_text(_coerce_date(row.membership_as_of_date)),
                'concept_type': row.concept_type,
                'concept_name': row.concept_name,
                'stock_count': row.stock_count,
                'up_count': row.up_count,
                'rise_ratio': row.rise_ratio,
                'avg_change_rate': row.avg_change_rate,
                'weighted_change_rate': row.weighted_change_rate,
                'total_deal_amount': row.total_deal_amount,
                'limit_up_count': row.limit_up_count,
                'score': row.score,
                'config_hash': row.config_hash,
            }
        )
    return concepts


def _resolve_selected_concept(concepts: list[dict[str, Any]], filters: dict[str, Any]) -> dict[str, Any] | None:
    if not concepts:
        return None

    selected_trade_date = filters['selected_trade_date']
    selected_type = filters['selected_concept_type']
    selected_name = filters['selected_concept_name']
    selected_config_hash = filters['selected_config_hash']

    for concept in concepts:
        if (
            concept['trade_date'] == selected_trade_date
            and concept['concept_type'] == selected_type
            and concept['concept_name'] == selected_name
            and concept['config_hash'] == selected_config_hash
        ):
            return concept
    return concepts[0]


def _load_top_stocks(db: Any, selected_concept: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    rows = db.query(
        f'SELECT `trade_date`, `captured_at`, `membership_as_of_date`, `concept_type`, `concept_name`, `rank`, '
        f'`code`, `name`, `new_price`, `change_rate`, `deal_amount`, `stock_count`, `score`, `config_hash` '
        f'FROM `{TOP_STOCK_TABLE}` '
        f'WHERE `trade_date` = %s AND `concept_type` = %s AND `concept_name` = %s AND `config_hash` = %s '
        f'ORDER BY `rank` ASC LIMIT %s',
        selected_concept['trade_date'],
        selected_concept['concept_type'],
        selected_concept['concept_name'],
        selected_concept['config_hash'],
        limit,
    )
    return [
        {
            'trade_date': _date_text(_coerce_date(row.trade_date)),
            'captured_at': _datetime_text(row.captured_at),
            'membership_as_of_date': _date_text(_coerce_date(row.membership_as_of_date)),
            'concept_type': row.concept_type,
            'concept_name': row.concept_name,
            'rank': row.rank,
            'code': row.code,
            'name': row.name,
            'new_price': row.new_price,
            'change_rate': row.change_rate,
            'deal_amount': row.deal_amount,
            'stock_count': row.stock_count,
            'score': row.score,
            'config_hash': row.config_hash,
        }
        for row in rows
    ]


def _load_selected_concept_history(db: Any, selected_concept: dict[str, Any], filters: dict[str, Any]) -> list[dict[str, Any]]:
    rows = db.query(
        f'SELECT `trade_date`, `concept_type`, `concept_name`, `score`, `avg_change_rate`, `weighted_change_rate`, '
        f'`rise_ratio`, `total_deal_amount`, `limit_up_count`, `stock_count`, `config_hash` '
        f'FROM `{HISTORY_TABLE}` '
        f'WHERE `trade_date` >= %s AND `trade_date` <= %s AND `concept_type` = %s AND `concept_name` = %s AND `config_hash` = %s '
        f'ORDER BY `trade_date` ASC',
        filters['start_date'],
        filters['end_date'],
        selected_concept['concept_type'],
        selected_concept['concept_name'],
        selected_concept['config_hash'],
    )
    return [
        {
            'trade_date': _date_text(_coerce_date(row.trade_date)),
            'concept_type': row.concept_type,
            'concept_name': row.concept_name,
            'score': row.score,
            'avg_change_rate': row.avg_change_rate,
            'weighted_change_rate': row.weighted_change_rate,
            'rise_ratio': row.rise_ratio,
            'total_deal_amount': row.total_deal_amount,
            'limit_up_count': row.limit_up_count,
            'stock_count': row.stock_count,
            'config_hash': row.config_hash,
        }
        for row in rows
    ]


def _decorate_concepts(concepts: list[dict[str, Any]], filters: dict[str, Any]) -> None:
    for concept in concepts:
        concept['display_total_deal_amount'] = _format_amount(concept.get('total_deal_amount'))
        concept['config_hash_short'] = _short_hash(concept.get('config_hash'))
        concept['url'] = _build_selected_url(filters, concept)


def _decorate_top_stocks(top_stocks: list[dict[str, Any]]) -> None:
    for stock in top_stocks:
        stock['display_deal_amount'] = _format_amount(stock.get('deal_amount'))
        stock['indicator_url'] = (
            f"/instock/data/indicators?code={stock.get('code', '')}&date={stock.get('trade_date', '')}&name={stock.get('name', '')}"
        )


def _build_history_trend_component(
    selected_concept: dict[str, Any] | None,
    selected_concept_history: list[dict[str, Any]],
    metric: str,
) -> dict[str, str]:
    metric_label = METRIC_LABELS.get(metric, '热度分数')
    if not selected_concept or not selected_concept_history:
        return _component_from_model(_placeholder_chart('历史趋势', '请选择一个概念后查看趋势'))

    x_values = [row['trade_date'] for row in selected_concept_history if row.get('trade_date')]
    y_values = [_to_float(row.get(metric)) for row in selected_concept_history]
    deal_amount_values = [_to_float(row.get('total_deal_amount')) for row in selected_concept_history]
    source = ColumnDataSource(
        {
            'trade_date': x_values,
            'metric_value': y_values,
            'display_metric_value': [f'{value:.4f}' if value is not None else '' for value in y_values],
            'display_deal_amount': [_format_amount(value) for value in deal_amount_values],
            'stock_count': [row.get('stock_count') for row in selected_concept_history],
        }
    )

    plot = figure(
        title=f"{selected_concept['concept_name']} 历史趋势 - {metric_label}",
        x_range=x_values,
        height=320,
        sizing_mode='stretch_width',
        toolbar_location='above',
        tools='pan,wheel_zoom,box_zoom,reset,save',
    )
    plot.xaxis.axis_label = '交易日'
    plot.yaxis.axis_label = metric_label
    plot.xaxis.major_label_orientation = 0.8
    plot.grid.grid_line_alpha = 0.3
    renderer = plot.line(
        x='trade_date',
        y='metric_value',
        source=source,
        line_width=2.4,
        color='#175cd3',
    )
    scatter_renderer = plot.scatter(x='trade_date', y='metric_value', source=source, size=7, color='#175cd3')
    plot.add_tools(
        HoverTool(
            tooltips=[
                ('交易日', '@trade_date'),
                (metric_label, '@display_metric_value'),
                ('总成交额', '@display_deal_amount'),
                ('成分股数', '@stock_count'),
            ],
            renderers=[renderer, scatter_renderer],
        )
    )
    return _component_from_model(plot)


def _build_topn_trend_component(concepts: list[dict[str, Any]], metric: str) -> dict[str, str]:
    metric_label = METRIC_LABELS.get(metric, '热度分数')
    if not concepts:
        return _component_from_model(_placeholder_chart('每日 TopN 概念走势', '暂无趋势数据'))

    trade_dates = sorted({concept['trade_date'] for concept in concepts if concept.get('trade_date')})
    grouped: dict[str, list[dict[str, Any]]] = {}
    for concept in concepts:
        concept_key = _concept_series_key(concept)
        grouped.setdefault(concept_key, []).append(concept)

    plot = figure(
        title=f'每日 TopN 概念走势 - {metric_label}',
        x_range=trade_dates,
        height=max(340, min(680, 260 + len(grouped) * 14)),
        sizing_mode='stretch_width',
        toolbar_location='above',
        tools='pan,wheel_zoom,box_zoom,reset,save',
    )
    plot.xaxis.axis_label = '交易日'
    plot.yaxis.axis_label = metric_label
    plot.xaxis.major_label_orientation = 0.8
    plot.grid.grid_line_alpha = 0.3

    for index, concept_key in enumerate(sorted(grouped)):
        rows = sorted(grouped[concept_key], key=lambda row: row.get('trade_date', ''))
        source = ColumnDataSource(
            {
                'trade_date': [row.get('trade_date', '') for row in rows],
                'metric_value': [_to_float(row.get(metric)) for row in rows],
                'display_metric_value': [f"{_to_float(row.get(metric)):.4f}" if _to_float(row.get(metric)) is not None else '' for row in rows],
                'concept_label': [_concept_series_label(row, len({item.get('config_hash') for item in concepts if item.get('config_hash')})) for row in rows],
                'display_deal_amount': [_format_amount(row.get('total_deal_amount')) for row in rows],
                'stock_count': [row.get('stock_count') for row in rows],
            }
        )
        color = SERIES_COLORS[index % len(SERIES_COLORS)]
        renderer = plot.line(
            x='trade_date',
            y='metric_value',
            source=source,
            line_width=2.1,
            color=color,
            legend_label=_concept_series_label(rows[0], len({item.get('config_hash') for item in concepts if item.get('config_hash')})),
        )
        scatter_renderer = plot.scatter(x='trade_date', y='metric_value', source=source, size=6, color=color)
        plot.add_tools(
            HoverTool(
                tooltips=[
                    ('交易日', '@trade_date'),
                    ('概念', '@concept_label'),
                    (metric_label, '@display_metric_value'),
                    ('总成交额', '@display_deal_amount'),
                    ('成分股数', '@stock_count'),
                ],
                renderers=[renderer, scatter_renderer],
            )
        )

    plot.legend.click_policy = 'hide'
    plot.legend.location = 'top_left'
    plot.legend.label_text_font_size = '10px'
    return _component_from_model(plot)


def _build_summary(concepts: list[dict[str, Any]]) -> dict[str, int]:
    return {
        'trade_dates': len({concept['trade_date'] for concept in concepts}),
        'concept_rows': len(concepts),
        'config_hashes': len({concept['config_hash'] for concept in concepts if concept.get('config_hash')}),
    }


def _build_selected_url(filters: dict[str, Any], concept: dict[str, Any]) -> str:
    params = {
        'start_date': filters['start_date'],
        'end_date': filters['end_date'],
        'concept_type': filters['concept_type'],
        'metric': filters['metric'],
        'config_hash': filters['config_hash'],
        'top_n': filters['top_n'],
        'top_stocks': filters['top_stocks'],
        'selected_trade_date': concept['trade_date'],
        'selected_concept_type': concept['concept_type'],
        'selected_concept_name': concept['concept_name'],
        'selected_config_hash': concept['config_hash'],
    }
    return f'{OVERVIEW_ROUTE}?{urlencode(params)}'


def _normalize_top_n(value: str | None, default: int) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        return default
    return max(1, min(hot_dashboard.MAX_TOP_N, parsed))


def _parse_date(value: str | None) -> dt.date | None:
    text = (value or '').strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        return None


def _coerce_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError:
        return None


def _date_text(value: dt.date | None) -> str:
    return value.isoformat() if value is not None else ''


def _datetime_text(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return '' if value is None else str(value)


def _format_amount(value: Any) -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return ''
    if amount >= 100000000:
        return f'{amount / 100000000:.2f}亿'
    if amount >= 10000:
        return f'{amount / 10000:.2f}万'
    return f'{amount:.0f}'


def _short_hash(value: Any) -> str:
    text = '' if value is None else str(value)
    return text[:12] if len(text) > 12 else text


def _concept_series_key(concept: dict[str, Any]) -> str:
    return '|'.join(
        [
            str(concept.get('concept_type') or ''),
            str(concept.get('concept_name') or ''),
            str(concept.get('config_hash') or ''),
        ]
    )


def _concept_series_label(concept: dict[str, Any], config_hash_count: int) -> str:
    label = f"{concept.get('concept_type', '')}:{concept.get('concept_name', '')}"
    if config_hash_count > 1 and concept.get('config_hash'):
        label = f"{label} ({_short_hash(concept.get('config_hash'))})"
    return label


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _placeholder_chart(title: str, message: str) -> Div:
    return Div(
        text=(
            f'<div style="background:#f7f8fb;border:1px dashed #d0d5dd;border-radius:8px;'
            f'padding:24px;text-align:center;color:#667085;min-height:120px;">'
            f'<strong>{title}</strong><div style="margin-top:8px;">{message}</div></div>'
        )
    )


def _component_from_model(model: Any) -> dict[str, str]:
    script, div = components(model)
    return {'script': script, 'div': div}
