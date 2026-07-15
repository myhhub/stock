#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable, Mapping

from bokeh.embed import components  # type: ignore[reportMissingImports]
from bokeh.models import BasicTicker, ColumnDataSource, Div, HoverTool, PrintfTickFormatter  # type: ignore[reportMissingImports]
from bokeh.plotting import figure  # type: ignore[reportMissingImports]
from bokeh.transform import linear_cmap  # type: ignore[reportMissingImports]


COMPONENT_KEYS = (
    "concept_trend_chart",
    "concept_heatmap",
    "selected_concept_chart",
    "stock_trend_chart",
)

METRIC_LABELS = {
    "score": "热度分数",
    "avg_change_rate": "平均涨跌幅",
    "weighted_change_rate": "成交额加权涨跌幅",
    "rise_ratio": "上涨比例",
    "total_deal_amount": "总成交额",
    "limit_up_count": "涨停数",
}

VISUAL_TOKENS = {
    "card_width": 820,
    "trend_height": 320,
    "heatmap_height": 360,
    "compact_height": 300,
    "placeholder_height": 160,
    "line_base_height": 260,
    "line_series_height": 28,
    "heatmap_base_height": 220,
    "heatmap_row_height": 34,
    "line_width": 2.2,
    "border_width": 1,
    "marker_size": 6,
    "heatmap_cell_width": 0.92,
    "heatmap_cell_height": 0.86,
    "legend_alpha": 0.72,
    "grid_alpha": 0.7,
    "title_font_size": "14px",
    "radius": "8px",
    "placeholder_padding": "24px",
    "muted_text": "#5f6876",
    "axis_text": "#344054",
    "grid": "#e6e8ee",
    "background": "#ffffff",
    "panel": "#f7f8fb",
    "positive": "#b42318",
    "neutral": "#d0d5dd",
    "negative": "#027a48",
    "series": ("#b42318", "#175cd3", "#027a48", "#93370d", "#5925dc", "#0e7090", "#c11574", "#475467"),
}

HEATMAP_PALETTE = (
    "#027a48",
    "#469f73",
    "#8bc4a1",
    "#d0d5dd",
    "#f6c8c3",
    "#e7837b",
    "#b42318",
)


def build_hot_concept_components(view_model: Mapping[str, Any] | None) -> dict[str, dict[str, str]]:
    """Build embeddable Bokeh script/div fragments for the hot concept dashboard."""
    normalized = _normalize_view_model(view_model)
    charts = {
        "concept_trend_chart": _build_concept_trend_chart(normalized),
        "concept_heatmap": _build_concept_heatmap(normalized),
        "selected_concept_chart": _build_selected_concept_chart(normalized),
        "stock_trend_chart": _build_stock_trend_chart(normalized),
    }

    fragments = {}
    for key in COMPONENT_KEYS:
        script, div = components(charts[key])
        fragments[key] = {"script": script, "div": div}
    return fragments


def _normalize_view_model(view_model: Mapping[str, Any] | None) -> dict[str, Any]:
    data = view_model or {}
    selected_metric = _safe_text(data.get("selected_metric") or data.get("metric") or "score")
    if selected_metric not in METRIC_LABELS:
        selected_metric = "score"

    concept_records = _concept_records(data)
    snapshot_times = _ordered_values(data.get("snapshot_times"), (row.get("snapshot_time") for row in concept_records))
    concept_keys = _ordered_values(
        data.get("displayed_concept_keys") or data.get("concept_keys"),
        (_concept_key(row) for row in concept_records),
    )
    if not concept_keys:
        concept_keys = _ordered_values(None, (row.get("concept") for row in concept_records))
    concept_labels = [_concept_label_from_records(concept_key, concept_records) for concept_key in concept_keys]

    selected_concept_key = _selected_concept_key(data, concept_keys)
    selected_concept = _concept_label_from_records(selected_concept_key, concept_records)
    stock_records = _stock_records(data, selected_concept)
    stock_snapshot_times = _ordered_values(data.get("snapshot_times"), (row.get("snapshot_time") for row in stock_records))

    return {
        "selected_metric": selected_metric,
        "concept_records": concept_records,
        "snapshot_times": snapshot_times,
        "concept_keys": concept_keys,
        "concept_labels": concept_labels,
        "concept_names": concept_labels,
        "selected_concept": selected_concept,
        "selected_concept_key": selected_concept_key,
        "stock_records": stock_records,
        "stock_snapshot_times": stock_snapshot_times,
    }


def _concept_records(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    for key in ("concept_trend_rows", "concept_trends", "concept_records", "heatmap_matrix", "concepts"):
        records = data.get(key)
        if isinstance(records, Mapping):
            return _series_mapping_records(records, "concept_name")
        if records:
            return [_ensure_concept_key(_normalize_record(record)) for record in _as_iterable(records)]
    return []


def _stock_records(data: Mapping[str, Any], selected_concept: str) -> list[dict[str, Any]]:
    for key in ("stock_trend_rows", "stock_trends", "stock_records", "selected_concept_stock_trends", "stocks"):
        records = data.get(key)
        if isinstance(records, Mapping):
            mapped_records = records.get(selected_concept) or records.get("records") or records.get("data") or records.values()
            return [_normalize_record(record) for record in _as_iterable(mapped_records)]
        if records:
            normalized = [_normalize_record(record) for record in _as_iterable(records)]
            if selected_concept:
                filtered = [row for row in normalized if not row.get("concept_name") or row.get("concept_name") == selected_concept]
                return filtered or normalized
            return normalized
    return []


def _series_mapping_records(series: Mapping[str, Any], name_field: str) -> list[dict[str, Any]]:
    records = []
    for name, rows in series.items():
        if name in ("records", "data"):
            records.extend(_ensure_concept_key(_normalize_record(row)) for row in _as_iterable(rows))
            continue
        for row in _as_iterable(rows):
            record = _normalize_record(row)
            record.setdefault(name_field, _safe_text(name))
            records.append(_ensure_concept_key(record))
    return records


def _selected_concept_key(data: Mapping[str, Any], concept_keys: list[str]) -> str:
    selected_key = _safe_text(data.get("selected_concept_key"))
    if selected_key:
        return selected_key
    selected = data.get("selected_concept")
    selected_type = _safe_text(data.get("selected_concept_type"))
    if isinstance(selected, Mapping):
        selected_type = _safe_text(selected.get("concept_type") or selected.get("type") or selected_type)
        selected = selected.get("concept_name") or selected.get("name")
    selected_name = _safe_text(selected)
    if selected_name:
        if selected_type:
            return _format_concept_key(selected_type, selected_name)
        return _match_concept_key(concept_keys, selected_name)
    return concept_keys[0] if concept_keys else ""


def _build_concept_trend_chart(data: Mapping[str, Any]) -> Any:
    if not data["concept_records"] or not data["snapshot_times"]:
        return _placeholder("概念趋势", "暂无概念趋势数据")

    metric = data["selected_metric"]
    metric_label = METRIC_LABELS[metric]
    plot = _base_figure(
        title=f"概念趋势 - {metric_label}",
        height=_line_chart_height(len(data["concept_keys"]), VISUAL_TOKENS["trend_height"]),
        x_range=data["snapshot_times"],
        y_axis_label=metric_label,
    )

    grouped = _group_by(data["concept_records"], "concept_key")
    labels_by_key = dict(zip(data["concept_keys"], data["concept_labels"]))
    for index, concept_key in enumerate(data["concept_keys"]):
        concept_label = labels_by_key.get(concept_key, concept_key)
        rows = _sorted_rows(grouped.get(concept_key, []), data["snapshot_times"])
        if not rows:
            continue
        source = ColumnDataSource(_concept_source(rows, metric))
        color = _series_color(index)
        renderer = plot.line(
            x="snapshot_time",
            y="metric_value",
            source=source,
            color=color,
            line_width=VISUAL_TOKENS["line_width"],
            legend_label=concept_label,
        )
        plot.scatter(x="snapshot_time", y="metric_value", source=source, color=color, size=VISUAL_TOKENS["marker_size"])
        plot.add_tools(HoverTool(tooltips=_concept_tooltips(metric_label), renderers=[renderer]))

    _finish_line_plot(plot)
    return plot


def _build_concept_heatmap(data: Mapping[str, Any]) -> Any:
    if not data["concept_records"] or not data["snapshot_times"] or not data["concept_labels"]:
        return _placeholder("概念热力", "暂无概念热力数据")

    metric = data["selected_metric"]
    metric_label = METRIC_LABELS[metric]
    rows = [_heatmap_record(row, metric) for row in data["concept_records"]]
    rows = [row for row in rows if row["snapshot_time"] and row["concept_key"]]
    if not rows:
        return _placeholder("概念热力", "暂无概念热力数据")

    values = [row["metric_value"] for row in rows]
    low, high = _value_range(values)
    source = ColumnDataSource({key: [row[key] for row in rows] for key in rows[0]})
    plot = _base_figure(
        title=f"概念热力 - {metric_label}",
        height=_heatmap_height(len(data["concept_labels"])),
        x_range=data["snapshot_times"],
        y_range=list(reversed(data["concept_labels"])),
        y_axis_label="概念",
    )
    plot.grid.grid_line_color = None
    plot.axis.axis_line_color = None
    plot.axis.major_tick_line_color = None

    mapper = linear_cmap("metric_value", HEATMAP_PALETTE, low=low, high=high)
    renderer = plot.rect(
        x="snapshot_time",
        y="concept_label",
        width=VISUAL_TOKENS["heatmap_cell_width"],
        height=VISUAL_TOKENS["heatmap_cell_height"],
        source=source,
        fill_color=mapper,
        line_color=VISUAL_TOKENS["background"],
        line_width=VISUAL_TOKENS["border_width"],
    )
    plot.add_layout(renderer.construct_color_bar(
        ticker=BasicTicker(desired_num_ticks=len(HEATMAP_PALETTE)),
        formatter=PrintfTickFormatter(format="%.2f"),
        label_standoff=8,
        border_line_color=None,
        padding=6,
    ), "right")
    plot.add_tools(HoverTool(tooltips=_concept_tooltips(metric_label), renderers=[renderer]))
    _finish_axis(plot)
    return plot


def _build_selected_concept_chart(data: Mapping[str, Any]) -> Any:
    selected = data["selected_concept"]
    rows = _sorted_rows(
        _group_by(data["concept_records"], "concept_key").get(data["selected_concept_key"], []), data["snapshot_times"]
    )
    if not selected or not rows:
        return _placeholder("选中概念指标", "暂无选中概念数据")

    source = ColumnDataSource(_selected_concept_source(rows))
    plot = _base_figure(
        title=f"选中概念指标 - {selected}",
        height=VISUAL_TOKENS["compact_height"],
        x_range=data["snapshot_times"],
        y_axis_label="指标值",
    )
    score_renderer = plot.line(
        x="snapshot_time",
        y="score",
        source=source,
        color=VISUAL_TOKENS["positive"],
        line_width=VISUAL_TOKENS["line_width"],
        legend_label="热度分数",
    )
    avg_renderer = plot.line(
        x="snapshot_time",
        y="avg_change_rate",
        source=source,
        color=VISUAL_TOKENS["negative"],
        line_width=VISUAL_TOKENS["line_width"],
        legend_label="平均涨跌幅",
    )
    plot.scatter(x="snapshot_time", y="score", source=source, color=VISUAL_TOKENS["positive"], size=VISUAL_TOKENS["marker_size"])
    plot.scatter(x="snapshot_time", y="avg_change_rate", source=source, color=VISUAL_TOKENS["negative"], size=VISUAL_TOKENS["marker_size"])
    plot.add_tools(HoverTool(tooltips=[
        ("概念", "@concept_label"),
        ("标识", "@concept_key"),
        ("时间", "@snapshot_time"),
        ("热度分数", "@score{0.00}"),
        ("平均涨跌幅", "@avg_change_rate{0.00}"),
        ("总成交额", "@total_deal_amount{0,0}"),
    ], renderers=[score_renderer, avg_renderer]))
    _finish_line_plot(plot)
    return plot


def _build_stock_trend_chart(data: Mapping[str, Any]) -> Any:
    if not data["stock_records"] or not data["stock_snapshot_times"]:
        return _placeholder("概念内股票趋势", "暂无概念内股票数据")

    grouped = _group_stocks(data["stock_records"])
    plot = _base_figure(
        title="概念内 TopN 股票趋势",
        height=_line_chart_height(len(grouped), VISUAL_TOKENS["compact_height"]),
        x_range=data["stock_snapshot_times"],
        y_axis_label="涨跌幅",
    )
    for index, (stock_key, rows) in enumerate(grouped.items()):
        sorted_rows = _sorted_rows(rows, data["stock_snapshot_times"])
        if not sorted_rows:
            continue
        source = ColumnDataSource(_stock_source(sorted_rows))
        color = _series_color(index)
        renderer = plot.line(
            x="snapshot_time",
            y="change_rate",
            source=source,
            color=color,
            line_width=VISUAL_TOKENS["line_width"],
            legend_label=stock_key,
        )
        plot.scatter(x="snapshot_time", y="change_rate", source=source, color=color, size=VISUAL_TOKENS["marker_size"])
        plot.add_tools(HoverTool(tooltips=[
            ("排名", "@rank"),
            ("代码", "@code"),
            ("名称", "@name"),
            ("时间", "@snapshot_time"),
            ("涨跌幅", "@change_rate{0.00}"),
            ("成交额", "@deal_amount{0,0}"),
        ], renderers=[renderer]))

    _finish_line_plot(plot)
    return plot


def _base_figure(title: str, height: int, x_range: list[str], y_axis_label: str = "", y_range: list[str] | None = None) -> Any:
    figure_options = {
        "title": title,
        "width": VISUAL_TOKENS["card_width"],
        "height": height,
        "sizing_mode": "stretch_width",
        "x_range": x_range,
        "tools": "pan,wheel_zoom,box_zoom,reset,save",
        "toolbar_location": "above",
    }
    if y_range is not None:
        figure_options["y_range"] = y_range
    plot = figure(**figure_options)
    plot.background_fill_color = VISUAL_TOKENS["background"]
    plot.border_fill_color = VISUAL_TOKENS["background"]
    plot.outline_line_color = VISUAL_TOKENS["grid"]
    plot.xaxis.axis_label = "快照时间"
    plot.yaxis.axis_label = y_axis_label
    plot.xaxis.major_label_orientation = math.pi / 4
    _finish_axis(plot)
    return plot


def _line_chart_height(series_count: int, minimum_height: int) -> int:
    dynamic_height = VISUAL_TOKENS["line_base_height"] + max(1, series_count) * VISUAL_TOKENS["line_series_height"]
    return max(minimum_height, dynamic_height)


def _heatmap_height(row_count: int) -> int:
    dynamic_height = VISUAL_TOKENS["heatmap_base_height"] + max(1, row_count) * VISUAL_TOKENS["heatmap_row_height"]
    return max(VISUAL_TOKENS["heatmap_height"], dynamic_height)


def _finish_axis(plot: Any) -> None:
    plot.title.text_color = VISUAL_TOKENS["axis_text"]
    plot.title.text_font_size = VISUAL_TOKENS["title_font_size"]
    plot.axis.major_label_text_color = VISUAL_TOKENS["axis_text"]
    plot.axis.axis_label_text_color = VISUAL_TOKENS["muted_text"]
    plot.grid.grid_line_color = VISUAL_TOKENS["grid"]
    plot.grid.grid_line_alpha = VISUAL_TOKENS["grid_alpha"]


def _finish_line_plot(plot: Any) -> None:
    _finish_axis(plot)
    plot.legend.location = "top_left"
    plot.legend.click_policy = "hide"
    plot.legend.background_fill_alpha = VISUAL_TOKENS["legend_alpha"]
    plot.legend.border_line_color = None


def _placeholder(title: str, message: str) -> Div:
    return Div(
        text=f"<section><h4>{title}</h4><p>{message}</p></section>",
        width=VISUAL_TOKENS["card_width"],
        height=VISUAL_TOKENS["placeholder_height"],
        sizing_mode="stretch_width",
        styles={
            "background": VISUAL_TOKENS["panel"],
            "border": f"{VISUAL_TOKENS['border_width']}px solid {VISUAL_TOKENS['grid']}",
            "border-radius": VISUAL_TOKENS["radius"],
            "color": VISUAL_TOKENS["muted_text"],
            "padding": VISUAL_TOKENS["placeholder_padding"],
        },
    )


def _concept_source(rows: list[dict[str, Any]], metric: str) -> dict[str, list[Any]]:
    return {
        "concept_key": [_safe_text(row.get("concept_key") or _concept_key(row)) for row in rows],
        "concept_label": [_concept_label(row) for row in rows],
        "concept_type": [_safe_text(row.get("concept_type")) for row in rows],
        "concept_name": [_safe_text(row.get("concept_name") or row.get("concept")) for row in rows],
        "snapshot_time": [_safe_text(row.get("snapshot_time")) for row in rows],
        "metric_value": [_safe_number(row.get(metric)) for row in rows],
        "score": [_safe_number(row.get("score")) for row in rows],
        "avg_change_rate": [_safe_number(row.get("avg_change_rate")) for row in rows],
        "weighted_change_rate": [_safe_number(row.get("weighted_change_rate")) for row in rows],
        "rise_ratio": [_safe_number(row.get("rise_ratio")) for row in rows],
        "total_deal_amount": [_safe_number(row.get("total_deal_amount")) for row in rows],
        "limit_up_count": [_safe_number(row.get("limit_up_count")) for row in rows],
    }


def _heatmap_record(row: Mapping[str, Any], metric: str) -> dict[str, Any]:
    record = _concept_source([dict(row)], metric)
    return {key: values[0] for key, values in record.items()}


def _selected_concept_source(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    return {
        "concept_key": [_safe_text(row.get("concept_key") or _concept_key(row)) for row in rows],
        "concept_label": [_concept_label(row) for row in rows],
        "concept_type": [_safe_text(row.get("concept_type")) for row in rows],
        "concept_name": [_safe_text(row.get("concept_name") or row.get("concept")) for row in rows],
        "snapshot_time": [_safe_text(row.get("snapshot_time")) for row in rows],
        "score": [_safe_number(row.get("score")) for row in rows],
        "avg_change_rate": [_safe_number(row.get("avg_change_rate")) for row in rows],
        "total_deal_amount": [_safe_number(row.get("total_deal_amount")) for row in rows],
    }


def _stock_source(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    return {
        "snapshot_time": [_safe_text(row.get("snapshot_time")) for row in rows],
        "rank": [_safe_text(row.get("rank")) for row in rows],
        "code": [_safe_text(row.get("code") or row.get("stock_code")) for row in rows],
        "name": [_safe_text(row.get("name") or row.get("stock_name")) for row in rows],
        "change_rate": [_safe_number(row.get("change_rate")) for row in rows],
        "deal_amount": [_safe_number(row.get("deal_amount")) for row in rows],
    }


def _concept_tooltips(metric_label: str) -> list[tuple[str, str]]:
    return [
        ("概念", "@concept_label"),
        ("标识", "@concept_key"),
        ("时间", "@snapshot_time"),
        (metric_label, "@metric_value{0.00}"),
        ("热度分数", "@score{0.00}"),
        ("平均涨跌幅", "@avg_change_rate{0.00}"),
        ("成交额加权涨跌幅", "@weighted_change_rate{0.00}"),
        ("上涨比例", "@rise_ratio{0.00}"),
        ("总成交额", "@total_deal_amount{0,0}"),
        ("涨停数", "@limit_up_count{0}"),
    ]


def _group_by(records: Iterable[Mapping[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped = defaultdict(list)
    for row in records:
        name = _safe_text(row.get(key) or row.get("concept"))
        if name:
            grouped[name].append(dict(row))
    return dict(grouped)


def _group_stocks(records: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = defaultdict(list)
    for row in records:
        code = _safe_text(row.get("code") or row.get("stock_code"))
        name = _safe_text(row.get("name") or row.get("stock_name"))
        label = f"{code} {name}".strip()
        if label:
            grouped[label].append(dict(row))
    return dict(grouped)


def _sorted_rows(rows: Iterable[Mapping[str, Any]], snapshot_order: list[str]) -> list[dict[str, Any]]:
    order = {snapshot_time: index for index, snapshot_time in enumerate(snapshot_order)}
    return sorted((dict(row) for row in rows), key=lambda row: order.get(_safe_text(row.get("snapshot_time")), len(order)))


def _ordered_values(preferred: Any, fallback: Iterable[Any]) -> list[str]:
    values = [_safe_text(value) for value in _as_iterable(preferred)] if preferred else []
    if not values:
        values = [_safe_text(value) for value in fallback]
    ordered = []
    seen = set()
    for value in values:
        if value and value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered


def _value_range(values: list[float]) -> tuple[float, float]:
    low = min(values)
    high = max(values)
    if low == high:
        return low - 1, high + 1
    return low, high


def _series_color(index: int) -> str:
    colors = VISUAL_TOKENS["series"]
    return colors[index % len(colors)]


def _normalize_record(record: Any) -> dict[str, Any]:
    if isinstance(record, Mapping):
        return dict(record)
    if hasattr(record, "_asdict"):
        return dict(record._asdict())
    if hasattr(record, "to_dict"):
        return dict(record.to_dict())
    if hasattr(record, "__dict__"):
        return dict(record.__dict__)
    return {}


def _ensure_concept_key(record: dict[str, Any]) -> dict[str, Any]:
    record.setdefault("concept_key", _concept_key(record))
    record.setdefault("concept_label", _concept_label(record))
    return record


def _concept_key(row: Mapping[str, Any]) -> str:
    existing = _safe_text(row.get("concept_key"))
    if existing:
        return existing
    concept_name = _safe_text(row.get("concept_name") or row.get("concept"))
    concept_type = _safe_text(row.get("concept_type"))
    return _format_concept_key(concept_type, concept_name) if concept_name else ""


def _format_concept_key(concept_type: str, concept_name: str) -> str:
    return f"{concept_type}:{concept_name}" if concept_type else concept_name


def _concept_label(row: Mapping[str, Any]) -> str:
    existing = _safe_text(row.get("concept_label"))
    if existing:
        return existing
    concept_name = _safe_text(row.get("concept_name") or row.get("concept"))
    concept_type = _safe_text(row.get("concept_type"))
    return f"{concept_type} {concept_name}".strip() if concept_name else ""


def _concept_label_from_records(concept_key: str, records: Iterable[Mapping[str, Any]]) -> str:
    for record in records:
        if _concept_key(record) == concept_key:
            return _concept_label(record)
    return concept_key.replace(":", " ", 1)


def _concept_name_from_key(concept_key: str) -> str:
    return concept_key.split(":", 1)[1] if ":" in concept_key else concept_key


def _match_concept_key(concept_keys: list[str], selected_name: str) -> str:
    for concept_key in concept_keys:
        if _concept_name_from_key(concept_key) == selected_name:
            return concept_key
    return selected_name


def _as_iterable(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, (str, bytes)):
        return [value]
    try:
        return list(value)
    except TypeError:
        return [value]


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _safe_number(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return 0.0
        return number
    except (TypeError, ValueError):
        return 0.0
