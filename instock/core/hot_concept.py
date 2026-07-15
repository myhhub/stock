"""Core utilities for hot concept analytics.

This module is intentionally pure DataFrame logic. Jobs provide source rows and
handle persistence; these helpers validate configuration, normalize membership,
score concepts/styles, and return rows ready for downstream writes.
"""

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[reportMissingImports]


CONCEPT_TYPE = "CONCEPT"
STYLE_TYPE = "STYLE"

KNOWN_SCORE_METRICS = {
    "weighted_change_rate_percentile",
    "avg_change_rate_percentile",
    "rise_ratio",
    "deal_amount_percentile",
    "limit_up_count_percentile",
}

DEFAULT_LIMIT_UP_RATE = 9.5
WEIGHT_SUM_TOLERANCE = 1e-9


class HotConceptConfigError(ValueError):
    """Raised when hot concept scoring configuration is invalid."""


def load_score_config(path: str | Path) -> dict[str, Any]:
    """Load and validate hot concept scoring configuration from JSON."""
    with open(path, "r", encoding="utf-8") as config_file:
        return validate_score_config(json.load(config_file))


def validate_score_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate and return a normalized scoring configuration."""
    if not isinstance(config, dict):
        raise HotConceptConfigError("config must be a JSON object")

    weights = config.get("weights")
    if not isinstance(weights, dict) or not weights:
        raise HotConceptConfigError("weights must be a non-empty object")

    unknown_metrics = sorted(set(weights) - KNOWN_SCORE_METRICS)
    if unknown_metrics:
        raise HotConceptConfigError(f"weights contain unknown metrics: {unknown_metrics}")

    missing_metrics = sorted(KNOWN_SCORE_METRICS - set(weights))
    if missing_metrics:
        raise HotConceptConfigError(f"weights are missing metrics: {missing_metrics}")

    normalized_weights: dict[str, float] = {}
    for metric, weight in weights.items():
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(weight):
            raise HotConceptConfigError(f"weights.{metric} must be a finite number")
        normalized_weights[metric] = float(weight)

    weight_sum = sum(normalized_weights.values())
    if abs(weight_sum - 1.0) > WEIGHT_SUM_TOLERANCE:
        raise HotConceptConfigError(f"weights must sum to 1.0, got {weight_sum}")

    min_stock_count = config.get("min_stock_count")
    if isinstance(min_stock_count, bool) or not isinstance(min_stock_count, int) or min_stock_count <= 0:
        raise HotConceptConfigError("min_stock_count must be a positive integer")

    normalized_config = dict(config)
    normalized_config["min_stock_count"] = int(min_stock_count)
    normalized_config["weights"] = {metric: normalized_weights[metric] for metric in sorted(normalized_weights)}
    return normalized_config


def config_hash(config: dict[str, Any]) -> str:
    """Return a stable SHA-256 hash for normalized scoring configuration."""
    normalized_config = validate_score_config(config)
    config_json = json.dumps(normalized_config, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(config_json.encode("utf-8")).hexdigest()


def config_json(config: dict[str, Any]) -> str:
    """Return stable normalized config JSON for storing with aggregate rows."""
    return json.dumps(validate_score_config(config), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def normalize_membership(
    df: pd.DataFrame,
    trade_date: Any,
    snapshot_time: str | None = None,
    captured_at: Any | None = None,
    membership_as_of_date: Any | None = None,
) -> pd.DataFrame:
    """Split comma-separated concept/style columns into normalized membership rows."""
    rows: list[dict[str, Any]] = []
    membership_date = membership_as_of_date if membership_as_of_date is not None else trade_date
    if df is None or df.empty:
        return _membership_frame(rows)

    for _, source_row in df.iterrows():
        code = _normalize_code(source_row.get("code"))
        if code is None:
            continue
        name = _clean_text(source_row.get("name"))
        for concept_type, column_name in ((CONCEPT_TYPE, "concept"), (STYLE_TYPE, "style")):
            for concept_name in _split_members(source_row.get(column_name)):
                rows.append(
                    {
                        "trade_date": trade_date,
                        "snapshot_time": snapshot_time,
                        "captured_at": captured_at,
                        "membership_as_of_date": membership_date,
                        "concept_type": concept_type,
                        "concept_name": concept_name,
                        "code": code,
                        "name": name,
                    }
                )

    membership = _membership_frame(rows)
    if membership.empty:
        return membership
    return membership.drop_duplicates(["trade_date", "snapshot_time", "concept_type", "concept_name", "code"]).reset_index(drop=True)


def prepare_stock_snapshot(
    df: pd.DataFrame,
    trade_date: Any,
    snapshot_time: str | None = None,
    captured_at: Any | None = None,
    config_hash: str | None = None,
) -> pd.DataFrame:
    """Normalize stock rows and filter missing/suspended price or change data."""
    rows: list[dict[str, Any]] = []
    if df is None or df.empty:
        return _stock_frame(rows)

    for _, source_row in df.iterrows():
        code = _normalize_code(source_row.get("code"))
        new_price = _to_number(source_row.get("new_price"))
        change_rate = _to_number(source_row.get("change_rate"))
        deal_amount = _to_number(source_row.get("deal_amount"))
        if code is None or new_price is None or new_price <= 0 or change_rate is None:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "snapshot_time": snapshot_time,
                "captured_at": captured_at,
                "code": code,
                "name": _clean_text(source_row.get("name")),
                "new_price": float(new_price),
                "change_rate": float(change_rate),
                "deal_amount": float(deal_amount) if deal_amount is not None else None,
                "config_hash": config_hash,
            }
        )

    stock_snapshot = _stock_frame(rows)
    if stock_snapshot.empty:
        return stock_snapshot
    return stock_snapshot.drop_duplicates("code", keep="last").reset_index(drop=True)


def aggregate_concepts(
    stock_df: pd.DataFrame,
    membership_df: pd.DataFrame,
    config: dict[str, Any],
    top_n: int = 20,
    snapshot_time: str | None = None,
    captured_at: Any | None = None,
    membership_as_of_date: Any | None = None,
    limit_up_rate: float = DEFAULT_LIMIT_UP_RATE,
) -> dict[str, Any]:
    """Aggregate and score concept/style hotness with deterministic TopN stocks."""
    normalized_config = validate_score_config(config)
    if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n <= 0:
        raise ValueError("top_n must be a positive integer")

    diagnostics = _base_diagnostics(stock_df, membership_df, normalized_config)
    if stock_df is None or stock_df.empty or membership_df is None or membership_df.empty:
        diagnostics["valid_concept_count"] = 0
        diagnostics["excluded_concepts"] = []
        return {"concepts": _concept_frame([]), "top_stocks": _top_stock_frame([]), "diagnostics": diagnostics}

    stocks = stock_df.copy()
    memberships = membership_df.copy()
    stocks["code"] = stocks["code"].map(_normalize_code)
    memberships["code"] = memberships["code"].map(_normalize_code)
    stocks = stocks.dropna(subset=["code", "new_price", "change_rate"])
    stocks = stocks.loc[pd.to_numeric(stocks["new_price"], errors="coerce") > 0].copy()
    memberships = memberships.dropna(subset=["code", "concept_type", "concept_name"])
    memberships["concept_name"] = memberships["concept_name"].map(_clean_text)
    memberships = memberships.loc[memberships["concept_name"].notna()].copy()

    joined = memberships.merge(stocks, on="code", how="inner", suffixes=("_membership", "_stock"))
    diagnostics["joined_membership_stock_count"] = int(len(joined))
    if joined.empty:
        diagnostics["valid_concept_count"] = 0
        diagnostics["excluded_concepts"] = []
        return {"concepts": _concept_frame([]), "top_stocks": _top_stock_frame([]), "diagnostics": diagnostics}

    joined["deal_amount"] = pd.to_numeric(joined["deal_amount"], errors="coerce")
    joined["change_rate"] = pd.to_numeric(joined["change_rate"], errors="coerce")
    joined["new_price"] = pd.to_numeric(joined["new_price"], errors="coerce")
    joined["deal_amount_for_calc"] = joined["deal_amount"].fillna(0)
    joined["up_flag"] = joined["change_rate"] > 0
    joined["limit_up_flag"] = joined["change_rate"] >= limit_up_rate
    joined["weighted_change_component"] = joined["change_rate"] * joined["deal_amount_for_calc"]

    group_columns = ["concept_type", "concept_name"]
    aggregate = joined.groupby(group_columns, as_index=False).agg(
        stock_count=("code", "nunique"),
        up_count=("up_flag", "sum"),
        avg_change_rate=("change_rate", "mean"),
        total_deal_amount=("deal_amount_for_calc", "sum"),
        weighted_change_sum=("weighted_change_component", "sum"),
        limit_up_count=("limit_up_flag", "sum"),
    )
    aggregate["rise_ratio"] = aggregate["up_count"] / aggregate["stock_count"]
    aggregate["weighted_change_rate"] = aggregate.apply(_weighted_change_rate, axis=1)

    min_stock_count = normalized_config["min_stock_count"]
    excluded = aggregate.loc[aggregate["stock_count"] < min_stock_count].copy()
    eligible = aggregate.loc[aggregate["stock_count"] >= min_stock_count].copy()
    diagnostics["excluded_concepts"] = excluded[["concept_type", "concept_name", "stock_count"]].to_dict("records")
    diagnostics["excluded_concept_count"] = int(len(excluded))
    diagnostics["valid_concept_count"] = int(len(eligible))

    if eligible.empty:
        return {"concepts": _concept_frame([]), "top_stocks": _top_stock_frame([]), "diagnostics": diagnostics}

    for value_column, percentile_column in (
        ("weighted_change_rate", "weighted_change_rate_percentile"),
        ("avg_change_rate", "avg_change_rate_percentile"),
        ("total_deal_amount", "deal_amount_percentile"),
        ("limit_up_count", "limit_up_count_percentile"),
    ):
        eligible[percentile_column] = eligible[value_column].rank(method="min", pct=True)

    weights = normalized_config["weights"]
    score = pd.Series(0.0, index=eligible.index)
    for metric, weight in weights.items():
        score = score + eligible[metric] * weight
    eligible["score"] = score.round(12)
    digest = config_hash(normalized_config)
    stable_config_json = config_json(normalized_config)
    trade_date = _first_present(joined, "trade_date_membership", "trade_date_stock", "trade_date")
    resolved_snapshot_time = snapshot_time if snapshot_time is not None else _first_present(joined, "snapshot_time_membership", "snapshot_time_stock", "snapshot_time")
    resolved_captured_at = captured_at if captured_at is not None else _first_present(joined, "captured_at_membership", "captured_at_stock", "captured_at")
    resolved_membership_date = membership_as_of_date if membership_as_of_date is not None else _first_present(joined, "membership_as_of_date")

    for metric, weight in weights.items():
        eligible[f"weight_{metric}"] = weight
    eligible["trade_date"] = trade_date
    eligible["snapshot_time"] = resolved_snapshot_time
    eligible["captured_at"] = resolved_captured_at
    eligible["membership_as_of_date"] = resolved_membership_date
    eligible["config_hash"] = digest
    eligible["config_json"] = stable_config_json

    concepts = _sort_concepts(eligible.drop(columns=["weighted_change_sum"]))
    top_stocks = _build_top_stocks(joined, concepts, top_n, digest)
    diagnostics["top_stock_count"] = int(len(top_stocks))
    return {"concepts": _concept_frame(concepts.to_dict("records")), "top_stocks": top_stocks, "diagnostics": diagnostics}


def _build_top_stocks(joined: pd.DataFrame, concepts: pd.DataFrame, top_n: int, digest: str) -> pd.DataFrame:
    concept_identity = concepts[["concept_type", "concept_name", "stock_count", "score"]]
    ranked_source = joined.merge(concept_identity, on=["concept_type", "concept_name"], how="inner")
    ranked_source = ranked_source.sort_values(
        ["concept_type", "concept_name", "change_rate", "deal_amount", "code"],
        ascending=[True, True, False, False, True],
        na_position="last",
        kind="mergesort",
    )
    ranked_source["rank"] = ranked_source.groupby(["concept_type", "concept_name"]).cumcount() + 1
    ranked_source = ranked_source.loc[ranked_source["rank"] <= top_n].copy()
    ranked_source["config_hash"] = digest
    if "name_stock" in ranked_source.columns:
        ranked_source["name"] = ranked_source["name_stock"].combine_first(ranked_source.get("name_membership"))
    elif "name" not in ranked_source.columns and "name_membership" in ranked_source.columns:
        ranked_source["name"] = ranked_source["name_membership"]

    top_stock_columns = [
        "trade_date_membership",
        "snapshot_time_membership",
        "captured_at_membership",
        "membership_as_of_date",
        "concept_type",
        "concept_name",
        "rank",
        "code",
        "name",
        "new_price",
        "change_rate",
        "deal_amount",
        "stock_count",
        "score",
        "config_hash",
    ]
    top_stocks = ranked_source.reindex(columns=top_stock_columns).rename(
        columns={
            "trade_date_membership": "trade_date",
            "snapshot_time_membership": "snapshot_time",
            "captured_at_membership": "captured_at",
        }
    )
    return _top_stock_frame(top_stocks.to_dict("records"))


def _sort_concepts(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        ["score", "total_deal_amount", "avg_change_rate", "concept_name"],
        ascending=[False, False, False, True],
        na_position="last",
        kind="mergesort",
    ).reset_index(drop=True)


def _weighted_change_rate(row: pd.Series) -> float:
    total_deal_amount = row["total_deal_amount"]
    if total_deal_amount and total_deal_amount > 0:
        return row["weighted_change_sum"] / total_deal_amount
    return row["avg_change_rate"]


def _base_diagnostics(stock_df: pd.DataFrame, membership_df: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
    stock_input_count = 0 if stock_df is None else int(len(stock_df))
    membership_input_count = 0 if membership_df is None else int(len(membership_df))
    return {
        "stock_input_count": stock_input_count,
        "membership_input_count": membership_input_count,
        "min_stock_count": config["min_stock_count"],
        "joined_membership_stock_count": 0,
        "excluded_concept_count": 0,
        "valid_concept_count": 0,
        "top_stock_count": 0,
    }


def _normalize_code(value: Any) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    if len(digits) > 6:
        digits = digits[-6:]
    return digits.zfill(6) if len(digits) <= 6 else None


def _split_members(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        candidates = value
    elif value is None or pd.isna(value):
        return []
    else:
        candidates = str(value).replace("，", ",").split(",")
    members: list[str] = []
    for item in candidates:
        cleaned = _clean_text(item)
        if cleaned:
            members.append(cleaned)
    return members


def _clean_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _to_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number) or not math.isfinite(float(number)):
        return None
    return float(number)


def _first_present(df: pd.DataFrame, *columns: str) -> Any | None:
    for column in columns:
        if column in df.columns:
            values = df[column].dropna()
            if not values.empty:
                return values.iloc[0]
    return None


def _membership_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "trade_date",
            "snapshot_time",
            "captured_at",
            "membership_as_of_date",
            "concept_type",
            "concept_name",
            "code",
            "name",
        ],
    )


def _stock_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "trade_date",
            "snapshot_time",
            "captured_at",
            "code",
            "name",
            "new_price",
            "change_rate",
            "deal_amount",
            "config_hash",
        ],
    )


def _concept_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "trade_date",
        "snapshot_time",
        "captured_at",
        "membership_as_of_date",
        "concept_type",
        "concept_name",
        "stock_count",
        "up_count",
        "rise_ratio",
        "avg_change_rate",
        "weighted_change_rate",
        "total_deal_amount",
        "limit_up_count",
        "score",
        "weighted_change_rate_percentile",
        "avg_change_rate_percentile",
        "deal_amount_percentile",
        "limit_up_count_percentile",
        "weight_avg_change_rate_percentile",
        "weight_deal_amount_percentile",
        "weight_limit_up_count_percentile",
        "weight_rise_ratio",
        "weight_weighted_change_rate_percentile",
        "config_hash",
        "config_json",
    ]
    return pd.DataFrame(rows, columns=columns)


def _top_stock_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "trade_date",
            "snapshot_time",
            "captured_at",
            "membership_as_of_date",
            "concept_type",
            "concept_name",
            "rank",
            "code",
            "name",
            "new_price",
            "change_rate",
            "deal_amount",
            "stock_count",
            "score",
            "config_hash",
        ],
    )
