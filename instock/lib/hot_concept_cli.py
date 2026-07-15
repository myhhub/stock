#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import datetime as dt
import logging
from pathlib import Path
from typing import Any

import instock.lib.trade_time as trade_time

ALLOWED_SNAPSHOT_TIMES = (
    '0925',
    '0930',
    '1000',
    '1030',
    '1100',
    '1130',
    '1300',
    '1330',
    '1400',
    '1430',
    '1500',
)

_LOG_FORMAT = '%(asctime)s hot_concept trade_date=%(trade_date)s snapshot_time=%(snapshot_time)s config_hash=%(config_hash)s %(message)s'


def parse_trade_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f'Invalid trade date {value!r}; expected YYYY-MM-DD') from exc


def validate_snapshot_time(value: str) -> str:
    if value in ALLOWED_SNAPSHOT_TIMES:
        return value
    allowed = ', '.join(ALLOWED_SNAPSHOT_TIMES)
    raise argparse.ArgumentTypeError(f'Invalid snapshot time {value!r}; expected one of: {allowed}')


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f'Invalid positive integer {value!r}') from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f'Invalid positive integer {value!r}')
    return parsed


def build_common_parser(description: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=description,
        allow_abbrev=False,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--trade-date', type=parse_trade_date, metavar='YYYY-MM-DD')
    parser.add_argument('--snapshot-time', type=validate_snapshot_time, metavar='HHMM')
    parser.add_argument('--config', required=True, type=Path, metavar='PATH')
    parser.add_argument('--top-n', default=20, type=positive_int, metavar='INT')
    return parser


def should_skip_trade_date(trade_date: dt.date) -> bool:
    return not trade_time.is_trade_date(trade_date)


def is_trade_date_or_skip(trade_date: dt.date) -> bool:
    """Backward-compatible alias for should_skip_trade_date()."""
    return should_skip_trade_date(trade_date)


def hot_concept_log_context(
    trade_date: dt.date,
    snapshot_time: str,
    config_hash: str | None = None,
) -> dict[str, Any]:
    return {
        'trade_date': trade_date.isoformat(),
        'snapshot_time': snapshot_time,
        'config_hash': config_hash or '-',
    }


class _HotConceptContextFilter(logging.Filter):
    def __init__(self, context: dict[str, Any]) -> None:
        super().__init__()
        self._context = context

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in self._context.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


def setup_hot_concept_logging(
    trade_date: dt.date,
    snapshot_time: str,
    config_hash: str | None = None,
    *,
    log_filename: str = 'stock_hot_concept.log',
    level: int = logging.INFO,
) -> logging.LoggerAdapter:
    context = hot_concept_log_context(trade_date, snapshot_time, config_hash)
    log_dir = Path(__file__).resolve().parents[1] / 'log'
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level,
        format=_LOG_FORMAT,
        filename=str(log_dir / log_filename),
        force=True,
    )
    root_logger = logging.getLogger()
    context_filter = _HotConceptContextFilter(context)
    root_logger.addFilter(context_filter)
    for handler in root_logger.handlers:
        handler.addFilter(context_filter)
    return logging.LoggerAdapter(logging.getLogger('hot_concept'), context)
