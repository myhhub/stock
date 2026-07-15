#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

print_usage() {
  cat <<'EOF'
Hot concept operator wrapper.

External scheduler only: cron or supervisor should invoke this wrapper; no internal scheduler loop is added.

Exact intraday commands:
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 0925 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 0930 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1000 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1030 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1100 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1130 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1300 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1330 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1400 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1430 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --snapshot-time 1500 --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --trade-date YYYY-MM-DD --config instock/config/hot_concept_score.json
  uv run python -m instock.job.hot_concept_intraday_job --config instock/config/hot_concept_score.json

History job:
  uv run python -m instock.job.hot_concept_history_job --start-date YYYY-MM-DD --end-date YYYY-MM-DD --config instock/config/hot_concept_score.json

Query script examples:
  uv run python -m instock.job.hot_concept_query --trade-date YYYY-MM-DD --snapshot-time 0925 --format json
  uv run python -m instock.job.hot_concept_query --start-date YYYY-MM-DD --end-date YYYY-MM-DD --format json

Usage:
  run_hot_concept_job.sh intraday [--trade-date YYYY-MM-DD] [--snapshot-time HHMM] --config PATH [--top-n INT]
  run_hot_concept_job.sh history --start-date YYYY-MM-DD --end-date YYYY-MM-DD --config PATH [--top-n INT]
  run_hot_concept_job.sh query [hot_concept_query args] [--format json|table]
  run_hot_concept_job.sh help

Auto mode notes:
  - If --snapshot-time is omitted and --trade-date is a past date, the job runs every auto slot: 0930,1000,1030,1100,1130,1300,1330,1400,1430,1500.
  - If --snapshot-time is omitted and --trade-date is today (or omitted), the job runs only slots whose time has already passed.
  - Auto mode skips slots that already have rows for the same trade_date + snapshot_time + config_hash.
  - 0925 remains supported only when passed explicitly via --snapshot-time 0925.
EOF
}

case "${1:-help}" in
  help|-h|--help)
    print_usage
    ;;
  intraday)
    shift
    cd "$ROOT_DIR"
    exec uv run python -m instock.job.hot_concept_intraday_job "$@"
    ;;
  history)
    shift
    cd "$ROOT_DIR"
    exec uv run python -m instock.job.hot_concept_history_job "$@"
    ;;
  query)
    shift
    cd "$ROOT_DIR"
    exec uv run python -m instock.job.hot_concept_query "$@"
    ;;
  *)
    print_usage >&2
    exit 1
    ;;
esac
