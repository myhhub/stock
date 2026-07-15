#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

START_DATE="${START_DATE:-2026-07-06}"
END_DATE="${END_DATE:-2026-07-13}"
CONFIG="${CONFIG:-instock/config/hot_concept_score.json}"
TOP_N="${TOP_N:-20}"

cd "$ROOT_DIR"
exec uv run python -m instock.job.hot_concept_history_job \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --config "$CONFIG" \
  --top-n "$TOP_N"
