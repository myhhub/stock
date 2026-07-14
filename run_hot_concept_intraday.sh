#!/bin/sh
uv run instock/job/hot_concept_intraday_job.py --trade-date 2026-07-14 --snapshot-time 1000 --config instock/config/hot_concept_score.json
