#!/bin/sh
for t in 1300 1330 1400 1430 1500; 
do sh instock/bin/run_hot_concept_job.sh intraday --trade-date 2026-07-14 --snapshot-time "$t" --config instock/config/hot_concept_score.json --top-n 20;
done
