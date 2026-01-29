#!/usr/bin/env bash
set -euo pipefail

bash scripts/create_kafka_topics.sh
python scripts/bootstrap_duckdb.py
python sample_data/generate_dummy_parquet.py

python -m redshift_streaming_analytics.cli consumer-duckdb &
CONSUMER_PID=$!

python -m redshift_streaming_analytics.cli ui &
UI_PID=$!

python -m redshift_streaming_analytics.cli producer

wait $CONSUMER_PID $UI_PID
