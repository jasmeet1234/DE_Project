#!/usr/bin/env python3
"""Replay Redset parquet data as a time-accelerated event stream.

Usage examples:
  python scripts/replay_producer.py \
    --input data/sample_0.001.parquet \
    --speedup 60 \
    --mode stdout

  python scripts/replay_producer.py \
    --input data/sample_0.001.parquet \
    --speedup 60 \
    --mode kafka \
    --kafka-bootstrap localhost:9092 \
    --kafka-topic redset.events
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from typing import Any, Iterable, Optional

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay Redset parquet events.")
    parser.add_argument("--input", required=True, help="Path to cleaned parquet file.")
    parser.add_argument(
        "--speedup",
        type=float,
        default=60.0,
        help="Replay speedup (e.g., 60 = 1 min == 1 hour).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows to fetch per batch.",
    )
    parser.add_argument(
        "--mode",
        choices=["stdout", "kafka"],
        default="stdout",
        help="Where to send events.",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--kafka-topic",
        default="redset.events",
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events (0 = no limit).",
    )
    parser.add_argument(
        "--assume-sorted",
        action="store_true",
        help="Skip ORDER BY if data already sorted by arrival_timestamp.",
    )
    parser.add_argument(
        "--sleep-cap",
        type=float,
        default=2.0,
        help="Max sleep per event in seconds (prevents long pauses).",
    )
    return parser.parse_args()


def to_json(row: dict[str, Any]) -> str:
    def default(o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        return o

    return json.dumps(row, default=default)


def iter_rows(
    parquet_path: str,
    batch_size: int,
    assume_sorted: bool,
) -> Iterable[dict[str, Any]]:
    con = duckdb.connect()
    query = f"SELECT * FROM read_parquet('{parquet_path}')"
    if not assume_sorted:
        query += " ORDER BY arrival_timestamp"
    cursor = con.execute(query)
    columns = [desc[0] for desc in cursor.description]
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        for row in batch:
            yield dict(zip(columns, row, strict=True))


def replay_events(args: argparse.Namespace) -> None:
    producer = None
    if args.mode == "kafka":
        try:
            from kafka import KafkaProducer  # type: ignore
        except ImportError as exc:
            raise SystemExit(
                "kafka-python is required for --mode kafka. "
                "Install with: pip install kafka-python"
            ) from exc

        producer = KafkaProducer(
            bootstrap_servers=args.kafka_bootstrap,
            value_serializer=lambda v: v.encode("utf-8"),
        )

    max_events = args.max_events if args.max_events > 0 else None
    sent = 0
    prev_ts: Optional[datetime] = None

    for row in iter_rows(args.input, args.batch_size, args.assume_sorted):
        if max_events is not None and sent >= max_events:
            break

        arrival_ts = row.get("arrival_timestamp")
        if isinstance(arrival_ts, str):
            arrival_ts = datetime.fromisoformat(arrival_ts)
        if isinstance(arrival_ts, datetime):
            if prev_ts is not None:
                delta = (arrival_ts - prev_ts).total_seconds() / args.speedup
                if delta > 0:
                    time.sleep(min(delta, args.sleep_cap))
            prev_ts = arrival_ts

        payload = to_json(row)
        if producer:
            producer.send(args.kafka_topic, payload)
        else:
            print(payload, flush=True)
        sent += 1

    if producer:
        producer.flush()
        producer.close()


def main() -> None:
    args = parse_args()
    replay_events(args)


if __name__ == "__main__":
    main()
