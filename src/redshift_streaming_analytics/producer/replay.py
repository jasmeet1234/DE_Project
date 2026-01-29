from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Optional

from kafka import KafkaProducer

from redshift_streaming_analytics.dataset.reader import iter_parquet_rows, normalize_event_time


def _to_json(row: dict[str, Any]) -> str:
    def default(o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        return o

    return json.dumps(row, default=default)


def replay_to_kafka(
    parquet_path: str,
    bootstrap_servers: str,
    topic: str,
    speed_factor: float,
    batch_size: int,
    sleep_cap_seconds: float,
    assume_sorted: bool,
    event_time_column: str,
    max_events: int | None = None,
) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    sent = 0
    prev_ts: Optional[datetime] = None

    for row in iter_parquet_rows(parquet_path, batch_size, assume_sorted, event_time_column):
        if max_events is not None and sent >= max_events:
            break

        arrival_ts = normalize_event_time(row.get(event_time_column))
        if arrival_ts is not None:
            if prev_ts is not None:
                delta = (arrival_ts - prev_ts).total_seconds() / speed_factor
                if delta > 0:
                    time.sleep(min(delta, sleep_cap_seconds))
            prev_ts = arrival_ts

        producer.send(topic, _to_json(row))
        sent += 1

    producer.flush()
    producer.close()
