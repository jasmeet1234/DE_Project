from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable

import duckdb
from kafka import KafkaConsumer

logger = logging.getLogger("redshift_streaming_analytics.consumer.duckdb")


@dataclass
class DuckDBConsumerConfig:
    bootstrap_servers: str
    topic: str
    duckdb_path: str
    window_minutes: int
    event_time_column: str
    group_id: str = "redset-duckdb-consumer"


def _parse_message(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))


def _floor_window(ts: datetime, window_minutes: int) -> datetime:
    minute = (ts.minute // window_minutes) * window_minutes
    return ts.replace(minute=minute, second=0, microsecond=0)


def _prepare_event(event: dict[str, Any], event_time_column: str, window_minutes: int) -> dict[str, Any]:
    ts_value = event.get(event_time_column)
    if isinstance(ts_value, str):
        ts_value = datetime.fromisoformat(ts_value)
    if not isinstance(ts_value, datetime):
        raise ValueError("Missing or invalid event timestamp")

    event["arrival_timestamp"] = ts_value
    event["window_start"] = _floor_window(ts_value, window_minutes)
    return event


def _insert_events(con: duckdb.DuckDBPyConnection, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    con.execute("INSERT INTO events SELECT * FROM df", {"df": events})


def _update_rollups(con: duckdb.DuckDBPyConnection, window_starts: Iterable[datetime]) -> None:
    windows = list({ws for ws in window_starts})
    if not windows:
        return

    con.execute(
        """
        MERGE INTO rollups_5m AS target
        USING (
            SELECT
                window_start,
                query_type,
                COUNT(*) AS total_queries,
                SUM(CASE WHEN mbytes_spilled > 0 THEN 1 ELSE 0 END) AS spilled_queries,
                SUM(COALESCE(mbytes_spilled, 0)) AS spilled_mb,
                SUM(CASE WHEN was_cached THEN 1 ELSE 0 END) AS cached_queries,
                AVG(COALESCE(execution_duration_ms, 0)) AS avg_exec_ms
            FROM events
            WHERE window_start IN (SELECT * FROM UNNEST($windows))
            GROUP BY window_start, query_type
        ) AS source
        ON target.window_start = source.window_start AND target.query_type = source.query_type
        WHEN MATCHED THEN UPDATE SET
            total_queries = source.total_queries,
            spilled_queries = source.spilled_queries,
            spilled_mb = source.spilled_mb,
            cached_queries = source.cached_queries,
            avg_exec_ms = source.avg_exec_ms
        WHEN NOT MATCHED THEN INSERT (
            window_start,
            query_type,
            total_queries,
            spilled_queries,
            spilled_mb,
            cached_queries,
            avg_exec_ms
        ) VALUES (
            source.window_start,
            source.query_type,
            source.total_queries,
            source.spilled_queries,
            source.spilled_mb,
            source.cached_queries,
            source.avg_exec_ms
        )
        """,
        {"windows": windows},
    )


class DuckDBConsumer:
    def __init__(self, config: DuckDBConsumerConfig) -> None:
        self.config = config
        self.consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self.con = duckdb.connect(config.duckdb_path)

    def run(self) -> None:
        logger.info("Starting DuckDB consumer...")
        batch: list[dict[str, Any]] = []
        window_starts: list[datetime] = []
        last_flush = datetime.utcnow()

        for message in self.consumer:
            event = _parse_message(message.value)
            try:
                prepared = _prepare_event(
                    event, self.config.event_time_column, self.config.window_minutes
                )
            except ValueError:
                logger.warning("Skipping event with invalid timestamp")
                continue

            batch.append(prepared)
            window_starts.append(prepared["window_start"])

            if len(batch) >= 1000:
                self._flush(batch, window_starts)
                batch = []
                window_starts = []
                last_flush = datetime.utcnow()

            if (datetime.utcnow() - last_flush) > timedelta(seconds=5) and batch:
                self._flush(batch, window_starts)
                batch = []
                window_starts = []
                last_flush = datetime.utcnow()

    def _flush(self, batch: list[dict[str, Any]], window_starts: list[datetime]) -> None:
        _insert_events(self.con, batch)
        _update_rollups(self.con, window_starts)
        logger.info("Inserted %s events", len(batch))
