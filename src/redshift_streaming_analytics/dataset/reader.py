from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

import duckdb


def iter_parquet_rows(
    parquet_path: str,
    batch_size: int,
    assume_sorted: bool,
    event_time_column: str,
) -> Iterable[dict[str, Any]]:
    con = duckdb.connect()
    query = f"SELECT * FROM read_parquet('{parquet_path}')"
    if not assume_sorted:
        query += f" ORDER BY {event_time_column}"
    cursor = con.execute(query)
    columns = [desc[0] for desc in cursor.description]

    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        for row in batch:
            yield dict(zip(columns, row, strict=True))


def normalize_event_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return None
