from __future__ import annotations

import duckdb


def connect(path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path)
