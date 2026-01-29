#!/usr/bin/env python3
from pathlib import Path

import duckdb

DB_PATH = Path("data/redset.duckdb")
SQL_PATH = Path("configs/sql/duckdb/bootstrap.sql")

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

with duckdb.connect(str(DB_PATH)) as con:
    con.execute(SQL_PATH.read_text())

print(f"Bootstrapped DuckDB at {DB_PATH}")
