#!/usr/bin/env python3
from datetime import datetime, timedelta

import pandas as pd

rows = []
start = datetime.utcnow() - timedelta(hours=1)
for i in range(500):
    ts = start + timedelta(seconds=i * 5)
    rows.append(
        {
            "instance_id": 1,
            "cluster_size": 2,
            "user_id": 10,
            "database_id": 20,
            "query_id": i,
            "arrival_timestamp": ts,
            "compile_duration_ms": 10 + i % 5,
            "queue_duration_ms": i % 3,
            "execution_duration_ms": 100 + i % 50,
            "feature_fingerprint": "abc",
            "was_aborted": False,
            "was_cached": i % 4 == 0,
            "cache_source_query_id": None,
            "query_type": "select" if i % 2 == 0 else "insert",
            "num_permanent_tables_accessed": 1,
            "num_external_tables_accessed": 0,
            "num_system_tables_accessed": 0,
            "read_table_ids": "1,2",
            "write_table_ids": "",
            "mbytes_scanned": 100.0 + i,
            "mbytes_spilled": 0.0 if i % 10 else 5.0,
            "num_joins": i % 3,
            "num_scans": 1,
            "num_aggregations": i % 2,
        }
    )

df = pd.DataFrame(rows)
df.to_parquet("sample_data/dummy_query_metrics.parquet", index=False)
print("Generated sample_data/dummy_query_metrics.parquet")
