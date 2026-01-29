from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

DB_PATH = "data/redset.duckdb"

st.set_page_config(page_title="Redshift Streaming Analytics", layout="wide")

st.title("Redshift Streaming Analytics")

with duckdb.connect(DB_PATH) as con:
    st.subheader("Latest 5-minute rollups")
    rollups = con.execute(
        """
        SELECT *
        FROM rollups_5m
        ORDER BY window_start DESC
        LIMIT 50
        """
    ).df()
    st.dataframe(rollups)

    st.subheader("Spilled MB by query type (last 1 hour)")
    spill = con.execute(
        """
        SELECT query_type, SUM(spilled_mb) AS spilled_mb
        FROM rollups_5m
        WHERE window_start >= NOW() - INTERVAL '1 hour'
        GROUP BY query_type
        ORDER BY spilled_mb DESC
        """
    ).df()
    if not spill.empty:
        st.bar_chart(spill.set_index("query_type"))

    st.subheader("Cache hit rate (last 1 hour)")
    cache = con.execute(
        """
        SELECT
            SUM(cached_queries)::DOUBLE / NULLIF(SUM(total_queries), 0) AS hit_rate
        FROM rollups_5m
        WHERE window_start >= NOW() - INTERVAL '1 hour'
        """
    ).fetchone()[0]
    st.metric("Cache hit rate", f"{cache or 0:.2%}")
