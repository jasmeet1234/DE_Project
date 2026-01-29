CREATE TABLE IF NOT EXISTS events (
  instance_id BIGINT,
  cluster_size BIGINT,
  user_id BIGINT,
  database_id BIGINT,
  query_id BIGINT,
  arrival_timestamp TIMESTAMP,
  compile_duration_ms BIGINT,
  queue_duration_ms BIGINT,
  execution_duration_ms BIGINT,
  feature_fingerprint VARCHAR,
  was_aborted BOOLEAN,
  was_cached BOOLEAN,
  cache_source_query_id BIGINT,
  query_type VARCHAR,
  num_permanent_tables_accessed BIGINT,
  num_external_tables_accessed BIGINT,
  num_system_tables_accessed BIGINT,
  read_table_ids VARCHAR,
  write_table_ids VARCHAR,
  mbytes_scanned DOUBLE,
  mbytes_spilled DOUBLE,
  num_joins BIGINT,
  num_scans BIGINT,
  num_aggregations BIGINT,
  window_start TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rollups_5m (
  window_start TIMESTAMP,
  query_type VARCHAR,
  total_queries BIGINT,
  spilled_queries BIGINT,
  spilled_mb DOUBLE,
  cached_queries BIGINT,
  avg_exec_ms DOUBLE,
  PRIMARY KEY (window_start, query_type)
);

CREATE TABLE IF NOT EXISTS metadata (
  key VARCHAR PRIMARY KEY,
  value VARCHAR
);
