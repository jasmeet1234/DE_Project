# Redshift Streaming Analytics

A production-grade, end-to-end demo application for **streaming analytics on Redshift query metrics**.

It provides:
- **Producer**: replays query-metric rows from a Parquet file/URL into Kafka (with configurable time scaling)
- **DuckDB consumer**: stores canonical events and computes **overlap-aware** 5-minute rollups
- **Streamlit UI**: live and historical dashboards reading from DuckDB
- **Optional Redshift loader**: batches Kafka events to Parquet -> uploads to S3 -> issues Redshift `COPY` to load events

> The project runs fully locally without AWS/Redshift. The Redshift loader is optional and guarded by configuration.

---

## Quickstart (local, end-to-end)

### 1) Prerequisites
- Python **3.11+**
- Docker + Docker Compose

### 2) Create and activate a virtualenv
```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -U pip
```

### 3) Install
```bash
pip install -e .
```

### 4) Start Kafka
```bash
docker compose up -d
bash scripts/create_kafka_topics.sh
```

### 5) Bootstrap DuckDB schema
```bash
python scripts/bootstrap_duckdb.py
```

### 6) Run the DuckDB consumer (Terminal A)
```bash
python -m redshift_streaming_analytics.cli consumer-duckdb
```

### 7) Run the producer (Terminal B)
Generate a tiny sample dataset (one-time) and replay it:
```bash
python sample_data/generate_dummy_parquet.py
python -m redshift_streaming_analytics.cli producer --input sample_data/dummy_query_metrics.parquet
```

### 8) Run the UI (Terminal C)
```bash
python -m redshift_streaming_analytics.cli ui
```
Streamlit will print a local URL.

---

## One-command local run

```bash
bash scripts/run_all_local.sh
```
This starts Kafka, creates topics, bootstraps DuckDB, and launches:
- DuckDB consumer
- UI
- Producer (using `dataset.source_url` from `configs/app.yaml` unless overridden)

---

## Configuration

Primary config file: `configs/app.yaml`.

### Environment variables
Copy `.env.example` to `.env` and adjust as needed.

Common overrides:
- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `S3_PARQUET_URL` / `EVENT_TIME_COLUMN`
- `REPLAY_SPEED_FACTOR` (default: 50)

Optional Redshift loader:
- `S3_CURATED_BUCKET`, `S3_CURATED_PREFIX`, `AWS_REGION`
- `REDSHIFT_HOST`, `REDSHIFT_PORT`, `REDSHIFT_DB`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_SCHEMA`
- `REDSHIFT_IAM_ROLE` (recommended for COPY)

---

## CLI commands

All commands are under the single module entrypoint:

```bash
python -m redshift_streaming_analytics.cli <command>
```

Commands:
- `producer` – replay Parquet into Kafka
- `consumer-duckdb` – consume Kafka into DuckDB + rollups
- `consumer-redshift` – optional S3 + Redshift COPY loader
- `ui` – Streamlit dashboard

---

## Project structure

```
redshift-streaming-analytics/
  configs/
    app.yaml
    logging.yaml
    sql/
      duckdb/
      redshift/
  sample_data/
  scripts/
  src/redshift_streaming_analytics/
    cli.py
    common/
    dataset/
    producer/
    consumers/
    storage/
    ui/
  tests/
```

---

## Testing

```bash
pytest -q
```

---

## Troubleshooting

### DuckDB cannot LOAD httpfs
The Parquet reader uses DuckDB’s `httpfs` extension for HTTPS/S3 URLs. If your environment blocks extension downloads, use a local parquet file path instead.

### Kafka not reachable
Ensure Docker is running and the broker is up:
```bash
docker compose ps
```

---

## License
MIT
