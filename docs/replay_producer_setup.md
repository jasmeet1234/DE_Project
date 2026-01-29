# Replay Producer Setup (Local Cleaned Data)

This guide shows how to run the **replay producer** against your **locally cleaned parquet file** (including the 0.001% sample).

## 1) Prerequisites

- **Python 3.9+**
- **Pip** (Python package manager)

### Install required packages
```bash
pip install duckdb
```

### Optional (only if you want Kafka output)
```bash
pip install kafka-python
```

## 2) Example: Run on the 0.001% sample

Assume your cleaned sample is at `data/sample_0.001.parquet`.

### Output to stdout (quick test)
```bash
python scripts/replay_producer.py \
  --input data/sample_0.001.parquet \
  --speedup 60 \
  --mode stdout
```

This prints JSON events to the terminal. You can pipe them to a file:
```bash
python scripts/replay_producer.py \
  --input data/sample_0.001.parquet \
  --speedup 60 \
  --mode stdout > events.jsonl
```

## 3) Example: Send events to Kafka

Start Kafka (local Docker or other setup), then run:
```bash
python scripts/replay_producer.py \
  --input data/sample_0.001.parquet \
  --speedup 60 \
  --mode kafka \
  --kafka-bootstrap localhost:9092 \
  --kafka-topic redset.events
```

## 4) Running on the full cleaned dataset (≈16GB)

```bash
python scripts/replay_producer.py \
  --input /path/to/cleaned_full.parquet \
  --speedup 60 \
  --mode kafka \
  --kafka-bootstrap localhost:9092 \
  --kafka-topic redset.events
```

## 5) Important options

- `--assume-sorted` : Skip sorting if your data is already ordered by `arrival_timestamp`.
- `--batch-size` : Control batch size pulled from parquet (default 1000).
- `--max-events` : Limit events for quick tests.
- `--sleep-cap` : Avoid long sleeps if large time gaps appear (default 2s).

## 6) Where to run it

- **Local laptop**: Best for the 0.001% sample.
- **VM/Server**: Recommended for the full 16GB dataset.

The producer only needs access to the parquet file (local disk) and Kafka (if using `--mode kafka`).
