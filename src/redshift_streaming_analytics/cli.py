from __future__ import annotations

import argparse
import logging.config
import subprocess
from pathlib import Path

from redshift_streaming_analytics.common.config import get_app_config, load_config
from redshift_streaming_analytics.consumers.duckdb_consumer import (
    DuckDBConsumer,
    DuckDBConsumerConfig,
)
from redshift_streaming_analytics.consumers.redshift_consumer import (
    RedshiftConsumer,
    RedshiftConsumerConfig,
)
from redshift_streaming_analytics.producer.replay import replay_to_kafka


def _setup_logging() -> None:
    logging.config.dictConfig(
        load_config("configs/logging.yaml")
        if Path("configs/logging.yaml").exists()
        else {}
    )


def producer_cmd(args: argparse.Namespace) -> None:
    cfg = get_app_config()
    replay_to_kafka(
        parquet_path=args.input or cfg.source_url,
        bootstrap_servers=cfg.kafka_bootstrap,
        topic=cfg.kafka_topic,
        speed_factor=cfg.speed_factor,
        batch_size=cfg.batch_size,
        sleep_cap_seconds=cfg.sleep_cap_seconds,
        assume_sorted=args.assume_sorted,
        event_time_column=cfg.event_time_column,
        max_events=args.max_events,
    )


def consumer_duckdb_cmd(_: argparse.Namespace) -> None:
    cfg = get_app_config()
    consumer = DuckDBConsumer(
        DuckDBConsumerConfig(
            bootstrap_servers=cfg.kafka_bootstrap,
            topic=cfg.kafka_topic,
            duckdb_path=cfg.duckdb_path,
            window_minutes=cfg.rollup_window_minutes,
            event_time_column=cfg.event_time_column,
        )
    )
    consumer.run()


def consumer_redshift_cmd(_: argparse.Namespace) -> None:
    cfg = get_app_config()
    consumer = RedshiftConsumer(
        RedshiftConsumerConfig(
            bootstrap_servers=cfg.kafka_bootstrap,
            topic=cfg.kafka_topic,
            output_dir="data/redshift_batches",
            batch_size=cfg.batch_size,
            enabled=cfg.redshift_enabled,
        )
    )
    consumer.run()


def ui_cmd(_: argparse.Namespace) -> None:
    app_path = Path("src/redshift_streaming_analytics/ui/app.py")
    subprocess.run(["python", "-m", "streamlit", "run", str(app_path)], check=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("redshift_streaming_analytics")
    sub = parser.add_subparsers(dest="command", required=True)

    producer = sub.add_parser("producer", help="Replay parquet events into Kafka")
    producer.add_argument("--input", help="Parquet path or URL")
    producer.add_argument("--assume-sorted", action="store_true")
    producer.add_argument("--max-events", type=int, default=None)

    sub.add_parser("consumer-duckdb", help="Consume Kafka into DuckDB")
    sub.add_parser("consumer-redshift", help="Optional Redshift batch loader")
    sub.add_parser("ui", help="Launch Streamlit UI")

    return parser


def main() -> None:
    _setup_logging()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "producer":
        producer_cmd(args)
    elif args.command == "consumer-duckdb":
        consumer_duckdb_cmd(args)
    elif args.command == "consumer-redshift":
        consumer_redshift_cmd(args)
    elif args.command == "ui":
        ui_cmd(args)


if __name__ == "__main__":
    main()
