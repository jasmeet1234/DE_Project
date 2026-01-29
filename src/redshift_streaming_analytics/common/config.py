from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


def _env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def load_config(path: str = "configs/app.yaml") -> dict[str, Any]:
    load_dotenv()
    cfg = yaml.safe_load(Path(path).read_text())

    cfg["kafka"]["bootstrap_servers"] = _env(
        "KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"]["bootstrap_servers"]
    )
    cfg["kafka"]["topic"] = _env("KAFKA_TOPIC", cfg["kafka"]["topic"])

    cfg["dataset"]["source_url"] = _env(
        "S3_PARQUET_URL", cfg["dataset"]["source_url"]
    )
    cfg["dataset"]["event_time_column"] = _env(
        "EVENT_TIME_COLUMN", cfg["dataset"]["event_time_column"]
    )

    cfg["replay"]["speed_factor"] = float(
        _env("REPLAY_SPEED_FACTOR", str(cfg["replay"]["speed_factor"]))
    )

    cfg["redshift"]["s3_curated_bucket"] = _env(
        "S3_CURATED_BUCKET", cfg["redshift"]["s3_curated_bucket"]
    )
    cfg["redshift"]["s3_curated_prefix"] = _env(
        "S3_CURATED_PREFIX", cfg["redshift"]["s3_curated_prefix"]
    )
    cfg["redshift"]["aws_region"] = _env(
        "AWS_REGION", cfg["redshift"]["aws_region"]
    )
    cfg["redshift"]["redshift_host"] = _env(
        "REDSHIFT_HOST", cfg["redshift"]["redshift_host"]
    )
    cfg["redshift"]["redshift_port"] = int(
        _env("REDSHIFT_PORT", str(cfg["redshift"]["redshift_port"]))
    )
    cfg["redshift"]["redshift_db"] = _env(
        "REDSHIFT_DB", cfg["redshift"]["redshift_db"]
    )
    cfg["redshift"]["redshift_user"] = _env(
        "REDSHIFT_USER", cfg["redshift"]["redshift_user"]
    )
    cfg["redshift"]["redshift_password"] = _env(
        "REDSHIFT_PASSWORD", cfg["redshift"]["redshift_password"]
    )
    cfg["redshift"]["redshift_schema"] = _env(
        "REDSHIFT_SCHEMA", cfg["redshift"]["redshift_schema"]
    )
    cfg["redshift"]["redshift_iam_role"] = _env(
        "REDSHIFT_IAM_ROLE", cfg["redshift"]["redshift_iam_role"]
    )

    return cfg


@dataclass
class AppConfig:
    kafka_bootstrap: str
    kafka_topic: str
    source_url: str
    event_time_column: str
    speed_factor: float
    sleep_cap_seconds: float
    batch_size: int
    duckdb_path: str
    rollup_window_minutes: int
    redshift_enabled: bool
    redshift: dict[str, Any]


def get_app_config(path: str = "configs/app.yaml") -> AppConfig:
    cfg = load_config(path)
    return AppConfig(
        kafka_bootstrap=cfg["kafka"]["bootstrap_servers"],
        kafka_topic=cfg["kafka"]["topic"],
        source_url=cfg["dataset"]["source_url"],
        event_time_column=cfg["dataset"]["event_time_column"],
        speed_factor=float(cfg["replay"]["speed_factor"]),
        sleep_cap_seconds=float(cfg["replay"]["sleep_cap_seconds"]),
        batch_size=int(cfg["replay"]["batch_size"]),
        duckdb_path=cfg["storage"]["duckdb_path"],
        rollup_window_minutes=int(cfg["rollups"]["window_minutes"]),
        redshift_enabled=bool(cfg["redshift"]["enabled"]),
        redshift=cfg["redshift"],
    )
