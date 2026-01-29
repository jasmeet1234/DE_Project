from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer

logger = logging.getLogger("redshift_streaming_analytics.consumer.redshift")


@dataclass
class RedshiftConsumerConfig:
    bootstrap_servers: str
    topic: str
    output_dir: str
    batch_size: int
    enabled: bool


def _write_parquet_batch(batch: list[dict[str, Any]], output_dir: Path) -> Path:
    import pandas as pd

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"events_{ts}.parquet"
    pd.DataFrame(batch).to_parquet(path, index=False)
    return path


def _parse_message(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))


class RedshiftConsumer:
    def __init__(self, config: RedshiftConsumerConfig) -> None:
        self.config = config
        self.consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id="redset-redshift-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

    def run(self) -> None:
        if not self.config.enabled:
            logger.info("Redshift loader disabled by configuration.")
            return

        logger.info("Starting Redshift loader (local parquet batch mode).")
        batch: list[dict[str, Any]] = []

        for message in self.consumer:
            batch.append(_parse_message(message.value))
            if len(batch) >= self.config.batch_size:
                path = _write_parquet_batch(batch, Path(self.config.output_dir))
                logger.info("Wrote batch parquet: %s", path)
                batch = []
