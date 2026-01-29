#!/usr/bin/env bash
set -euo pipefail

TOPIC=${KAFKA_TOPIC:-redset.events}

if ! docker compose ps kafka >/dev/null 2>&1; then
  echo "Kafka container not running. Start with: docker compose up -d" >&2
  exit 1
fi

docker compose exec -T kafka bash -c \
  "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic ${TOPIC} --partitions 3 --replication-factor 1"

echo "Created topic: ${TOPIC}"
