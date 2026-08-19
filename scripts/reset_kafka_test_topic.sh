#!/usr/bin/env bash
# Purga ohlcv.raw entre suites de test Kafka (higiene, no correctness-critical
# tras el fix KAF-001/R-05). AUTO_CREATE_TOPICS_ENABLE=true lo recrea al
# primer produce. Uso manual — NO se invoca automáticamente en conftest.py.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:9092 --delete --topic ohlcv.raw
echo "Topic ohlcv.raw eliminado — se recreará automáticamente al siguiente produce."
