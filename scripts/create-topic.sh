#!/usr/bin/env bash
# Usage: create-topic.sh <topic> <partitions> <replication-factor> [extra --config key=value ...]
#
# Examples:
#   ./scripts/create-topic.sh demo 3 3
#   ./scripts/create-topic.sh safety-demo 1 3 min.insync.replicas=2
set -euo pipefail

# Windows Git Bash 경로 자동 변환 방지 (Linux/Mac에서는 no-op)
export MSYS_NO_PATHCONV=1

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <topic> <partitions> <replication-factor> [config=value ...]"
  exit 1
fi

TOPIC="$1"
PARTITIONS="$2"
RF="$3"
shift 3

CONFIG_ARGS=()
for kv in "$@"; do
  CONFIG_ARGS+=(--config "$kv")
done

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$RF" \
  "${CONFIG_ARGS[@]}"

echo "[ok] topic '$TOPIC' created (partitions=$PARTITIONS, rf=$RF, extra=$*)"

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --describe \
  --topic "$TOPIC"
