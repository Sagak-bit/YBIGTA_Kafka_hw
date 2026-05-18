#!/usr/bin/env bash
# Usage: describe-topic.sh <topic>
# 살아있는 broker 중 하나에 붙어 describe 합니다. kafka-1이 죽었을 때도 동작.
set -euo pipefail

# Windows Git Bash 경로 자동 변환 방지 (Linux/Mac에서는 no-op)
export MSYS_NO_PATHCONV=1

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic>"
  exit 1
fi

TOPIC="$1"

# kafka-1, kafka-2, kafka-3 중 RUNNING 인 첫번째를 선택
for B in kafka-1 kafka-2 kafka-3; do
  if docker ps --format '{{.Names}}' | grep -q "^${B}$"; then
    BROKER="$B"
    break
  fi
done

if [[ -z "${BROKER:-}" ]]; then
  echo "[error] no kafka broker container is running" >&2
  exit 1
fi

echo "[describe] via container=$BROKER topic=$TOPIC"
docker exec "$BROKER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BROKER}:19092" \
  --describe \
  --topic "$TOPIC"
