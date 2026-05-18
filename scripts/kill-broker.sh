#!/usr/bin/env bash
# Usage: kill-broker.sh <broker-container-name>
# 예: ./scripts/kill-broker.sh kafka-1
#
# 실험 3, 4 에서 사용. 되살릴 때는 `docker compose start kafka-1`
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <kafka-1|kafka-2|kafka-3>"
  exit 1
fi

BROKER="$1"

case "$BROKER" in
  kafka-1|kafka-2|kafka-3) ;;
  *) echo "[error] broker must be one of kafka-1, kafka-2, kafka-3" >&2; exit 1 ;;
esac

echo "[kill] stopping container $BROKER"
docker stop "$BROKER"
echo "[ok] $BROKER stopped. 되살리려면: docker compose start $BROKER"
