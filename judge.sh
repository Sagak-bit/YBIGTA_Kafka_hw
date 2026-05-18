#!/usr/bin/env bash
#
# Kafka Lab 자동 채점기.
#
# 사용법:
#   ./judge.sh
#
# 14개 체크 항목 중 12개 이상 통과하면 PASS.
# 각 체크는 클러스터 상태와 artifacts/ 폴더의 결과 파일을 검사합니다.

set -uo pipefail

export MSYS_NO_PATHCONV=1

ARTIFACTS_DIR="${ARTIFACTS_DIR:-artifacts}"
PASS_COUNT=0
FAIL_COUNT=0
TOTAL=0
FAIL_REASONS=()

C_GREEN=$'\033[0;32m'
C_RED=$'\033[0;31m'
C_YELLOW=$'\033[0;33m'
C_DIM=$'\033[0;90m'
C_RESET=$'\033[0m'

pass() {
  TOTAL=$((TOTAL + 1))
  PASS_COUNT=$((PASS_COUNT + 1))
  printf "${C_GREEN}[ok]${C_RESET}   %s\n" "$1"
}

fail() {
  TOTAL=$((TOTAL + 1))
  FAIL_COUNT=$((FAIL_COUNT + 1))
  FAIL_REASONS+=("$1: $2")
  printf "${C_RED}[fail]${C_RESET} %s ${C_DIM}(%s)${C_RESET}\n" "$1" "$2"
}

section() {
  printf "\n${C_YELLOW}== %s ==${C_RESET}\n" "$1"
}

# -----------------------------------------------------------
# PHASE 1: cluster bootstrap
# -----------------------------------------------------------
section "PHASE 1: 클러스터 부트스트랩"

# 1. 3 brokers running
running_brokers=$(docker ps --filter "name=kafka-" --filter "status=running" \
  --format '{{.Names}}' 2>/dev/null | grep -E '^kafka-[123]$' | wc -l)
if [[ "$running_brokers" == "3" ]]; then
  pass "broker 3개 running"
else
  fail "broker 3개 running" "현재 $running_brokers 개만 running. docker compose up -d"
fi

# 2. kafka-1 reachable
if docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
     --bootstrap-server kafka-1:19092 >/dev/null 2>&1; then
  pass "kafka-1 API 응답"
else
  fail "kafka-1 API 응답" "kafka-1 컨테이너에서 broker가 아직 준비 안 됐을 수 있음"
fi

# -----------------------------------------------------------
# PHASE 2: Experiment 1 - Partitioning
# -----------------------------------------------------------
section "PHASE 2: 파티셔닝 (Experiment 1)"

# 3. partitioning-demo topic exists
if docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server kafka-1:19092 --list 2>/dev/null \
     | grep -qx "partitioning-demo"; then
  pass "partitioning-demo 토픽 존재"
else
  fail "partitioning-demo 토픽 존재" "토픽이 없음. PHASE 2 1단계 다시"
fi

# 4. alice key 모두 같은 partition
alice_log="${ARTIFACTS_DIR}/exp1-alice.log"
if [[ -f "$alice_log" ]]; then
  sent_count=$(grep -c "^sent " "$alice_log" 2>/dev/null || echo 0)
  partitions=$(grep -oE "partition=[0-9]+" "$alice_log" 2>/dev/null | sort -u | wc -l)
  if [[ "$sent_count" -ge 5 ]] && [[ "$partitions" == "1" ]]; then
    pass "alice 메시지 5개+ 모두 같은 partition"
  else
    fail "alice 메시지 5개+ 모두 같은 partition" \
      "sent=$sent_count, 사용된 partition 수=$partitions"
  fi
else
  fail "alice 메시지 5개+ 모두 같은 partition" "$alice_log 없음"
fi

# -----------------------------------------------------------
# PHASE 3: Experiment 2 - Consumer Group
# -----------------------------------------------------------
section "PHASE 3: Consumer Group (Experiment 2)"

# 5. cg-demo topic exists
if docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server kafka-1:19092 --list 2>/dev/null \
     | grep -qx "cg-demo"; then
  pass "cg-demo 토픽 존재"
else
  fail "cg-demo 토픽 존재" "토픽이 없음. PHASE 3 1단계 다시"
fi

# 6. REBALANCE assigned 로그가 최소 1개 컨슈머 산출물에 존재
rebalance_found=0
for f in "${ARTIFACTS_DIR}/exp2-c1.log" \
         "${ARTIFACTS_DIR}/exp2-c2.log" \
         "${ARTIFACTS_DIR}/exp2-c3.log"; do
  [[ -f "$f" ]] || continue
  if grep -q "REBALANCE.*assigned" "$f"; then
    rebalance_found=$((rebalance_found + 1))
  fi
done
if [[ "$rebalance_found" -ge 1 ]]; then
  pass "REBALANCE assigned 로그 캡처됨 ($rebalance_found 개 파일)"
else
  fail "REBALANCE assigned 로그 캡처됨" \
    "artifacts/exp2-c*.log 중 어디에도 REBALANCE assigned 없음"
fi

# -----------------------------------------------------------
# PHASE 4: Experiment 3 - Leader Election
# -----------------------------------------------------------
section "PHASE 4: Leader Election (Experiment 3) ⭐"

# 7. failover-demo topic exists with RF=3
fd_describe=$(docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server kafka-1:19092 \
                --describe --topic failover-demo 2>/dev/null)
if echo "$fd_describe" | grep -q "ReplicationFactor: 3"; then
  pass "failover-demo 토픽 RF=3"
else
  fail "failover-demo 토픽 RF=3" "토픽이 없거나 RF가 3이 아님"
fi

# 8. exp3-before.txt: 모든 파티션 ISR 3개
before="${ARTIFACTS_DIR}/exp3-before.txt"
if [[ -f "$before" ]]; then
  bad_lines=$(grep -E "Partition: [0-9]+" "$before" \
                | grep -vE "Isr: [0-9]+,[0-9]+,[0-9]+" | wc -l)
  partition_lines=$(grep -cE "Partition: [0-9]+" "$before")
  if [[ "$partition_lines" -ge 3 ]] && [[ "$bad_lines" == "0" ]]; then
    pass "kill 전: 모든 파티션 ISR=3"
  else
    fail "kill 전: 모든 파티션 ISR=3" \
      "partition_lines=$partition_lines, ISR<3 인 라인=$bad_lines"
  fi
else
  fail "kill 전: 모든 파티션 ISR=3" "$before 없음"
fi

# 9. exp3-after.txt: 적어도 한 파티션 ISR 축소
after="${ARTIFACTS_DIR}/exp3-after.txt"
if [[ -f "$after" ]]; then
  shrunk=$(grep -E "Partition: [0-9]+" "$after" \
             | grep -E "Isr: [0-9]+,[0-9]+([^,]|$)" \
             | grep -vE "Isr: [0-9]+,[0-9]+,[0-9]+" | wc -l)
  if [[ "$shrunk" -ge 1 ]]; then
    pass "kill 후: ISR 축소된 파티션 ≥1"
  else
    fail "kill 후: ISR 축소된 파티션 ≥1" \
      "ISR 가 여전히 3개거나 파일 형식 인식 실패"
  fi
else
  fail "kill 후: ISR 축소된 파티션 ≥1" "$after 없음"
fi

# 10. exp3-producer.log: 40개 메시지 손실 없이 sent (offset=39 까지)
prod_log="${ARTIFACTS_DIR}/exp3-producer.log"
if [[ -f "$prod_log" ]]; then
  if grep -q "offset=39" "$prod_log"; then
    pass "Producer offset=39 까지 전송 (손실 없음)"
  else
    sent_n=$(grep -c "^sent " "$prod_log" 2>/dev/null || echo 0)
    fail "Producer offset=39 까지 전송 (손실 없음)" \
      "마지막 offset=39 라인이 없음. sent 라인 수=$sent_n"
  fi
else
  fail "Producer offset=39 까지 전송 (손실 없음)" "$prod_log 없음"
fi

# -----------------------------------------------------------
# PHASE 5: Experiment 4 - acks
# -----------------------------------------------------------
section "PHASE 5: acks & min.insync.replicas (Experiment 4)"

# 11. safety-demo with min.insync.replicas=2
sd_describe=$(docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server kafka-1:19092 \
                --describe --topic safety-demo 2>/dev/null)
if echo "$sd_describe" | grep -q "min.insync.replicas=2"; then
  pass "safety-demo 토픽 min.insync.replicas=2"
else
  fail "safety-demo 토픽 min.insync.replicas=2" "토픽이 없거나 설정 누락"
fi

# 12. acks=all 시도 시 NotEnoughReplicas 에러 캡처
acks_all="${ARTIFACTS_DIR}/exp4-acks-all.log"
if [[ -f "$acks_all" ]]; then
  if grep -qE "NotEnoughReplicas|messages are rejected since there are fewer in-sync replicas" "$acks_all"; then
    pass "acks=all 에서 NotEnoughReplicas 에러 캡처"
  else
    fail "acks=all 에서 NotEnoughReplicas 에러 캡처" \
      "에러 문자열을 찾지 못함. 정말 broker 2개 죽은 상태였는지 확인"
  fi
else
  fail "acks=all 에서 NotEnoughReplicas 에러 캡처" "$acks_all 없음"
fi

# 13. acks=1 성공 sent 라인 ≥ 3
acks_1="${ARTIFACTS_DIR}/exp4-acks-1.log"
if [[ -f "$acks_1" ]]; then
  sent_n=$(grep -c "^sent " "$acks_1" 2>/dev/null || echo 0)
  if [[ "$sent_n" -ge 3 ]]; then
    pass "acks=1 에서 sent 3개+"
  else
    fail "acks=1 에서 sent 3개+" "sent 라인=$sent_n"
  fi
else
  fail "acks=1 에서 sent 3개+" "$acks_1 없음"
fi

# -----------------------------------------------------------
# PHASE 6: Experiment 5 - Consumer Offset
# -----------------------------------------------------------
section "PHASE 6: Consumer Offset (Experiment 5)"

# 14. __consumer_offsets 토픽 캡처
offsets_log="${ARTIFACTS_DIR}/exp5-offsets.log"
if [[ -f "$offsets_log" ]]; then
  if grep -q "__consumer_offsets" "$offsets_log"; then
    pass "__consumer_offsets 토픽 존재 캡처됨"
  else
    fail "__consumer_offsets 토픽 존재 캡처됨" "$offsets_log 에 해당 문자열 없음"
  fi
else
  fail "__consumer_offsets 토픽 존재 캡처됨" "$offsets_log 없음"
fi

# -----------------------------------------------------------
# Summary
# -----------------------------------------------------------
section "Summary"

printf "통과: %s/%s\n" "$PASS_COUNT" "$TOTAL"

if [[ "$PASS_COUNT" -ge 12 ]]; then
  printf "${C_GREEN}PASS${C_RESET} (12점 이상)\n"
  exit 0
else
  printf "${C_RED}FAIL${C_RESET} (12점 미만)\n"
  printf "\n부족 항목:\n"
  for r in "${FAIL_REASONS[@]}"; do
    printf "  - %s\n" "$r"
  done
  exit 1
fi
