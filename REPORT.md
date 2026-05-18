# Kafka Lab 실험 보고서

**이름**:
**소요 시간**: (PHASE 0 시작부터 PHASE 7 까지)
**`./judge.sh` 점수**: / 14

각 실험의 결과 캡처와 짧은 해석을 채우세요. 발제 자료(`KAFKA.pdf`) 챕터를 인용하면 좋습니다.

## 실험 1. Topic, Partition, Key 해싱

**1-1.** `alice` 와 `bob` 의 파티션 번호.

**1-2.** 같은 key 가 항상 같은 파티션으로 가는 이유.

## 실험 2. Consumer Group 과 Rebalancing

**2-1.** 컨슈머 C1, C2, C3 (모두 `g1`) 이 받은 파티션.

| Consumer | 받은 partition |
|---|---|
| C1 | |
| C2 | |
| C3 | |

**2-2.** C1 을 죽였을 때 REBALANCE 가 어떻게 일어났는지 한 줄.

## 실험 3. Replication, ISR, Leader Election

**3-1.** 브로커를 죽이기 **전** ISR 상태 (`artifacts/exp3-before.txt` 인용).

**3-2.** 죽인 **직후** ISR 상태 (`artifacts/exp3-after.txt` 인용). 어떤 컬럼이 바뀌었는지 한 줄.

**3-3.** Producer 가 메시지 40개를 손실 없이 보냈는지 (`exp3-producer.log` 의 마지막 offset 확인).

**3-4.** 손실이 없었다면 왜 그런지 두세 줄. `acks=all` + `min.insync.replicas` + `replication.factor=3` 의 보호 메커니즘 관점에서.

## 실험 4. acks 와 min.insync.replicas

**4-1.** broker 2개를 죽인 상태에서 `acks=all` 시도 시 발생한 예외 클래스명과 메시지.

```
(여기에 캡처)
```

**4-2.** `acks=1` 으로 바꿨을 때 성공했다면, ISR 정의 관점에서 왜 그런지 한 줄.

## 실험 5. Consumer Offset 과 auto.offset.reset

**5-1.** 같은 그룹으로 두 번째 consume 했을 때 받은 메시지 수와 그 이유.

**5-2.** Consumer offset 이 저장되는 토픽 이름.

## 자유 회고 (선택)

가장 인상 깊었던 실험과 그 이유를 한두 줄로.
