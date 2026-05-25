# Kafka Lab 실험 보고서

**이름**: 지호
**소요 시간**: 약 30분  
**`./judge.sh` 점수**: 14 / 14

각 실험은 `artifacts/` 폴더의 로그로 캡처했습니다.

## 실험 1. Topic, Partition, Key 해싱

**1-1.** `alice` 와 `bob` 의 파티션 번호.

- `alice`: partition 0 (`artifacts/exp1-alice.log`)
- `bob`: partition 0 (`artifacts/exp1-bob.log`)

**1-2.** 같은 key 가 항상 같은 파티션으로 가는 이유.

Kafka producer는 key가 있는 record에 대해 key의 serialized bytes를 hash하고, 그 hash 값을 토픽의 partition 수로 나누어 partition을 결정한다. 따라서 토픽의 partition 수가 유지되는 동안 같은 key는 같은 hash 결과를 사용하므로 같은 partition으로 들어간다.

## 실험 2. Consumer Group 과 Rebalancing

**2-1.** 컨슈머 C1, C2, C3 (모두 `g1`) 이 받은 파티션.

| Consumer | 받은 partition |
| -------- | -------------- |
| C1       | `cg-demo-0`    |
| C2       | `cg-demo-2`    |
| C3       | `cg-demo-1`    |

**2-2.** C1 을 죽였을 때 REBALANCE 가 어떻게 일어났는지 한 줄.

C1이 가진 partition이 revoke되고, 남은 consumer에게 재분배되었다. 로그에서는 C2가 추가로 `cg-demo-0`을 assigned 받아 C1의 partition을 이어받았다.

## 실험 3. Replication, ISR, Leader Election

**3-1.** 브로커를 죽이기 **전** ISR 상태 (`artifacts/exp3-before.txt` 인용).

세 partition 모두 ISR에 broker 3개가 있었다.

```text
Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

**3-2.** 죽인 **직후** ISR 상태 (`artifacts/exp3-after.txt` 인용). 어떤 컬럼이 바뀌었는지 한 줄.

partition 0의 leader가 `1`에서 `2`로 바뀌었고, 죽은 broker 1이 ISR에서 빠져 각 partition의 ISR이 2개로 줄었다.

```text
Partition: 0  Leader: 2  Replicas: 1,2,3  Isr: 2,3
Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3
Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,2
```

**3-3.** Producer 가 메시지 40개를 손실 없이 보냈는지 (`exp3-producer.log` 의 마지막 offset 확인).

손실 없이 전송됐다. 마지막 로그가 `offset=39`이고 `sent` 라인은 총 40개였다.

**3-4.** 손실이 없었다면 왜 그런지 두세 줄. `acks=all` + `min.insync.replicas` + `replication.factor=3` 의 보호 메커니즘 관점에서.

`replication.factor=3`이라 leader broker가 죽어도 ISR에 남아 있던 replica 중 하나가 새 leader가 될 수 있었다. 이 토픽의 `min.insync.replicas`는 기본값 1이므로 broker 1개 장애 후에도 남은 ISR이 요구 조건을 만족했고, `acks=all` producer는 새 leader/ISR 기준으로 ack를 받은 record만 성공 처리했다.

## 실험 4. acks 와 min.insync.replicas

**4-1.** broker 2개를 죽인 상태에서 `acks=all` 시도 시 발생한 예외 클래스명과 메시지.

```text
FAILED i=0 key=k1 cause=org.apache.kafka.common.errors.NotEnoughReplicasException msg=Messages are rejected since there are fewer in-sync replicas than required.
```

**4-2.** `acks=1` 으로 바꿨을 때 성공했다면, ISR 정의 관점에서 왜 그런지 한 줄.

`acks=all`은 `min.insync.replicas=2`를 만족해야 하지만, `acks=1`은 leader 1개의 append ack만 기다리므로 ISR이 1개뿐인 상태에서도 `sent`가 성공했다.

## 실험 5. Consumer Offset 과 auto.offset.reset

**5-1.** 같은 그룹으로 두 번째 consume 했을 때 받은 메시지 수와 그 이유.

두 번째 consume에서 받은 메시지 수는 0개였다. 첫 번째 consume에서 `g-earliest` group의 offset이 commit되었기 때문에, 같은 group으로 다시 실행하면 `OFFSET_RESET=earliest`가 있어도 이미 commit된 offset 이후부터 읽는다.

**5-2.** Consumer offset 이 저장되는 토픽 이름.

`__consumer_offsets`

## 자유 회고 (선택)
