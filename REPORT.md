# Kafka Lab 실험 보고서

**이름**: 박정현
**소요 시간**: 약 30분 (PHASE 0 ~ PHASE 7)
**`./judge.sh` 점수**: 14 / 14

각 실험의 결과 캡처와 짧은 해석을 채우세요. 발제 자료(`KAFKA.pdf`) 챕터를 인용하면 좋습니다.

## 실험 1. Topic, Partition, Key 해싱

**1-1.** `alice` 와 `bob` 의 파티션 번호.

- `alice` → **partition 0** (`artifacts/exp1-alice.log`: 5건 모두 `partition=0, offset=0..4`)
- `bob`   → **partition 0** (`artifacts/exp1-bob.log`: 5건 모두 `partition=0, offset=5..9`)

`partitioning-demo` 토픽은 partition 3개(`0, 1, 2`)로 만들었고, 두 키 모두 murmur2 해시 결과가 partition 0 에 떨어졌다. 다른 두 키가 같은 파티션에 가는 것은 충돌일 뿐 비정상이 아니다.

**1-2.** 같은 key 가 항상 같은 파티션으로 가는 이유.

발제 2장 *Partition* 슬라이드대로, Kafka 의 기본 파티셔너는 key 가 있을 때 `murmur2(key.bytes()) % numPartitions` 로 파티션을 결정한다. murmur2 는 결정적(deterministic) 해시이므로 같은 입력에 항상 같은 출력을 주고, 토픽의 파티션 수가 바뀌지 않는 한 같은 key 는 항상 같은 partition 으로 라우팅된다. 덕분에 “같은 user_id 의 이벤트는 같은 partition → 순서 보장” 이 가능하다.

## 실험 2. Consumer Group 과 Rebalancing

**2-1.** 컨슈머 C1, C2, C3 (모두 `g1`) 이 받은 파티션.

CooperativeStickyAssignor 가 점진적으로(incremental) revoke / assign 을 반복하므로 로그에는 여러 줄이 찍히지만, 3개가 모두 join 한 뒤의 **최종 상태**는 다음과 같다.

| Consumer | 받은 partition |
|---|---|
| C1 | `cg-demo-0` |
| C2 | `cg-demo-2` |
| C3 | `cg-demo-1` |

로그 추적:
- C1 단독 → `assigned = [cg-demo-0, cg-demo-1, cg-demo-2]`
- C2 합류 → C1 `revoked = [cg-demo-2]`, C2 `assigned = [cg-demo-2]`
- C3 합류 → C1 `revoked = [cg-demo-1]`, C3 `assigned = [cg-demo-1]`

**2-2.** C1 을 죽였을 때 REBALANCE 가 어떻게 일어났는지 한 줄.

C1 이 갖고 있던 `cg-demo-0` 만 다른 컨슈머(C2) 에게 추가 할당되고, C3 의 `cg-demo-1` 과 C2 의 기존 `cg-demo-2` 는 그대로 유지됐다 (`artifacts/exp2-c2.log` 마지막 줄: `assigned = [cg-demo-0]`). Cooperative Sticky 답게 “이미 잘 붙어있는 것은 안 흔드는” 모습.

## 실험 3. Replication, ISR, Leader Election

**3-1.** 브로커를 죽이기 **전** ISR 상태 (`artifacts/exp3-before.txt` 인용).

```
Partition: 0   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1
Partition: 1   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
Partition: 2   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
```

모든 파티션이 `replication.factor=3` 이고 ISR 도 3개로 가득 차 있다.

**3-2.** 죽인 **직후** ISR 상태 (`artifacts/exp3-after.txt` 인용). 어떤 컬럼이 바뀌었는지 한 줄.

```
Partition: 0   Leader: 3   Replicas: 2,3,1   Isr: 3,1
Partition: 1   Leader: 3   Replicas: 3,1,2   Isr: 3,1
Partition: 2   Leader: 1   Replicas: 1,2,3   Isr: 1,3
```

`kafka-2` 가 ISR 에서 빠지면서 모든 파티션의 **`Isr` 컬럼이 3개 → 2개로 줄었고**, 동시에 partition 0 의 **`Leader` 가 2 → 3 으로 교체**되었다. Replicas 자체(브로커 배치)는 그대로다.

**3-3.** Producer 가 메시지 40개를 손실 없이 보냈는지 (`exp3-producer.log` 의 마지막 offset 확인).

마지막 라인이 `sent topic=failover-demo partition=0 offset=39 ...` 로 끝났다. 0..39 = 40 건 전부 성공. 중간에 `WARN ... NOT_LEADER_OR_FOLLOWER, retrying` 가 찍힌 뒤 metadata 갱신 → 다음 시도부터 새 leader(3) 로 정상 전송됐다.

**3-4.** 손실이 없었다면 왜 그런지 두세 줄. `acks=all` + `min.insync.replicas` + `replication.factor=3` 의 보호 메커니즘 관점에서.

- `acks=all` 이면 leader 가 메시지를 받자마자 ack 하는 것이 아니라, **현재 ISR 의 모든 follower 가 복제를 완료한 뒤에야** ack 한다. 즉 ack 받은 메시지는 ISR 안의 모든 노드에 이미 들어가 있다.
- `replication.factor=3` 이고 broker 1 개가 죽어도 ISR 에 여전히 2 개가 남으므로 (`min.insync.replicas=1` 도 만족), kill 직전까지 ack 된 모든 메시지는 살아남은 follower 중 하나가 이미 갖고 있다. 컨트롤러는 그 follower 를 새 leader 로 승격시키기만 하면 된다 → 손실 0.
- 추가로 producer 의 `enable.idempotence=true` 가 retry 시 중복 produce 도 막아주므로, leader 교체 중 `NOT_LEADER_OR_FOLLOWER` 재시도가 일어나도 같은 메시지가 두 번 쓰이지 않는다.

발제 6장 “브로커 장애” 슬라이드의 권장 조합 `replication.factor=3 + min.insync.replicas=2 + acks=all` 의 약한 버전 (`min.insync.replicas=1`) 을 그대로 검증한 셈이다.

## 실험 4. acks 와 min.insync.replicas

**4-1.** broker 2개를 죽인 상태에서 `acks=all` 시도 시 발생한 예외 클래스명과 메시지.

```
FAILED i=0 key=k1 cause=org.apache.kafka.common.errors.NotEnoughReplicasException
                  msg=Messages are rejected since there are fewer in-sync replicas than required.
FAILED i=1 key=k1 cause=org.apache.kafka.common.errors.NotEnoughReplicasException ...
FAILED i=2 key=k1 cause=org.apache.kafka.common.errors.NotEnoughReplicasException ...
```

`safety-demo` 는 `min.insync.replicas=2` 로 만들었는데 broker 2 개를 죽이면 ISR 이 leader(`kafka-1`) 1 개만 남는다. 1 < 2 이므로 broker 가 produce 자체를 거부한다.

**4-2.** `acks=1` 으로 바꿨을 때 성공했다면, ISR 정의 관점에서 왜 그런지 한 줄.

`acks=1` 은 **leader 한 명에게만 쓰면** 끝이고 ISR 크기를 확인하지 않는다. `min.insync.replicas` 는 `acks=all` 일 때만 검사되는 게이트라서 leader 하나만 살아있으면 그냥 통과한다 — 단, 직후에 leader 가 죽으면 그 메시지는 follower 에 복제되기 전이라 **유실 가능**. 가용성을 얻은 대신 내구성을 잃는 트레이드.

## 실험 5. Consumer Offset 과 auto.offset.reset

**5-1.** 같은 그룹으로 두 번째 consume 했을 때 받은 메시지 수와 그 이유.

- 1차 consume (`g-earliest`): `recv` 라인 10건 (전체 메시지).
- 2차 consume (같은 `g-earliest`): `recv` 라인 **0건**.

이유: 1차 실행 중 `enable.auto.commit=true` 가 5초 주기로 마지막 처리 오프셋을 `__consumer_offsets` 토픽에 기록한다. 2차 실행에서 같은 group id 로 join 하면 broker(Group Coordinator) 가 저장된 오프셋을 돌려주고, 컨슈머는 그 다음 위치(=현재 high watermark)부터 polling 한다. `auto.offset.reset=earliest` 는 **저장된 오프셋이 없을 때만** 적용되므로 재실행에는 영향이 없다.

**5-2.** Consumer offset 이 저장되는 토픽 이름.

`__consumer_offsets` (`artifacts/exp5-offsets.log` 에 토픽 목록 캡처). 내부 토픽이며, 본 클러스터에서는 `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3` 으로 설정돼 있어 안정적으로 복제된다.

## 자유 회고 (선택)

가장 인상 깊었던 건 **실험 3 (Leader Election)**. broker 를 죽이는 순간 producer 로그에 `NOT_LEADER_OR_FOLLOWER` 가 떴다가, metadata 가 자동 갱신되고 새 leader 로 곧바로 재시도해서 40 개 메시지가 전부 전송된 흐름이 발제에서 본 “리더 선출로 무중단 복구” 한 줄을 그대로 눈으로 확인하는 느낌이었다. `acks=all + idempotence` 조합이 운영 권장인 이유도 함께 체감.
