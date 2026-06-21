# Kafka Lab 실험 보고서

**이름**: 박준범
**소요 시간**: 약 3시간
**`./judge.sh` 점수**: 14 / 14

각 실험의 결과 캡처와 짧은 해석을 채우세요. 발제 자료(`KAFKA.pdf`) 챕터를 인용하면 좋습니다.

## 실험 1. Topic, Partition, Key 해싱

**1-1.** `alice` 와 `bob` 의 파티션 번호.

- alice → partition **0**
- bob → partition **0**

**1-2.** 같은 key 가 항상 같은 파티션으로 가는 이유.

Kafka producer는 key를 murmur2 해시한 뒤 파티션 수로 나머지 연산해서 파티션을 결정한다. 같은 key는 항상 같은 해시값이 나오므로 파티션 수가 안 바뀌는 한 항상 같은 파티션으로 간다. alice랑 bob은 서로 다른 key인데 `murmur2("alice") % 3`이랑 `murmur2("bob") % 3`이 우연히 둘 다 0이 나온 것이다.

## 실험 2. Consumer Group 과 Rebalancing

**2-1.** 컨슈머 C1, C2, C3 (모두 `g1`) 이 받은 파티션.

| Consumer | 받은 partition |
|---|---|
| C1 | cg-demo-0 |
| C2 | cg-demo-2 |
| C3 | cg-demo-1 |

**2-2.** C1 을 죽였을 때 REBALANCE 가 어떻게 일어났는지 한 줄.

C1이 죽자 Group Coordinator가 리밸런싱을 트리거했고, C1이 갖고 있던 cg-demo-0이 C2에 재분배됐다. 컨슈머가 들어오거나 나올 때마다 파티션을 다시 나눠갖는 구조다.

## 실험 3. Replication, ISR, Leader Election

**3-1.** 브로커를 죽이기 **전** ISR 상태 (`artifacts/exp3-before.txt` 인용).

```
Partition: 0  Leader: 1  Isr: 1,2,3
Partition: 1  Leader: 2  Isr: 2,3,1
Partition: 2  Leader: 3  Isr: 3,1,2
```

**3-2.** 죽인 **직후** ISR 상태 (`artifacts/exp3-after.txt` 인용). 어떤 컬럼이 바뀌었는지 한 줄.

```
Partition: 0  Leader: 2  Isr: 2,3
Partition: 1  Leader: 2  Isr: 2,3
Partition: 2  Leader: 3  Isr: 3,2
```

Leader가 1→2로 바뀌고, ISR에서 브로커 1이 빠져서 3개→2개로 줄었다.

**3-3.** Producer 가 메시지 40개를 손실 없이 보냈는지 (`exp3-producer.log` 의 마지막 offset 확인).

마지막 라인 `offset=39` 확인 → 40개 전송 완료, 손실 없음.

**3-4.** 손실이 없었다면 왜 그런지 두세 줄. `acks=all` + `min.insync.replicas` + `replication.factor=3` 의 보호 메커니즘 관점에서.

리더가 죽는 순간 아주 짧게 전송 실패가 생기지만, producer의 `retries=5` 덕분에 새 리더가 선출될 때까지 재시도해서 결국 성공한다. `replication.factor=3`으로 복제본이 3개 있어서 리더가 죽어도 팔로워가 데이터를 갖고 있고, `acks=all`로 모든 ISR이 받은 메시지만 커밋되기 때문에 이미 전송된 메시지는 날아가지 않는다.

## 실험 4. acks 와 min.insync.replicas

**4-1.** broker 2개를 죽인 상태에서 `acks=all` 시도 시 발생한 예외 클래스명과 메시지.

```
cause=org.apache.kafka.common.errors.NotEnoughReplicasException
msg=Messages are rejected since there are fewer in-sync replicas than required.
```

**4-2.** `acks=1` 으로 바꿨을 때 성공했다면, ISR 정의 관점에서 왜 그런지 한 줄.

`acks=1`은 리더 1개만 확인하고 끝내기 때문에 `min.insync.replicas=2` 설정을 무시한다. ISR이 1개뿐이어도 리더가 살아있으면 전송이 성공한다. 내구성 보장을 우회하는 셈이라 데이터 손실 위험이 생기는 tradeoff다.

## 실험 5. Consumer Offset 과 auto.offset.reset

**5-1.** 같은 그룹으로 두 번째 consume 했을 때 받은 메시지 수와 그 이유.

메시지 0개. 메시지가 사라진 게 아니라, 첫 번째 consume 후 읽은 위치(offset)가 `__consumer_offsets`에 커밋된다. 두 번째 실행하면 그 기록을 보고 커밋된 offset 다음부터 읽으려 하는데, 이미 마지막 메시지까지 읽은 상태라 offset이 끝을 가리키고 있어서 아무것도 못 받는다.

**5-2.** Consumer offset 이 저장되는 토픽 이름.

`__consumer_offsets`

## 자유 회고 (선택)

`acks=1`로 바꿨더니 `min.insync.replicas=2` 설정이 있는데도 전송이 성공했다. producer의 `acks`와 토픽의 `min.insync.replicas`가 함께 맞아야 데이터 내구성 보장이 제대로 동작한다는 걸 직접 확인한 게 인상 깊었다.
