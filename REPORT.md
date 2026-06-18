# Kafka Lab 실험 보고서

**이름**: 신영군  
**소요 시간**: 약 1시간(PHASE 0 시작부터 PHASE 7 까지)  
**`./judge.sh` 점수**: 14 / 14

각 실험의 결과 캡처와 짧은 해석을 채우세요. 발제 자료(`KAFKA.pdf`) 챕터를 인용하면 좋습니다.

## 실험 1. Topic, Partition, Key 해싱

**1-1.** `alice` 와 `bob` 의 파티션 번호.
![](captures/exp1_alice.png)
![](captures/exp1_bob.png)
Alice and Bob's data all goes to partition 0, but different offsets

**1-2.** 같은 key 가 항상 같은 파티션으로 가는 이유.  
Since producer hashes the key with the same hashing algorithm, same key value will always goes to the same partition.

## 실험 2. Consumer Group 과 Rebalancing

**2-1.** 컨슈머 C1, C2, C3 (모두 `g1`) 이 받은 파티션.

| Consumer | 받은 partition |
| -------- | -------------- |
| C1       | 0              |
| C2       | 1 (0)          |
| C3       | 2              |

**2-2.** C1 을 죽였을 때 REBALANCE 가 어떻게 일어났는지 한 줄.
![](captures/exp2_c1.png)
![](captures/exp2_c2.png)
![](captures/exp2_c3.png)
We can observe that when we terminated c1, c2 was being assigned to the demo-0, which was previously assigned to c1.

## 실험 3. Replication, ISR, Leader Election

**3-1.** 브로커를 죽이기 **전** ISR 상태 (`artifacts/exp3-before.txt` 인용).
\*displayed in the image below
**3-2.** 죽인 **직후** ISR 상태 (`artifacts/exp3-after.txt` 인용). 어떤 컬럼이 바뀌었는지 한 줄.
![](captures/exp3.png)
After killing broker 2, it was removed from the ISR of all partitions, reducing the ISR replica count from 3 to 2 (e.g., Isr: 1,3). Additionally, the Leader of Partition 1 changed from 2 to 3.

**3-3.** Producer 가 메시지 40개를 손실 없이 보냈는지 (`exp3-producer.log` 의 마지막 offset 확인).
40 messages are sent without any data loss

**3-4.** 손실이 없었다면 왜 그런지 두세 줄. `acks=all` + `min.insync.replicas` + `replication.factor=3` 의 보호 메커니즘 관점에서.
With replication.factor=3, every message is replicated across 3 brokers. acks=all ensures the producer only considers a message committed after all ISR replicas acknowledge it, so no acknowledged message can be lost even if a broker crashes. When one broker was killed, the ISR shrank from 3 to 2, but since min.insync.replicas=1 was still satisfied, the producer retried and continued sending without any data loss.

## 실험 4. acks 와 min.insync.replicas

**4-1.** broker 2개를 죽인 상태에서 `acks=all` 시도 시 발생한 예외 클래스명과 메시지.
![](captures/exp4_all.png)
The exception name is NotEnoughReplicasException and the message is NotEnoughReplicasException: Not enough in-sync replicas match the required consistency level, including replicas 0, 1, 2 with insync replicas 0.

**4-2.** `acks=1` 으로 바꿨을 때 성공했다면, ISR 정의 관점에서 왜 그런지 한 줄.
![](captures/exp4_1.png)
With acks=1, the producer only waits for the leader to acknowledge the message, so as long as there is a leader, the producer can send messages without waiting for ISR replication.

## 실험 5. Consumer Offset 과 auto.offset.reset

**5-1.** 같은 그룹으로 두 번째 consume 했을 때 받은 메시지 수와 그 이유.
![](captures/exp5.png)
We can observe that when we consume for the second time, we receive 0 messages, because we have already consumed all the messages in the first time.

**5-2.** Consumer offset 이 저장되는 토픽 이름.
The offset is stored in the `__consumer_offsets` topic.

## 자유 회고 (선택)

가장 인상 깊었던 실험과 그 이유를 한두 줄로.
