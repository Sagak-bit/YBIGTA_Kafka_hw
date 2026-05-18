# 실습

3-broker Kafka 클러스터를 Docker로 띄우고, Java producer/consumer로 발제 내용을 직접 손으로 검증해봅시다.

각 PHASE는 명령어와 "이게 보이면 성공" 기준을 함께 제공합니다. 각 실험의 결과는 `artifacts/` 폴더에 텍스트로 저장하고, 마지막에 `./judge.sh` 로 자동 채점합니다.

```
PHASE 0  사전 준비
PHASE 1  클러스터 부트스트랩
PHASE 2  파티셔닝 & Key 해싱        (Experiment 1)
PHASE 3  Consumer Group & Rebalancing (Experiment 2)
PHASE 4  Leader Election             (Experiment 3) ⭐
PHASE 5  acks & min.insync.replicas  (Experiment 4)
PHASE 6  Consumer Offset             (Experiment 5)
PHASE 7  자동 채점 & 제출
```

마감: **5/26 세션 전까지**

# PHASE 0: 사전 준비

## 1. Docker Desktop / Docker Engine

`docker --version` 과 `docker compose version` 이 정상 출력되면 성공.

## 2. Java 17 + Gradle 8

[SDKMAN](https://sdkman.io/) 추천.

```bash
sdk install java 17.0.12-tem
sdk install gradle 8.9
```

`java -version` 과 `gradle --version` 이 정상 출력되면 성공.

## 3. 저장소 클론

```bash
git clone <세션에서 안내한 저장소>
cd kafka-lab
```

## 4. (Windows 사용자) Shell

Windows라면 **WSL** 또는 **Git Bash** 에서 진행합니다.

# PHASE 1: 클러스터 부트스트랩

## 1. 클러스터 띄우기

```bash
docker compose up -d
docker compose ps
```

다음과 같이 4개 컨테이너가 `Up` 상태이면 성공.

```
NAME       STATUS         PORTS
kafka-1    Up             0.0.0.0:9092->9092/tcp
kafka-2    Up             0.0.0.0:9094->9094/tcp
kafka-3    Up             0.0.0.0:9096->9096/tcp
kafka-ui   Up             0.0.0.0:8080->8080/tcp
```

브라우저로 `http://localhost:8080` 접속하면 Kafka UI도 볼 수 있습니다 (선택).

## 2. Java 코드 빌드

```bash
gradle build
```

`BUILD SUCCESSFUL` 이 보이면 성공. (Wrapper가 없다면 `gradle wrapper` 한 번 실행)

## 3. artifacts 폴더 생성

이후 단계에서 결과 로그를 여기에 저장합니다.

```bash
mkdir -p artifacts
```

# PHASE 2: 파티셔닝 & Key 해싱 (Experiment 1)

발제 2장 Partition / Offset 슬라이드.

같은 key가 정말 같은 파티션으로 가는지 직접 확인합니다.

## 1. 토픽 생성

```bash
./scripts/create-topic.sh partitioning-demo 3 3
```

마지막 줄에 다음 같은 라인 3개가 보이면 성공.

```
Topic: partitioning-demo  Partition: 0  Leader: ...  Replicas: ...  Isr: ...
```

## 2. 같은 key 로 메시지 보내기

```bash
gradle run --args="produce partitioning-demo alice 5" | tee artifacts/exp1-alice.log
gradle run --args="produce partitioning-demo bob 5"   | tee artifacts/exp1-bob.log
```

`artifacts/exp1-alice.log` 안에 `sent topic=partitioning-demo partition=X offset=N key=alice value=...` 라인이 5개 모두 같은 `partition=X` 로 적혀 있으면 성공.

## 3. REPORT 1번 항목 작성

- `alice` / `bob` 의 파티션 번호
- 같은 key가 같은 파티션으로 가는 이유

# PHASE 3: Consumer Group & Rebalancing (Experiment 2)

발제 2장 Consumer Group / 6장 Rebalancing.

Consumer Group 안에서 partition assignment가 어떻게 분배되는지 관찰합니다.

## 1. 토픽 + 메시지 채워두기

```bash
./scripts/create-topic.sh cg-demo 3 3
gradle run --args="produce cg-demo seed 30"
```

## 2. 같은 그룹으로 컨슈머 3개 띄우기

각각 다른 터미널에서 실행합니다.

```bash
# 터미널 1
gradle run --args="consume cg-demo g1" | tee artifacts/exp2-c1.log

# 터미널 2
gradle run --args="consume cg-demo g1" | tee artifacts/exp2-c2.log

# 터미널 3
gradle run --args="consume cg-demo g1" | tee artifacts/exp2-c3.log
```

각 로그에 `REBALANCE [consumer-...] assigned = [demo-X]` 라인이 보이면 성공.

## 3. 컨슈머 1번을 Ctrl+C 로 죽이기

남은 두 컨슈머의 로그에 `REBALANCE` 가 한 번 더 찍히고, 죽은 컨슈머의 파티션이 재분배되면 성공.

종료된 컨슈머 1번 로그는 그대로 `artifacts/exp2-c1.log` 로 남깁니다.

## 4. REPORT 2번 항목 작성

# PHASE 4: Leader Election (Experiment 3) ⭐

발제 2장 Replication / ISR, 6장 브로커 장애.

리더 브로커를 강제로 죽이고 ISR과 새 리더가 어떻게 결정되는지 관찰합니다. **과제의 핵심**입니다.

## 1. 토픽 생성 후 초기 상태 캡처

```bash
./scripts/create-topic.sh failover-demo 3 3
./scripts/describe-topic.sh failover-demo | tee artifacts/exp3-before.txt
```

`artifacts/exp3-before.txt` 의 각 파티션 `Isr` 컬럼에 broker id 3개가 모두 들어 있으면 성공.

## 2. Producer 를 백그라운드로 흘리기 (acks=all + idempotence)

```bash
ACKS=all IDEMPOTENCE=true gradle run --args="produce failover-demo steady 40 500" \
  | tee artifacts/exp3-producer.log &
```

(`500` 은 메시지당 sleep ms. 40개 × 0.5초 = 20초 동안 흘러갑니다.)

## 3. 파티션 0 의 Leader 브로커 죽이기

먼저 누가 파티션 0 의 leader 인지 확인합니다.

```bash
./scripts/describe-topic.sh failover-demo | grep "Partition: 0"
```

`Leader: 2` 였다면:

```bash
./scripts/kill-broker.sh kafka-2
./scripts/describe-topic.sh failover-demo | tee artifacts/exp3-after.txt
```

`artifacts/exp3-after.txt` 에서 파티션 0 의 Leader 가 다른 broker 로 바뀌고, ISR 컬럼이 2개로 줄어 있으면 성공.

## 4. Producer 완료 대기

```bash
wait
tail -5 artifacts/exp3-producer.log
```

마지막 `sent ... offset=39` 라인이 보이면 손실 없이 끝난 것입니다.

## 5. 브로커 되살리기

```bash
docker compose start kafka-2
sleep 10
./scripts/describe-topic.sh failover-demo
```

ISR 컬럼이 다시 3개로 복귀하면 성공.

## 6. REPORT 3번 항목 작성

# PHASE 5: acks & min.insync.replicas (Experiment 4)

발제 3장 Producer 설정 / Broker 설정 권장 조합.

내구성과 가용성의 트레이드오프를 직접 깨뜨려봅니다.

## 1. 안전 강화 토픽 생성

```bash
./scripts/create-topic.sh safety-demo 1 3 min.insync.replicas=2
```

## 2. 정상 상황에서 acks=all 시도

```bash
ACKS=all gradle run --args="produce safety-demo k1 3"
```

성공 (sent 라인 3개) 이 정상.

## 3. 브로커 2개 죽이기

```bash
./scripts/kill-broker.sh kafka-2
./scripts/kill-broker.sh kafka-3
```

## 4. 같은 produce 다시 시도

```bash
ACKS=all gradle run --args="produce safety-demo k1 3" 2>&1 | tee artifacts/exp4-acks-all.log
```

`NotEnoughReplicasException` 또는 유사한 에러가 로그에 보이면 성공.

## 5. acks=1 로 재시도

```bash
ACKS=1 IDEMPOTENCE=false gradle run --args="produce safety-demo k1 3" 2>&1 \
  | tee artifacts/exp4-acks-1.log
```

`sent` 라인이 3개 보이면 성공.

## 6. 브로커 복구

```bash
docker compose start kafka-2 kafka-3
sleep 10
```

## 7. REPORT 4번 항목 작성

# PHASE 6: Consumer Offset (Experiment 5)

발제 2장 Offset / 3장 auto.offset.reset.

## 1. 토픽 + 메시지 채우기

```bash
./scripts/create-topic.sh offset-demo 1 3
gradle run --args="produce offset-demo init 10"
```

## 2. earliest 그룹으로 첫 소비

다른 터미널에서:

```bash
OFFSET_RESET=earliest gradle run --args="consume offset-demo g-earliest"
```

10개 메시지를 다 받았는지 확인 후 Ctrl+C.

## 3. 같은 group 으로 재실행

```bash
OFFSET_RESET=earliest gradle run --args="consume offset-demo g-earliest"
```

이미 커밋된 메시지는 받지 않아야 정상. 5초 정도 기다린 뒤 Ctrl+C.

## 4. `__consumer_offsets` 토픽 존재 확인

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --list \
  | tee artifacts/exp5-offsets.log
```

`__consumer_offsets` 가 출력에 포함되어 있으면 성공.

## 5. REPORT 5번 항목 작성

# PHASE 7: 자동 채점 & 제출

## 1. 자동 채점

```bash
./judge.sh
```

다음과 비슷한 출력이 보입니다.

```
[Kafka Lab Judge]
[ok] PHASE 1   docker cluster (3 brokers + ui)
[ok] PHASE 2   partitioning-demo topic
[ok] PHASE 2   alice key all to same partition
...
Score: 13/14   PASS
```

**12점 이상이면 통과**입니다. PASS 라인이 보이도록 부족한 항목을 보완합니다.

## 2. 클러스터 정리

```bash
docker compose down -v
```

## 3. REPORT.md 마무리

`REPORT.md` 상단의 다음 정보를 채워 넣습니다.

- 이름
- 소요 시간 (PHASE 0 시작부터 PHASE 7 까지)
- judge 점수

## 4. PR 제출

```bash
git checkout -b submission/<본인이름>
git add REPORT.md artifacts/
git commit -m "feat: 26-1 Kafka lab 제출 (<본인이름>)"
git push origin submission/<본인이름>
```

PR 본문에 다음을 포함합니다.

- `./judge.sh` 실행 결과 캡처
- 본인 소요 시간

PR 머지 후 README 의 리더보드 표가 업데이트됩니다.

# 채점 기준

| 항목 | 비중 |
|---|---|
| `./judge.sh` 12점 이상 (자동) | 60% |
| REPORT.md 각 문항 답변이 발제 내용과 연결됨 | 30% |
| PR 본문에 judge 결과 캡처 + 소요 시간 포함 | 10% |

# 트러블슈팅

**클러스터가 안 떠요**: `docker compose down -v` 로 완전 정리 후 다시 `up -d`.

**`gradle wrapper` 없다는 에러**: 저장소 루트에서 `gradle wrapper` 한 번 실행 후 `./gradlew` 사용.

**컨슈머가 메시지를 못 받아요**: `OFFSET_RESET=earliest` 환경변수를 붙여서 처음부터 읽도록 시도. 그래도 안 받으면 토픽에 메시지가 실제로 있는지 `kafka-ui` (http://localhost:8080) 로 확인.

**Windows Git Bash 에서 docker exec 경로 에러**: 스크립트 첫 줄에 `export MSYS_NO_PATHCONV=1` 가 이미 들어있으므로 그대로 실행하면 됩니다. 직접 docker exec 을 칠 때만 같은 변수를 환경에 미리 export.
