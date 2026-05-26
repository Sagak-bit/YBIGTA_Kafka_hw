# Kafka Lab (26-1 DE Session)

발제 자료(`KAFKA.pdf`)에서 다룬 카프카 동작을 직접 손으로 검증하는 실습입니다. 3-broker KRaft 클러스터를 Docker 로 띄우고 Java producer/consumer 로 5개 실험을 수행한 뒤 자동 채점기로 통과 여부를 확인합니다.

**마감: 5/26 세션 전까지**

## 학습 목표

- 같은 key 를 가진 메시지가 정말 같은 파티션으로 들어가는지
- Consumer Group 안에서 partition assignment 가 어떻게 분배되고 재분배되는지
- Broker 가 죽으면 ISR 과 Leader 가 어떻게 변하는지, 그동안 producer/consumer 는 어떻게 행동하는지
- `acks` 와 `min.insync.replicas` 조합이 가용성과 내구성에 미치는 영향
- Consumer offset 이 어떻게 보관되고 `auto.offset.reset` 이 언제 동작하는지

## 사전 준비

| 도구 | 권장 버전 | 비고 |
|---|---|---|
| Docker Desktop / Docker Engine | 최신 | Compose v2 필요 |
| Java (JDK) | 17 이상 | `JAVA_HOME` 설정 |
| Gradle | 8 이상 | 없으면 IntelliJ 가 wrapper 자동 생성 |
| Shell | bash | Windows 는 WSL 또는 Git Bash |

Java/Gradle 이 없다면 [SDKMAN](https://sdkman.io/) 이 가장 쉽습니다.

```bash
sdk install java 17.0.12-tem
sdk install gradle 8.9
```

## 시작하기

```bash
# 1. 클러스터 띄우기 (3-broker KRaft + Kafka UI)
docker compose up -d

# 2. 상태 확인 (4개 모두 Up)
docker compose ps

# 3. Java 코드 빌드
gradle build

# 4. artifacts 폴더 만들기 (실험 결과 저장 위치)
mkdir -p artifacts
```

이후 [`ASSIGNMENT.md`](./ASSIGNMENT.md) 의 PHASE 2 ~ 7 을 순서대로 따라가면 됩니다.

## 자동 채점

각 실험 결과를 `artifacts/` 폴더에 저장한 뒤 다음을 실행합니다.

```bash
./judge.sh
```

14개 체크 항목 중 12개 이상 통과하면 `PASS` 입니다.

```
== PHASE 1: 클러스터 부트스트랩 ==
[ok]   broker 3개 running
[ok]   kafka-1 API 응답
...
== Summary ==
통과: 13/14
PASS (12점 이상)
```

## 제출 방법

1. `./judge.sh` 가 PASS 로 끝나는지 확인
2. `REPORT.md` 채우기 (이름, 소요 시간, judge 점수, 각 실험 답변)
3. PR 올리기

```bash
git checkout -b submission/<본인이름>
git add REPORT.md artifacts/
git commit -m "feat: 26-1 Kafka lab 제출 (<본인이름>)"
git push origin submission/<본인이름>
```

PR 본문에 다음을 포함합니다.

- `./judge.sh` 실행 결과 캡처 (또는 텍스트)
- 본인 소요 시간 (PHASE 0 시작부터 PHASE 7 까지)

## 리더보드

제출이 머지될 때마다 아래 표가 업데이트됩니다.

_마지막 업데이트: 2026-05-26_

| 순위 | 이름 | 총점 | judge 점수 | 소요 시간 | 비고 |
|---|---|---|---|---|---|
| T1 | 지호 | 100 / 100 | 14 / 14 | 30분 | PR 본문에 judge 출력·소요시간 모두 포함 |
| T1 | 박정현 | 100 / 100 | 14 / 14 | 30분 | 발제 슬라이드 인용·환경 메모까지 풍부 |
| 3 | 오유림 | 99.5 / 100 | 12 / 14 | 26분 | judge 임계값 통과, 실험 1 sticky partitioner 설명 일부 부정확 |
| 4 | 신영군 | 95 / 100 | 14 / 14 | 약 1시간 | PR 본문에 소요 시간 누락 |
| 5 | 박준범 | 95 / 100 | 14 / 14 | 약 3시간 | PR 본문에 소요 시간 누락 |

## 디렉토리 구조

```
kafka-lab/
├── README.md
├── ASSIGNMENT.md           PHASE 별 실험 가이드
├── REPORT.md               보고서 템플릿
├── judge.sh                자동 채점기
├── docker-compose.yml      3-broker KRaft 클러스터
├── build.gradle.kts
├── settings.gradle.kts
├── src/main/java/lab/
│   ├── App.java
│   ├── SimpleProducer.java
│   └── SimpleConsumer.java
├── scripts/
│   ├── create-topic.sh
│   ├── describe-topic.sh
│   └── kill-broker.sh
└── artifacts/              실험 결과 저장 (학생이 채움)
```

## 마지막에 꼭 할 일

과제가 끝나면 컨테이너와 볼륨을 정리합니다.

```bash
docker compose down -v
```
