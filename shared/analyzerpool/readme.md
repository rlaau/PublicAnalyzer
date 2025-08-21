## AnalyzerPool
### 개념
4개의 모듈(CC / EE / CCE / EEC) 간 포트 메서드콜(읽기) + 이벤트 버스(커맨드/쿼리) + 공유 Tx 토픽 소비를 단일 브릿지로 제공하는 풀(Pool).

1. 의도: 모듈 간 직접 참조를 끊고, 강타입/고신뢰 방식으로 상호작용을 단순화

2. 디자인 핵심
    a. 포트(View)로 동기 읽기 메서드콜
    b. 타이핑된 SPSC 이벤트 버스(모듈당 1개, 내부는 pending + out 구조)
    c. 공유 Kafka 토픽 + 모듈별 그룹으로 Tx 팬아웃
    d. 시작/종료 시에만 JSONL 파일로 동기 백업(실행 중 디스크 I/O 없음)
    e. 모드(bool) 에 따라 저장 루트가 루트부터 분기

3. 무엇을 해결하나
    a. 모듈 간 의존 꼬임 제거: pool.GetViewCC().SomeRead() 식의 읽기 포트만 노출
    b. 커맨드/쿼리의 이벤트 통신 통일: pool.PublishToCC(evt) / for e := range pool.DequeueCC()
    c. 하나의 Tx 토픽에서 모듈별 소비자 그룹으로 분리: 각 모듈은 자기 채널에서 안전하게 소비

4. 재시작 복구: 종료 시 남은 이벤트만 JSONL에 저장, 시작 시 읽어와 채널에 재주입

### 아키텍처 개요
+------------------- AnalyzerPool -------------------+
| Ports (read-only views)                            |
|  - ccPort, eePort, ccePort, eecPort                |
|                                                    |
| Typed Event Buses (SPSC, JSONL at start/close)     |
|  - busCC  (PublishToCC / DequeueCC)                |
|  - busEE  (PublishToEE / DequeueEE)                |
|  - busCCE (PublishToCCE / DequeueCCE)              |
|  - busEEC (PublishToEEC / DequeueEEC)              |
|  ▶ 내부: pending 슬라이스(+ cond) + out 채널 1개    |
|                                                    |
| Tx Fan-out (shared topic, per-group consume)       |
|  - KafkaBatchConsumer[T] ×4 → ch{XX}Tx             |
|  - atomic counters: TxCountCC/EE/CCE/EEC           |
+----------------------------------------------------+

### 저장 경로(모드 기반 루트 분기)

테스트 모드(true): computation.FindTestingStorageRootPath()

프로덕션(false): computation.FindProductionStorageRootPath()

풀 루트: <ROOT>/analyzer_pool

이벤트 버스 파일: <ROOT>/analyzer_pool/eventbus/{cc|ee|cce|eec}.jsonl

시작 시 JSONL을 읽어 backlog 적재 → 즉시 삭제

종료 시 남은 pending만 JSONL로 동기 저장

모드는 type Mode = bool (true→test / false→prod). 토픽/그룹/브로커 등은 초기 자리값(testval)으로 채워두고 프로젝트에서 교체.

### 공개 API
API
```go
// 생성: 모드만 전달(직렬화는 JSON 고정)
CreateAnalyzerPoolFrame[CCEvt, EEEvt, CCEEvt, EECEvt, TX](isTest Mode) (*AnalyzerPool[...], error)

// 포트 등록 & 뷰 획득(읽기 메서드콜용)
Register(cc CCPort, ee EEPort, cce CCEPort, eec EECPort)
GetViewCC()  CCPort
GetViewEE()  EEPort
GetViewCCE() CCEPort
GetViewEEC() EECPort

// 이벤트 통신
PublishToCC(evt CCEvt)  error
PublishToEE(evt EEEvt)  error
PublishToCCE(evt CCEEvt) error
PublishToEEC(evt EECEvt) error

DequeueCC()  <-chan CCEvt
DequeueEE()  <-chan EEEvt
DequeueCCE() <-chan CCEEvt
DequeueEEC() <-chan EECEvt

// Tx 소비(공유 토픽, 그룹 분리)
ConsumeCCTx()  <-chan TX
ConsumeEETx()  <-chan TX
ConsumeCCETx() <-chan TX
ConsumeEECTx() <-chan TX

// 메트릭
TxCountCC()  uint64
TxCountEE()  uint64
TxCountCCE() uint64
TxCountEEC() uint64

// 종료(순서: Kafka 닫기 → pending 저장 → 채널 close)
Close(ctx context.Context) error
```

빠른 시작 (예시)
```go
package main

import (
    "context"
    "log"

    ap "your/module/analyzerpool"   // AnalyzerPool 패키지
    comp "your/module/computation"  // FindTesting/ProductionStorageRootPath
    kb "your/module/kafka"          // KafkaBatchConsumer 패키지
)

// 이벤트/Tx 타입(예시)
type CCEvent  struct{ Cmd, Arg string }
type EEEvent  struct{ /* ... */ }
type CCEEvent struct{ /* ... */ }  // 주의: 철자 일관성
type EECEvent struct{ /* ... */ }
type Tx struct{ Hash, From, To string }

// 포트(읽기 전용 인터페이스) 구현체 예시
type ccPortImpl struct{}
func main() {
    var isTest ap.Mode = true // test = true / prod = false

    pool, err := ap.CreateAnalyzerPoolFrame[CCEvent, EEEvent, CCEEvent, EECEvent, Tx](isTest)
    if err != nil { log.Fatal(err) }
    defer pool.Close(context.Background())

    // 포트 등록
    pool.Register(&ccPortImpl{}, /*ee*/ nil, /*cce*/ nil, /*eec*/ nil)

    // CC 이벤트 소비 루프
    go func() {
        for evt := range pool.DequeueCC() {
            _ = evt // handle CCEvent
        }
    }()

    // EE → CC로 커맨드 발행
    _ = pool.PublishToCC(CCEvent{Cmd: "rebuild", Arg: "shard-3"})

    // CC용 Tx 소비
    go func() {
        for tx := range pool.ConsumeCCTx() {
            _ = tx // handle Tx
        }
    }()

    // ...
    _ = comp // 임포트 참조 예시 (실제 코드에서는 경로/설정 교체)
    _ = kb
}
```

### 주의

1. Kafka 브로커/토픽/그룹 자리값(testval)은 배포 환경에 맞게 설정.

2. 제네릭 타입 철자 혼동 주의(CcEevt vs CceEvt 등). IDE 리팩터 혹은 grep으로 일관성 체크 권장.

### 디자인 디테일
1. 이벤트 버스(SPSC, 강타입)
    a. 대상 모듈당 1개(다중 생산자 → 단일 소비자)
    b. 내부는 pending []T + sync.Cond와 out <-chan T(1개) 구성
    c. Publish: pending에 push(용량 초과 시 cond wait로 역압)
    d. Dequeue: out 채널에서 블로킹 읽기

2. 시작/종료 시의 I/O
    a. 시작: JSONL 로드 → pending에 적재 → 파일 즉시 삭제
    b. 종료: 생산 중단 → pending을 JSONL로 동기 저장 → out close
3. 왜 pending이 필요한가?
“종료 시 남은 값 저장”을 레이스 없이 정확히 수행하기 위해,
소비자(out)와 경합 없이 전달 전 집합을 Pool이 완전히 통제해야 함

3. Kafka Tx 팬아웃
    a. 공유 토픽 + 모듈별 GroupID 분리
    b. 라이브러리의 KafkaBatchConsumer[T] 사용(배치 크기/타임아웃 튜닝)
    c. 읽은 Tx를 모듈별 채널로 전달 + 원자 카운터 집계

4. 모드(bool) & 저장 루트
    a. Mode패키지의 Mode와 IsTest로 테스트 판별
    b. 루트는 루트부터 분기
    c. test: computation.FindTestingStorageRootPath()
    d. prod: computation.FindProductionStorageRootPath()
    e. 풀 루트: <ROOT>/analyzer_pool
    f. 이벤트 버스: <ROOT>/analyzer_pool/eventbus/*.jsonl

5. 신뢰성 & 보장 범위
    a. 이벤트 전달: at-least-once(중복 가능). 필요 시 상위에서 idempotency 구현 추천
    b. 재시작 복구: 시작 시 JSONL 로드→삭제 / 종료 시 pending 저장
    c. 실행 중 디스크 I/O 없음(지연·경합 최소화)

6. 성능/튜닝 팁
    a. pending cap(cond-wait 임계)을 모드별로 다르게(테스트 작게, 프로덕션 크게)
    b. Kafka BatchSize/BatchTimeout: 트래픽 패턴에 맞춰 조정
    c. 소비자가 느릴 경우 자연스런 역압(Publish 블로킹)으로 폭주 방지. 그러나 이는 웬만해선 없을듯. 애초에 이벤트 쿼리가 그리 많을까?

7. 테스트 가이드
    a. test 모드에서 루트를 FindTestingStorageRootPath()로 격리
    b. 종료 직후 JSONL 파일을 열어 pending 덤프를 확인(유닛테스트에서 golden과 비교)
    c. 강제 종료 시나리오: 재시작 복구 동작 테스트(시작 로드→파일 삭제 확인)

### FAQ

Q. 왜 out 채널만 두지 않고 내부 pending도 두나요? (내 질문이었음)
A. “종료 시 남은 값 저장”을 정확히 하려면 전달 직전 집합을 Pool이 완전히 통제해야 합니다. pending이 그 역할을 하며, 소비자 채널과의 레이스 없이 정확한 스냅샷을 보장합니다. out만으로 구현하려면 모듈 측 정지/합류(ACK) 프로토콜이 추가로 필요합니다.

### 체크리스트

1. 모듈 포트 구현(읽기 메서드) 후 Register로 주입

2. 이벤트 타입/Tx 타입 정의(제네릭 매개변수)

3. Kafka 브로커/토픽/그룹 설정 반영

4. 모드별 루트 경로 함수 연결(computation.*)

5. 종료 시 Close(ctx) 호출로 pending 저장