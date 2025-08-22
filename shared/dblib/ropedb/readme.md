# RopeDB

`Vertex` 간의 관계를 `Trait`로 표현하고, **같은 Trait로 연결된 정점들의 묶음**을 **Rope**로 관리합니다.

핫패스(조회/얕은 갱신)는 동기, 무거운 마크(Rope/Trait)는 **이벤트버스 기반 비동기 업서트**로 처리합니다.

---
### 사용 시
```go

dbA, _ := NewRopeDB(mode.Prod, "cluster-A")
dbB, _ := NewRopeDB(mode.Prod, "cluster-B")
```
## 핵심 컨셉

- **Vertex**: 요약 정점(주소 기준). 이웃과의 관계를 `PartnerLink`로 보관.
- **Trait**: 관계의 유형(예: A, B, C…). `(Vertex, Vertex)` 간 간선의 성격을 뜻함.
- **Rope**: **단일 Trait**로 이어지는 정점들의 그룹. *Rope는 저장되는 엔터티가 아니라, `RopeMark`로 요약 저장됨.*
- **RopeMark / TraitMark**: 각각 Rope, Trait 의 집계/메타 정보(멤버 목록, 크기, 볼륨, 최신 시각 등).

### 불변식(도메인 규칙)

- 하나의 Vertex에서 **Trait ↔ RopeID**는 **1:1**입니다.
    
    → Vertex 내부의 `RopeRef`는 Trait별로 **정확히 한 칸**만 유지됩니다.
    
- `PartnerLink`는 `(Partner, Trait)` 쌍에 대해 **최대 1개**만 유지됩니다.
    
    → 같은 파트너와 여러 Trait로 연결 가능(중복 Trait 금지).
    
- Rope의 권장 최대 크기: **1000** (`maxRopeSize`).

---

## 설계 개요

### 저장소

- **BadgerDB** 1개 인스턴스(기본):
    - `v:<address>` → `Vertex` JSON
    - `r:<ropeID>` → `RopeMark` JSON
    - `t:<traitID>` → `TraitMark` JSON
    - `ctr:rope` / `ctr:trait` → `uint64` 카운터(BigEndian)
- **이벤트버스(EventBus)**: `RopeMarkUpsert`, `TraitMarkUpsert`를 채널로 처리
    - 시작 시 **백로그(JSONL)**를 읽어 pending 복원
    - 종료 시 현재 pending을 **JSONL로 저장**
    - 경로(모드에 따라 루트 분기):
        - 테스트: `computation.FindTestingStorageRootPath()/rope_db/dbName/eventbus/…`
        - 프로덕션: `computation.FindProductionStorageRootPath()/rope_db/dbName/eventbus/…`

### 일관성 모델

- **Vertex**: 동기 경로 — `PushTraitEvent` 시 즉시 저장 → 조회에 **즉시 반영**.
- **RopeMark/TraitMark**: 비동기 워커가 업서트 → **Eventual Consistency**.
    - `ViewRopeByNode`(Vertex 기반)는 **즉시** 변화 확인 가능
    - `ViewRope(id)`(RopeMark 기반)는 반영까지 **약간의 지연** 가능

### 워커

- `busTrait` → `applyTraitUpsert` → `t:<traitID>` 업데이트
- `busRope` → `applyRopeUpsert` → `r:<ropeID>` 업데이트(+ 병합 처리)

---

## 패키지/타입 개요

> 실제 타입들은 프로젝트의 internal/ee/domain 등에서 정의됩니다. 아래는 개략.
> 

```go
type Vertex struct {
  Address shareddomain.Address
  Links   []PartnerLink     // (Partner, Trait, TraitID) 단위
  Ropes   []RopeRef         // Trait별로 정확히 1칸 유지
}

type PartnerLink struct {
  Partner shareddomain.Address
  Trait   TraitCode
  TraitID TraitID
}

type RopeRef struct {
  ID    RopeID
  Trait TraitCode
}

type TraitEvent struct {
  Trait   TraitCode
  RuleA   RuleCode
  RuleB   RuleCode
  Time    chaintimer.ChainTime
  Score   int32
}

type RopeMark struct {
  ID       RopeID
  Trait    TraitCode
  Members  []shareddomain.Address
  Size     uint32
  Volume   uint32
  LastSeen chaintimer.ChainTime
}

type TraitMark struct {
  ID       TraitID
  Trait    TraitCode
  RuleA    RuleCode
  RuleB    RuleCode
  Score    int32
  Volume   uint32
  LastSeen chaintimer.ChainTime
}

type RopeMarkUpsert struct {
  RopeID      RopeID
  Trait       TraitCode
  AddMembers  []shareddomain.Address
  SizeDelta   int32
  VolumeDelta uint32
  LastSeen    chaintimer.ChainTime
  MergeFrom   []RopeID // 병합 소스
}

type TraitMarkUpsert struct {
  TraitID     TraitID
  Trait       TraitCode
  RuleA       RuleCode
  RuleB       RuleCode
  ScoreDelta  int32
  VolumeDelta uint32
  LastSeen    chaintimer.ChainTime
}

```

---

## 퍼블릭 API

```go
type RopeDB interface {
  // Trait 이벤트 입력(핫패스): Vertex 동기 갱신 + Rope/Trait 마크는 비동기 큐잉
  PushTraitEvent(a1, a2 shareddomain.Address, ev domain.TraitEvent) error

  // 조회
  ViewRopeByNode(a shareddomain.Address) (*domain.Rope, error)
  ViewRope(id domain.RopeID) (*domain.Rope, error)
  ViewInSameRope(a1, a2 shareddomain.Address) (bool, error)

  Close() error
}

```

생성자:

```go
// 모드(true=test, false=prod)에 따라 루트 디렉터리 분기
db, err := infra.NewRopeDB(true /* isTest */)
defer db.Close()

```

테스트 편의를 위한 루트 주입형(선택 구현):

```go
// 매 테스트마다 t.TempDir()로 완전 격리 가능
db, err := infra.NewRopeDBWithRoot(true, tmpRoot)

```

---

## 동작 흐름

### 1) PushTraitEvent

1. **Vertex 로드/생성** (v1, v2)
2. `(v1, v2, Trait)` 기준으로 **PartnerLink 업서트**, 공통 `TraitID` 확보
3. `TraitMarkUpsert` 이벤트 **Publish**
4. **Rope 결정/병합 로직**
    - 두 Vertex에 해당 Trait의 Rope가 없으면 → 새 RopeID 부여, 두 Vertex에 `RopeRef` 세팅
    - 한쪽만 Rope가 있으면 → 다른 한쪽을 **편입**(`RopeRef` 세팅)
    - 둘 다 Rope가 있으면:
        - **같은 RopeID**: 볼륨 증가
        - **다른 RopeID**: **병합**
            - 두 Rope의 `RopeMark.Size` 비교하여 큰 쪽을 **타깃**
            - 소스 멤버들의 Vertex에서 `RopeRef(Trait)`를 타깃 ID로 **일괄 치환**
            - `RopeMarkUpsert`(AddMembers, MergeFrom…) 이벤트 **Publish**
5. Vertex **동기 저장**

> Vertex는 즉시 일관, 마크는 워커가 비동기 반영.
> 

### 2) 워커가 마크 반영

- `TraitMarkUpsert` → `t:<traitID>`에 집계(Score, Volume, LastSeen)
- `RopeMarkUpsert` → `r:<ropeID>`에 멤버 dedup + Size/Volume/LastSeen 반영
    
    `MergeFrom`은 소스 RopeMark 키 삭제(간단화; 필요 시 Tombstone/아카이브로 변경 가능)
    

---

## 사용 예시

```go
// 1,2,3을 TraitA로 연결(1-2, 2-3)
_ = db.PushTraitEvent(a1, a2, TraitEvent{Trait: TraitA, Time: now, Score: 1})
_ = db.PushTraitEvent(a2, a3, TraitEvent{Trait: TraitA, Time: now, Score: 1})

// 3,4를 TraitB로 연결
_ = db.PushTraitEvent(a3, a4, TraitEvent{Trait: TraitB, Time: now, Score: 1})

// 3,5,6을 TraitC로 연결(3-5, 5-6)
_ = db.PushTraitEvent(a3, a5, TraitEvent{Trait: TraitC, Time: now, Score: 1})
_ = db.PushTraitEvent(a5, a6, TraitEvent{Trait: TraitC, Time: now, Score: 1})

// 조회(즉시 일관성 경로)
rope, _ := db.ViewRopeByNode(a2) // a2가 속한 첫 번째 Rope 반환
same, _ := db.ViewInSameRope(a1, a3)

```

> ViewRope(id)는 RopeMark를 읽으므로 비동기 반영까지 약간의 지연이 있을 수 있습니다.
> 

---

## 병합 규칙

- 서로 다른 RopeID를 가진 두 정점이 **같은 Trait**으로 연결되면 병합.
- **큰 RopeMark.Size**를 가진 쪽을 타깃으로 선택.
- 소스 멤버들의 `RopeRef(Trait)`를 타깃 ID로 **치환 저장**(Vertex 경로는 동기 처리).
- 타깃 RopeMark에 `AddMembers`/`SizeDelta`/`MergeFrom` 반영. 소스 RopeMark는 삭제(간단화).

---

## 디렉터리 구조

- 테스트 모드: `computation.FindTestingStorageRootPath()`
- 프로덕션 모드: `computation.FindProductionStorageRootPath()`

```
<root>/
  rope_db/
    badger/                    # Badger 데이터디렉터리
    eventbus/
      ropemark.jsonl           # RopeMark 업서트 백로그
      traitmark.jsonl          # TraitMark 업서트 백로그

```

> 중요: 테스트 시 이전 실행의 디스크 상태가 남아 있으면 결과가 오염됩니다.
> 
> - 방법 A: 테스트 시작 전 `<root>/rope_db` 삭제
> - 방법 B(권장): `NewRopeDBWithRoot(true, t.TempDir())`로 매번 **격리 루트** 사용

---

## 성능/운영 팁

- 단일 Badger 인스턴스에서도 의도대로 동작(동시 다수 read + 단일 write)
    
    다만 Rope/Trait 마크 쓰기 폭주 시 레이턴시 스파이크가 있을 수 있음
    
- 필요 시 **3-DB 분리**(Vertex/RopeMark/TraitMark)로 자원 경합 완화
- 이벤트버스 **capacity** 넉넉히, 워커 **동시성**(개수) 조정
- `apply*Upsert`는 **짧은 write 트랜잭션** 유지(배치 크기/주기 튜닝)
- Vertex JSON을 작게 설계(핫패스 read 안정화). Badger 옵션(ValueThreshold 등)은 워크로드에 맞춰 조정

---

## 테스트 가이드

- 비동기 반영을 고려해 **폴링 헬퍼**로 조건을 기다립니다(예: 최대 8초).
- 캐시/잔존 데이터 방지를 위해:
    - `count=1` 사용
    - 또는 `NewRopeDBWithRoot(true, t.TempDir())`
- 병렬 실행 시 스토리지 충돌 주의

---

## 실패/백오프

- 이벤트버스 `Publish`는 capacity 초과 시 **역압**(블록)될 수 있습니다.
    
    핫패스 지연을 피하려면 `TryPublish`나 타임아웃 래핑을 고려하세요.
    
- 병합 중간에 장애가 나더라도:
    - Vertex 경로는 **동기 저장**되어 Trait↔RopeID 매핑이 유지됩니다.
    - 마크 반영은 이벤트버스 **백로그**로 재적용됩니다.

---

## 확장/선택 기능 제안(옵션)

- **워터마크/오프셋 노출**: “이 시각까지 반영된 뷰” 제공
- **RopeMark Tombstone**: 삭제 대신 이관 기록 유지
- **임시 조립 뷰**: RopeMark 미반영 시 Vertex 그래프를 따라 일시 조립

---

## 요약

- Vertex는 **얕고 빠르게**, Rope/Trait 집계는 **버스로 뒤로**.
- **Trait ↔ RopeID 1:1** 불변을 강제하여 병합/확장 로직을 단순화.
- Badger 1개로도 의도가 충분히 통하고, 필요 시 3-DB 분리로 더 안정화 가능.
- 이벤트버스 백로그로 **내결함성**(재시작 복원) 제공.

