# Contract Database

BadgerDB 기반의 Contract 저장소입니다. Address를 키로 하여 Contract 정보를 저장하고 조회할 수 있습니다.

## 주요 기능

- **기본 CRUD 연산**: Put, Get, IsContain, Delete
- **배치 처리**: BatchPut, BatchGet, BatchDelete
- **병렬 처리 안전성**: sync.RWMutex로 동시성 제어
- **JSON 직렬화**: Contract 구조체의 JSON 직렬화/역직렬화
- **유틸리티 함수**: Count, GetAll

## 사용 방법

### 초기화
```go
db, err := NewContractDB("/path/to/database")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### 기본 연산
```go
// Contract 저장
contract := domain.Contract{
    Address:   someAddress,
    IsERC20:   true,
    IsERC721:  false,
    // ... 기타 필드
}
err := db.Put(contract)

// Contract 조회
contract, err := db.Get(someAddress)

// 존재 여부 확인
exists := db.IsContain(someAddress)

// Contract 삭제
err := db.Delete(someAddress)
```

### 배치 연산
```go
// 여러 Contract 저장
contracts := []domain.Contract{contract1, contract2, contract3}
err := db.BatchPut(contracts)

// 여러 Contract 조회
addresses := []domain.Address{addr1, addr2, addr3}
results, err := db.BatchGet(addresses)

// 여러 Contract 삭제
err := db.BatchDelete(addresses)
```

### 유틸리티
```go
// 총 개수 조회
count, err := db.Count()

// 모든 Contract 조회 (개발/디버깅용)
allContracts, err := db.GetAll()
```

## 병렬 처리

이 데이터베이스는 병렬 접근에 안전하게 설계되었습니다:
- **읽기 연산**: 동시에 여러 goroutine에서 실행 가능
- **쓰기 연산**: Exclusive lock으로 보호
- **혼합 연산**: RWMutex로 적절한 동시성 제어

## 성능 고려사항

- **BadgerDB 옵션**: 데이터 내구성을 위한 SyncWrites 활성화
- **배치 처리**: 여러 연산을 단일 트랜잭션으로 처리하여 성능 최적화
- **키 prefix**: "contract:" prefix로 네임스페이스 구분

## 테스트

```bash
go test -v ./internal/nod/co/app/
```

테스트는 다음을 포함합니다:
- 기본 CRUD 연산 테스트
- 배치 처리 테스트
- 동시성 안전성 테스트
- 카운팅 기능 테스트