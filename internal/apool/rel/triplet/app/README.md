# EE Module Transaction Generator

트랜잭션 생성기는 EE 모듈의 Union-Find 알고리즘을 테스트하기 위한 스트리밍 트랜잭션 데이터를 생성합니다.

## 주요 기능

### 1. Mock Deposit Addresses
- `mockedAndHiddenDepositAddress.txt`: 10만 개의 가상 입금 주소
- 실제 CEX 입금 주소를 시뮬레이션

### 2. Transaction Generation Patterns
- **총 4억 건** 트랜잭션 생성
- **1초당 100만 건** 속도로 생성
- **15건당 1초씩** 시간 증가 (2025-01-01 기준)

### 3. Transaction Types
- **50건 중 1건**: MockDepositAddress → CEXAddress
- **20건 중 1건**: RandomAddress → MockDepositAddress  
- **나머지**: RandomAddress → RandomAddress

## 사용법

### 기본 테스트 실행
```go
// 1000건의 테스트 트랜잭션 생성
err := app.RunTxGeneratorExample()
if err != nil {
    log.Fatal(err)
}
```

### 프로덕션 설정
```go
// 4억 건의 실제 규모 트랜잭션 생성
generator := app.ProductionTxGeneratorExample()

ctx := context.Background()
generator.Start(ctx)

// 트랜잭션 소비
for tx := range generator.GetTxChannel() {
    // Union-Find 알고리즘에 트랜잭션 전달
    processTransaction(tx)
}
```

## 파일 구조

```
internal/ee/
├── cex.txt                              # CEX 핫월렛 주소 (112개)
├── mockedAndHiddenDepositAddress.txt    # Mock 입금 주소 (10만개)
├── domain/
│   ├── cex.go                           # CEX 도메인 모델
│   └── txgenerator.go                   # 트랜잭션 생성기 도메인
├── app/
│   ├── txgenerator.go                   # 트랜잭션 생성기 앱 서비스
│   └── txgenerator_example.go           # 사용 예제
└── cmd/
    └── test_generator/main.go           # 테스트 실행기
```

## 성능 특성

- **메모리 효율적**: 스트리밍 방식으로 메모리 사용량 최소화
- **고성능**: 고루틴 기반 비동기 생성
- **실시간**: Union-Find 알고리즘과 실시간 연동 가능

## 테스트 실행

```bash
# 간단한 테스트 (1000건)
go run internal/ee/cmd/test_generator/main.go

# 또는 빌드 후 실행
go build -o txgen internal/ee/cmd/test_generator/
./txgen
```

## 보완 가능한 점

1. **트랜잭션 값 분포**: 현재 0.1-10 ETH 범위의 단순 랜덤 생성
2. **Gas 설정**: 고정된 21000 가스, 실제 컨트랙트 호출 시뮬레이션 없음
3. **블록 번호**: 간단한 공식 사용, 실제 이더리움 블록 패턴과 차이
4. **논스 관리**: 순차적 증가, 실제 계정별 논스 관리 미지원
5. **네트워크 지연**: 실제 블록체인 네트워크의 지연 시뮬레이션 없음

이러한 점들은 필요에 따라 더 정교한 시뮬레이션으로 개선할 수 있습니다.