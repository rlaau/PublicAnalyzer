# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Ethereum blockchain transaction analysis project (chainAnalyzer) written in Go that detects anomalous contracts through transaction analysis. It uses a modular monolithic architecture with Domain-Driven Design principles.

## Common Commands

### Running the Project

```bash
# Start Kafka infrastructure
docker-compose up -d

# Build the project
go build -o chainanalyzer ./cmd/main.go

# Run the project (currently contains TODOs)
./chainanalyzer
```

### Testing

```bash
# Run all tests
go test -v ./...

# Run tests for specific module
go test -v ./internal/txingester/app/...

# Run a specific test
go test -v ./internal/txingester/app -run TestTestingIngester
```

### Development Environment Setup

The project runs on WSL2 and requires significant memory allocation. Ensure your `.wslconfig` has:
```
[wsl2]
memory=24GB
swap=96GB
```

## Architecture

### Module Structure

Each module in `/internal` follows this pattern:
- `/app/` directory - Use cases and business logic (multiple files)
- `/domain/` directory - Domain models and entities (multiple files)  
- `infra.go` - Infrastructure adapters
- `interface.go` - Infrastructure access interfaces
- `monitoring.go` - Module-specific monitoring (if present)

**Note**: Currently only `txingester` module is fully implemented. Other modules exist as empty directories or are planned for future development.

### Key Modules

**Currently Implemented:**
1. **TxIngester** - Ingests transaction data from external sources, marks transactions by type (EOA-EOA, EOA-Contract, etc.), and publishes to Kafka. Communicates with CCE module for contract verification.

**Planned/In Development:**
2. **CCE Analyzer** - Will analyze Contract-EOA relationships and maintain contract information
3. **CC Analyzer** - Will analyze Contract-Contract relationships
4. **EEC Analyzer** - Will analyze EOA-Contract relationships
5. **EE Analyzer** - Will analyze EOA-EOA relationships.
6. **KnowledgePropagator** - Will propagate analysis results between modules
7. **EventTraper** - Will aggregate analysis results and filter important events
8. **EventInterpreter** - Will interpret important events
9. **EventManager** - Will manage and store events

### Inter-Module Communication

- Modules communicate primarily through Kafka topics
- The main Kafka topic for transactions is `ingested-transactions`
- Kafka broker runs on `localhost:9092`

### Shared Services

Located in `/shared`:
- `kafka/` - Kafka client implementation
- `dto/` - Common data transfer objects
- `alerting/` - Email alerting service
- `monitoring/` - Profiling tools
- `windowkeeper/` - Window-based data processing
- `workerpool/` - Worker pool implementation
- `txfeeder` - 테스트 환경에서 "가상의 트랜잭션"을 생성 후 각 모듈에 주입해주는 역할을 해
## Important Notes

- The project cannot store all blockchain data due to volume constraints
- Analyzers use continuous learning and discard training data after use
- Analysis results are stored hierarchically - some permanently, most temporarily
- Follow Effective Go coding and naming conventions
- The main entry point (`cmd/main.go`) is still under development with TODOs

## Test Data

The `/testdata` directory contains Ethereum transaction CSV files (eth_tx_000000000000.csv through eth_tx_000000000046.csv) for testing the ingestion pipeline.

### 주의 사항. 코딩 시 염두할 것.
1. *_test.go파일은 go run하지 못하니, go test로 실행하든, 파일명을 바꾸든 해야 해.
2. 임포트 사이클 오류를 조심해야 해.
3. 클로드는 파일경로 참조를 많이 실수해. 파일 관련 오류가 있으면 파일경로 함수나 명령을 반드시 검토해.
4. 너는 파일을 읽을 때 파싱을 잘못하는 경우가 많아. 파일 관련 로직을 쓰기 전, 파일 내부를 본 후 올바른 전처리, 파싱을 점검해.
5. 퍼시스턴트 데이터를 관리할 땐, 해당 데이터가 로컬의 어디에 저장되어서 로드되는지 반드시 그 경로를 검토해. 모킹 데이터를 다룰 때도 마찬가지야.

## annotaions
1. effective go를 따라
2. 다만, 주석은 
-1 "//TODO" <-나중에 할 것을 표시
-2 "//FIXME:"<- 추후 수정 표기
-3 "//!" <-아주 중요한 내용
-4 "//?" <-더 알아볼 내용들
-5 "//*" <-참고할 흥미로운 사실
-5 "//" <- 그냥 주석
을 참고해서 써줘. 나머지 주석 스타일은 지양해줘. 이게 내 에디터에 잘 뜨거든.