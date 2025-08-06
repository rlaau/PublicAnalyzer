# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Ethereum blockchain transaction analysis project (chainAnalyzer) written in Go that detects anomalous contracts through transaction analysis. It uses a modular monolithic architecture with Domain-Driven Design principles.

## Common Commands

### Kafka Infrastructure Setup

```bash
# Start Kafka and Zookeeper (required before running any services)
docker-compose up -d

# Check Kafka health status
docker-compose ps

# View Kafka logs
docker logs kafka

# Test Kafka connectivity
go run ./testcmd/kafka_auto_create.go
```

### Building and Running

```bash
# Build the main application
go build -o chainanalyzer ./cmd/main.go

# Build specific test utilities
go build -o kafka-test ./testcmd/kafka_auto_create.go

# Run the main application (currently minimal - contains TODOs)
./chainanalyzer
```

### Testing

```bash
# Run all tests (make sure Kafka is running first)
go test -v ./...

# Run tests for specific module
go test -v ./internal/txingester/app/...

# Run the main integration test (requires Kafka)
go test -v ./internal/txingester/app -run TestTestingIngester

# Test individual components
go test -v ./shared/kafka/...
go test -v ./shared/groundknowledge/ct/...
```

### Module Dependencies

```bash
# Update Go modules
go mod tidy

# View module dependencies
go mod graph

# Download dependencies
go mod download
```

### Development Environment Setup

The project runs on WSL2 and requires significant memory allocation. Ensure your `.wslconfig` has:
```
[wsl2]
memory=24GB
swap=96GB
```

## Architecture

### Main Concept
This sysytem uses Domain-Driven and Event-Driven architecture to get simplexity and high performence.
This sysyem runs on monolytic codebase.

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
- `kafka/` - Kafka client implementation with producer/consumer patterns
- `dto/` - Common data transfer objects  
- `domain/` - Core domain models (Address, Block, Contract, EOA, Transaction)
- `alerting/` - Email alerting service
- `monitoring/` - Performance profiling and metrics collection
  - `meter/` - Performance meters (count, time, TPS)
  - `monitor/` - System monitors (channel, Kafka)
- `groundknowledge/` - Shared project knowledge (single source of truth)
  - `ct/` - ChainTimer for unified time source
  - `ingestcontract/` - Contract ingestion utilities
- `workflow/` - High-performance data processing patterns
  - `workerpool/` - Worker pool implementation
  - `fp/` - Functional programming utilities (map, monad patterns)
- `txfeeder/` - Mock transaction generation for testing environments
## Important Notes

- **Kafka Dependency**: All tests and services require Kafka to be running (`docker-compose up -d`)
- **Memory Requirements**: Project runs on WSL2 and requires significant memory allocation (24GB+)
- **Data Constraints**: Cannot store all blockchain data due to volume constraints
- **Learning Architecture**: Analyzers use continuous learning and discard training data after use
- **Storage Hierarchy**: Analysis results stored hierarchically - some permanently, most temporarily
- **Main Entry Point**: `cmd/main.go` is minimal and contains TODOs for future development
- **Module Status**: Only `txingester` module is fully implemented; others are planned/in development

## Test Data and Development Files

- `/testdata/` - Ethereum transaction CSV files (eth_tx_000000000000.csv through eth_tx_000000000046.csv) for testing ingestion pipeline
- `/testcmd/` - Utility commands for testing Kafka connectivity and setup
- `docker-compose.yml` - Kafka and Zookeeper infrastructure setup

## Development Guidelines

### Key Dependencies and External Services

```go
// Core dependencies in go.mod
"github.com/segmentio/kafka-go"     // Kafka client
"github.com/dgraph-io/badger/v4"    // Embedded database
"cloud.google.com/go/bigquery"     // BigQuery integration
```

### Critical Development Considerations

1. **Test Files**: `*_test.go` files cannot be run with `go run` - use `go test` or rename files
2. **Import Cycles**: Avoid circular dependencies between modules - check import paths carefully
3. **File Path Handling**: Always verify file paths and parsing logic before implementation
4. **Persistent Data**: Check local storage paths for data persistence and mock data handling
5. **Shared Resources**: 
   - Use `/shared/groundknowledge/` for shared data access
   - Use `/shared/workflow/workerpool/` for parallel processing patterns
6. **Kafka Integration**: Ensure Kafka is running before executing any service or test

### Code Style and Annotations

Follow Effective Go conventions with these specific comment styles:
- `//TODO` - Future implementation tasks
- `//FIXME:` - Issues requiring future fixes
- `//!` - Critical/important information
- `//?` - Questions or areas needing investigation
- `//*` - Interesting facts or references
- `//` - Standard comments

Avoid other comment styles for editor compatibility.