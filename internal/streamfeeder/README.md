# StreamFeeder Module

The StreamFeeder module provides functionality to stream RawTransactions from TxStreamer to Kafka using a backpressure-enabled producer. It maintains persistent state for resumable streaming operations.

## Architecture

### Components

1. **TxStreamFeeder** - Main streaming component that orchestrates transaction feeding
2. **StreamPosition** - Domain model for tracking streaming position (date + index)
3. **StreamTarget** - Domain model for defining streaming ranges
4. **StreamPositionRepository** - Interface for position persistence
5. **FileStreamPositionRepository** - File-based position persistence implementation

### Key Features

- **Resumable Streaming**: Automatically saves and resumes from last processed position
- **Date Range Streaming**: Stream transactions between specific dates
- **Backpressure Integration**: Uses Kafka backpressure producer for optimal throughput
- **Direct Mode**: Utilizes direct mode of backpressure producer as requested
- **Persistent State**: Saves position to file for crash recovery
- **Error Handling**: Retry logic with configurable parameters
- **Graceful Shutdown**: Proper cleanup and final position saving

## Usage

### Basic Configuration

```go
config := app.TxStreamFeederConfig{
    // TxStreamer configuration
    StreamerDir: "/path/to/stream_storage/sorted", // Contains tx_YYYY-MM-DD.mm files
    
    // Kafka configuration  
    KafkaBrokers: []string{"localhost:9092"},
    Topic:        kafka.ProductionTxTopic, // "production-tx"
    
    // Backpressure configuration
    InitialBatchSize:  100,
    InitialInterval:   1 * time.Second,
    DirectChannelSize: 10000,
    MaxBatchSize:      1000,
    MaxInterval:       60 * time.Second,
    
    // Position persistence
    DataDir:      "/path/to/data", // For position file storage
    SaveInterval: 10 * time.Second,
    
    // Retry configuration
    MaxRetries: 3,
    RetryDelay: 1 * time.Second,
}
```

### Creating and Starting TxStreamFeeder

```go
// Create backpressure instance
backpressure := tools.NewSimpleBackpressure(tools.SimpleBackpressureConfig{
    InitialBatchSize:      100,
    InitialIntervalSecond: 1,
    MaxBatchSize:         1000,
    MaxIntervalSecond:    60,
})

// Create feeder
feeder, err := app.NewTxStreamFeeder(config, backpressure)
if err != nil {
    log.Fatalf("Failed to create feeder: %v", err)
}

// Start feeder
if err := feeder.Start(); err != nil {
    log.Fatalf("Failed to start feeder: %v", err)
}
```

### Streaming Methods

#### 1. StreamBetween - Stream within date range
```go
// Stream all transactions from 2023-09-01 to 2024-12-31 (inclusive)
err := feeder.StreamBetween("2023-09-01", "2024-12-31")
```

#### 2. StreamFrom - Stream from specific position
```go
// Stream starting from specific date and index
err := feeder.StreamFrom("2023-09-01", 50000)
```

#### 3. StreamTo - Stream to specific date
```go
// Stream from current position to end date
err := feeder.StreamTo("2024-12-31")
```

### Position Management

The system automatically manages streaming position:

- **Semi-open Interval**: The stored index represents the "next record to fetch"
- **Automatic Saving**: Position is saved periodically during streaming
- **Resume Capability**: On restart, automatically loads and resumes from last position
- **File Storage**: Position stored in JSON format in specified data directory

Example position file (`streamfeeder_position.json`):
```json
{
  "date": "2023-09-15",
  "index": 75000
}
```

### Internal Workflow

For `StreamBetween("2023-09-01", "2024-12-31")`:

1. Sets initial position to `{Date: "2023-09-01", Index: 0}`
2. For each date in range:
   - Calls `txStreamer.GetTenThousandsTx(date, index)` to get up to 10,000 transactions
   - Converts to Kafka messages with TxId as partition key
   - Sends via `kafka.SendDirect(messages)` using backpressure producer
   - Updates position: `Index += len(transactions)`
   - Saves position periodically
3. Moves to next date when current date is exhausted

### Integration Points

#### TxStreamer Integration
- Uses `txStreamer.GetTenThousandsTx(date, index)` method
- Processes transactions in batches of up to 10,000
- Handles file-not-found errors gracefully

#### Kafka Integration  
- Uses `KafkaBatchProducerWithBackpressure` in Direct mode
- Messages keyed by transaction ID for consistent partitioning
- Publishes to `kafka.ProductionTxTopic` by default

#### Backpressure Integration
- Integrates with `tools.CountingBackpressure` interface  
- Producer automatically adjusts batch sizes and intervals based on backpressure signals
- Supports configurable limits (max batch size, max interval)

## Error Handling

- **Retry Logic**: Configurable retries for failed Kafka sends
- **Graceful Degradation**: Continues to next date if current date fails
- **Position Recovery**: Saves position before shutdown for restart capability
- **File I/O Errors**: Atomic position file writes using temp files

## Monitoring

### Statistics
```go
stats := feeder.GetStats()
// Returns:
// {
//     "is_running": bool,
//     "current_position": StreamPosition,
//     "topic": string,
//     "total_produced": uint64,
//     "total_errors": uint64,
//     "producer_mode": ProducerMode
// }
```

### Logging
- Position updates and streaming progress
- Batch processing statistics  
- Error conditions and retries
- Startup and shutdown events

## Testing

Run tests:
```bash
go test ./internal/streamfeeder/app/...
```

Test files include:
- `tx_stream_feeder_test.go` - Unit tests for core functionality
- `example.go` - Usage examples and integration patterns

## Dependencies

- `shared/txstreamer` - For reading transaction data
- `shared/kafka` - For Kafka producer with backpressure
- `shared/monitoring/tools` - For backpressure interface
- `shared/domain` - For RawTransaction domain model

## File Structure

```
internal/streamfeeder/
├── README.md
├── app/
│   ├── tx_stream_feeder.go      # Main implementation
│   ├── tx_stream_feeder_test.go # Unit tests
│   └── example.go               # Usage examples
├── domain/
│   └── streamfeeder_domain.go   # Domain models
├── interface.go                 # Repository interfaces
└── infra.go                    # Infrastructure implementations
```