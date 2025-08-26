package app

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

// ExampleUsage demonstrates how to use TxStreamFeeder
func ExampleUsage() {
	// Create configuration
	config := TxStreamFeederConfig{
		// TxStreamer configuration

		// Kafka configuration
		KafkaBrokers: []string{"localhost:9092"},
		Topic:        "production-tx", // Uses kafka.ProductionTxTopic

		// Backpressure configuration
		InitialBatchSize:  100,
		InitialInterval:   1 * time.Second,
		DirectChannelSize: 10000,
		MaxBatchSize:      1000,
		MaxInterval:       60 * time.Second,

		// Position persistence
		PositionDataDir: "/path/to/data", // Directory to store position file
		SaveInterval:    10 * time.Second,

		// Retry configuration
		MaxRetries: 3,
		RetryDelay: 1 * time.Second,
	}

	// Create backpressure instance
	backpressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    100000, // 모니터 쪽 스펙 (관찰 값)
			TargetSaturation: 0.5,
		},
		tools.SeedProducingConfig{SeedInterval: 1, SeedBatchSize: 1000},
		"./bp_state",
	)

	// Create TxStreamFeeder
	feeder, err := NewTxStreamFeeder(config, backpressure)
	if err != nil {
		log.Fatalf("Failed to create TxStreamFeeder: %v", err)
	}

	// Start the feeder
	if err := feeder.Start(); err != nil {
		log.Fatalf("Failed to start TxStreamFeeder: %v", err)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start streaming in a goroutine
	go func() {
		// Example 1: Stream between specific dates
		if err := feeder.StreamBetween("2023-09-01", "2024-12-31"); err != nil {
			log.Printf("Stream error: %v", err)
		}

		// Example 2: Stream from specific position
		// if err := feeder.StreamFrom("2023-09-01", 50000); err != nil {
		//     log.Printf("Stream error: %v", err)
		// }

		// Example 3: Stream to specific date
		// if err := feeder.StreamTo("2024-12-31"); err != nil {
		//     log.Printf("Stream error: %v", err)
		// }
	}()

	// Monitor stats
	statsTicker := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-sigChan:
			log.Println("Received shutdown signal")

			// Stop the feeder
			if err := feeder.Stop(); err != nil {
				log.Printf("Error stopping feeder: %v", err)
			}

			log.Println("Shutdown complete")
			return

		case <-statsTicker.C:
			stats := feeder.GetStats()
			log.Printf("Stats: %+v", stats)
		}
	}
}

// ExampleStreamFromCurrentPosition demonstrates resuming from saved position
func ExampleStreamFromCurrentPosition() {
	// Configuration (same as above)
	config := TxStreamFeederConfig{
		KafkaBrokers:      []string{"localhost:9092"},
		Topic:             "production-tx",
		InitialBatchSize:  100,
		InitialInterval:   1 * time.Second,
		DirectChannelSize: 50_000,
		MaxBatchSize:      20000,
		MaxInterval:       60 * time.Second,
		PositionDataDir:   "/path/to/data",
		SaveInterval:      1 * time.Second,
		MaxRetries:        3,
		RetryDelay:        1 * time.Second,
	}

	backpressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    100000, // 모니터 쪽 스펙 (관찰 값)
			TargetSaturation: 0.5,
		},
		tools.SeedProducingConfig{SeedInterval: 1, SeedBatchSize: 1000},
		"./bp_state",
	)
	// Create feeder (will automatically load previous position if exists)
	feeder, err := NewTxStreamFeeder(config, backpressure)
	if err != nil {
		log.Fatalf("Failed to create TxStreamFeeder: %v", err)
	}

	// Start the feeder
	if err := feeder.Start(); err != nil {
		log.Fatalf("Failed to start TxStreamFeeder: %v", err)
	}

	// Check current position
	position := feeder.GetCurrentPosition()
	log.Printf("Current position: %s", position.String())

	// If we have a valid position, resume from there
	if position.IsValid() {
		log.Printf("Resuming from saved position: %s", position.String())

		// Continue streaming from current position
		go func() {
			if err := feeder.StreamFrom(position.Date, position.Index); err != nil {
				log.Printf("Stream error: %v", err)
			}
		}()
	} else {
		log.Printf("No previous position found, starting fresh")

		// Start streaming from specific date
		go func() {
			if err := feeder.StreamBetween("2023-01-01", "2024-12-31"); err != nil {
				log.Printf("Stream error: %v", err)
			}
		}()
	}

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	if err := feeder.Stop(); err != nil {
		log.Printf("Error stopping feeder: %v", err)
	}
}
