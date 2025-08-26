package app

import (
	"fmt"
	"log"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/txstreamer"
)

// TxStreamFeeder feeds RawTransactions from TxStreamer to Kafka using backpressure producer
type TxStreamFeeder struct {
	// Dependencies
	txStreamer    *txstreamer.TxStreamer
	kafkaProducer *kafka.KafkaBatchProducerWithBackpressure[shareddomain.RawTransaction]
	positionRepo  streamfeeder.StreamPositionRepository

	// State
	currentPosition domain.StreamPosition
	isRunning       bool
	stopChan        chan struct{}

	// Configuration
	topic        string
	maxRetries   int
	retryDelay   time.Duration
	saveInterval time.Duration // How often to save position
}

// TxStreamFeederConfig configuration for TxStreamFeeder
type TxStreamFeederConfig struct {

	// Kafka configuration
	KafkaBrokers []string
	Topic        string

	// Backpressure configuration
	InitialBatchSize  int
	InitialInterval   time.Duration
	MaxBufferSize     int
	DirectChannelSize int
	MaxBatchSize      int
	MaxInterval       time.Duration

	// Position persistence
	PositionDataDir string        // Directory to store position file
	SaveInterval    time.Duration // How often to save position (default 10s)

	// Retry configuration
	MaxRetries int           // Maximum retries for failed operations (default 3)
	RetryDelay time.Duration // Delay between retries (default 1s)
}

// NewTxStreamFeeder creates a new TxStreamFeeder instance
func NewTxStreamFeeder(config TxStreamFeederConfig, backpressure tools.CountingBackpressure) (*TxStreamFeeder, error) {
	// Set defaults
	if config.Topic == "" {
		config.Topic = kafka.ProductionTxTopic
	}
	if config.SaveInterval <= 0 {
		config.SaveInterval = 1 * time.Second
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 5 * time.Second
	}

	if config.PositionDataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}

	// Create TxStreamer
	txStreamer := txstreamer.NewTxStreamer(computation.FindProjectRootPath() + "/stream_storage/sorted")

	// Create Kafka producer with backpressure (Direct mode)
	producerConfig := kafka.BackpressureProducerConfig{
		Brokers:           config.KafkaBrokers,
		Topic:             config.Topic,
		Mode:              kafka.ModeDirect, // Use direct mode as requested
		InitialBatchSize:  config.InitialBatchSize,
		InitialInterval:   config.InitialInterval,
		DirectChannelSize: config.DirectChannelSize,
		MaxBatchSize:      config.MaxBatchSize,
		MaxInterval:       config.MaxInterval,
	}

	kafkaProducer := kafka.NewKafkaBatchProducerWithBackpressure[shareddomain.RawTransaction](
		producerConfig,
		backpressure,
	)

	// Create position repository
	positionRepo := streamfeeder.NewFileStreamPositionRepository(config.PositionDataDir)

	feeder := &TxStreamFeeder{
		txStreamer:    txStreamer,
		kafkaProducer: kafkaProducer,
		positionRepo:  positionRepo,
		topic:         config.Topic,
		maxRetries:    config.MaxRetries,
		retryDelay:    config.RetryDelay,
		saveInterval:  config.SaveInterval,
		stopChan:      make(chan struct{}),
	}

	// Load existing position if available
	if positionRepo.HasPosition() {
		pos, err := positionRepo.LoadPosition()
		if err != nil {
			log.Printf("Failed to load existing position: %v", err)
		} else {
			feeder.currentPosition = pos
			log.Printf("Loaded existing position: %s", pos.String())
		}
	}

	return feeder, nil
}

// Start starts the TxStreamFeeder
func (f *TxStreamFeeder) Start() error {
	if f.isRunning {
		return fmt.Errorf("TxStreamFeeder is already running")
	}

	// Start Kafka producer
	if err := f.kafkaProducer.Start(); err != nil {
		return fmt.Errorf("failed to start Kafka producer: %w", err)
	}

	f.isRunning = true
	log.Printf("TxStreamFeeder started with topic: %s", f.topic)

	return nil
}

// Stop stops the TxStreamFeeder
func (f *TxStreamFeeder) Stop() error {
	if !f.isRunning {
		return nil
	}

	log.Printf("Stopping TxStreamFeeder...")

	// Signal stop
	close(f.stopChan)

	// Stop Kafka producer
	if err := f.kafkaProducer.Stop(); err != nil {
		log.Printf("Error stopping Kafka producer: %v", err)
	}

	// Save final position
	if f.currentPosition.IsValid() {
		if err := f.positionRepo.SavePosition(f.currentPosition); err != nil {
			log.Printf("Error saving final position: %v", err)
		} else {
			log.Printf("Final position saved: %s", f.currentPosition.String())
		}
	}

	f.isRunning = false
	log.Printf("TxStreamFeeder stopped")

	return nil
}

// StreamFrom streams transactions starting from the given date and index
func (f *TxStreamFeeder) StreamFrom(startDate string, startIndex int64) error {
	if !f.isRunning {
		return fmt.Errorf("TxStreamFeeder is not running")
	}

	// Update position
	f.currentPosition = domain.NewStreamPosition(startDate, startIndex)

	// Start streaming from current position
	return f.streamFromCurrentPosition()
}

// StreamTo streams transactions up to the given date (until end of day)
func (f *TxStreamFeeder) StreamTo(endDate string) error {
	if !f.isRunning {
		return fmt.Errorf("TxStreamFeeder is not running")
	}

	// If no current position, start from beginning of end date
	if !f.currentPosition.IsValid() {
		f.currentPosition = domain.NewStreamPosition(endDate, 0)
	}

	// Create target
	target := domain.NewStreamTarget(f.currentPosition.Date, endDate)

	return f.streamWithinTarget(target)
}

// StreamBetween streams transactions between startDate and endDate (inclusive)
func (f *TxStreamFeeder) StreamBetween(startDate, endDate string) error {
	if !f.isRunning {
		return fmt.Errorf("TxStreamFeeder is not running")
	}

	// Create target
	//* 양쪽 inclusive
	target := domain.NewStreamTarget(startDate, endDate)
	if !target.IsValid() {
		return fmt.Errorf("invalid date range: %s to %s", startDate, endDate)
	}

	// Set initial position
	f.currentPosition = domain.NewStreamPosition(startDate, 0)

	log.Printf("Starting to stream transactions from %s to %s", startDate, endDate)

	return f.streamWithinTarget(target)
}

// streamWithinTarget streams transactions within the specified target range
func (f *TxStreamFeeder) streamWithinTarget(target domain.StreamTarget) error {
	dates, err := target.GetDateRange()
	if err != nil {
		return fmt.Errorf("failed to get date range: %w", err)
	}

	log.Printf("Streaming %d days of data", len(dates))

	// Set up position saving ticker
	positionTicker := time.NewTicker(f.saveInterval)
	defer positionTicker.Stop()

	for _, date := range dates {
		// Check if we should stop
		select {
		case <-f.stopChan:
			log.Printf("Streaming stopped by user")
			return nil
		default:
		}

		// Update current position date if needed
		if f.currentPosition.Date != date {
			f.currentPosition.Date = date
			f.currentPosition.Index = 0
		}

		log.Printf("Streaming transactions for date: %s, starting from index: %d",
			f.currentPosition.Date, f.currentPosition.Index)

		// Stream all transactions for this date
		if err := f.streamDate(date, positionTicker); err != nil {
			return fmt.Errorf("failed to stream date %s: %w", date, err)
		}

		// Move to next date
		f.currentPosition.Index = 0
	}

	log.Printf("Completed streaming target range")
	return nil
}

// streamFromCurrentPosition streams from current position indefinitely
func (f *TxStreamFeeder) streamFromCurrentPosition() error {
	if !f.currentPosition.IsValid() {
		return fmt.Errorf("invalid current position: %v", f.currentPosition)
	}

	log.Printf("Starting to stream from position: %s", f.currentPosition.String())

	// Set up position saving ticker
	positionTicker := time.NewTicker(f.saveInterval)
	defer positionTicker.Stop()

	// Stream continuously from current position
	for {
		select {
		case <-f.stopChan:
			log.Printf("Streaming stopped by user")
			return nil
		default:
		}

		if err := f.streamDate(f.currentPosition.Date, positionTicker); err != nil {
			log.Printf("Error streaming date %s: %v", f.currentPosition.Date, err)
			// Move to next date and continue
			if nextDate, err := f.getNextDate(f.currentPosition.Date); err == nil {
				f.currentPosition.Date = nextDate
				f.currentPosition.Index = 0
				log.Printf("Moving to next date: %s", nextDate)
			} else {
				log.Printf("Cannot determine next date, stopping: %v", err)
				return err
			}
		}
	}
}

// streamDate streams all transactions for a specific date
func (f *TxStreamFeeder) streamDate(date string, positionTicker *time.Ticker) error {
	for {
		// Check if we should stop
		select {
		case <-f.stopChan:
			return nil
		case <-positionTicker.C:
			// Save position periodically
			if err := f.saveCurrentPosition(); err != nil {
				log.Printf("Error saving position: %v", err)
			}
		default:
		}

		// Get next batch of transactions
		transactions, err := f.txStreamer.GetTenThousandsTx(date, f.currentPosition.Index)
		if err != nil {
			return fmt.Errorf("failed to get transactions: %w", err)
		}

		// If no transactions, we've reached the end of this date
		if len(transactions) == 0 {
			log.Printf("Completed streaming date: %s", date)
			return nil
		}

		log.Printf("Got %d transactions for date %s, index %d",
			len(transactions), date, f.currentPosition.Index)

		// Send transactions to Kafka with retry
		if err := f.sendTransactionsWithRetry(transactions); err != nil {
			return fmt.Errorf("failed to send transactions: %w", err)
		}

		// Update position (semi-open interval: next index to fetch)
		f.currentPosition.Index += int64(len(transactions))

		log.Printf("Successfully sent %d transactions, updated position: %s",
			len(transactions), f.currentPosition.String())
	}
}

// sendTransactionsWithRetry sends transactions to Kafka with retry logic
func (f *TxStreamFeeder) sendTransactionsWithRetry(transactions []shareddomain.RawTransaction) error {
	messages := make([]kafka.Message[shareddomain.RawTransaction], len(transactions))

	for i, tx := range transactions {
		// Use TxId as key for partitioning
		key := []byte(tx.TxId)
		messages[i] = kafka.Message[shareddomain.RawTransaction]{
			Key:   key,
			Value: tx,
		}
	}

	var lastErr error
	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if err := f.kafkaProducer.SendDirect(messages); err != nil {
			lastErr = err
			log.Printf("Attempt %d failed to send transactions: %v", attempt+1, err)

			if attempt < f.maxRetries {
				time.Sleep(f.retryDelay)
				continue
			}
		} else {
			// Success
			return nil
		}
	}

	return fmt.Errorf("failed to send transactions after %d attempts: %w", f.maxRetries+1, lastErr)
}

// saveCurrentPosition saves the current position
func (f *TxStreamFeeder) saveCurrentPosition() error {
	if !f.currentPosition.IsValid() {
		return fmt.Errorf("invalid current position")
	}

	return f.positionRepo.SavePosition(f.currentPosition)
}

// getNextDate gets the next date after the given date
func (f *TxStreamFeeder) getNextDate(currentDate string) (string, error) {
	date, err := time.Parse("2006-01-02", currentDate)
	if err != nil {
		return "", fmt.Errorf("invalid date format: %w", err)
	}

	nextDate := date.AddDate(0, 0, 1)
	return nextDate.Format("2006-01-02"), nil
}

// GetCurrentPosition returns the current streaming position
func (f *TxStreamFeeder) GetCurrentPosition() domain.StreamPosition {
	return f.currentPosition
}

// IsRunning returns whether the feeder is currently running
func (f *TxStreamFeeder) IsRunning() bool {
	return f.isRunning
}

// GetStats returns statistics about the feeder
func (f *TxStreamFeeder) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"is_running":       f.isRunning,
		"current_position": f.currentPosition,
		"topic":            f.topic,
		"total_produced":   f.kafkaProducer.GetTotalProduced(),
		"total_errors":     f.kafkaProducer.GetTotalErrors(),
		"producer_mode":    f.kafkaProducer.GetMode(),
	}
}
