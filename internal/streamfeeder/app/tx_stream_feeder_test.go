package app

import (
	"os"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

func TestTxStreamFeederCreation(t *testing.T) {
	// Create temporary directories for testing
	tmpDir := "/tmp/streamfeeder_test"
	defer os.RemoveAll(tmpDir)

	dataDir := tmpDir + "/data"

	os.MkdirAll(dataDir, 0755)

	// Create a simple backpressure instance for testing
	// 백프레셔(모니터) 준비
	backpressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    100000, // 모니터 쪽 스펙 (관찰 값)
			TargetSaturation: 0.5,
		},
		tools.SeedProducingConfig{SeedInterval: 1, SeedBatchSize: 1000},
		"./bp_state",
	)

	config := TxStreamFeederConfig{

		KafkaBrokers:      []string{"localhost:9092"},
		Topic:             "test-topic",
		InitialBatchSize:  100,
		InitialInterval:   1 * time.Second,
		DirectChannelSize: 1000,
		MaxBatchSize:      1000,
		MaxInterval:       60 * time.Second,
		PositionDataDir:   dataDir,
		SaveInterval:      5 * time.Second,
		MaxRetries:        3,
		RetryDelay:        1 * time.Second,
	}

	feeder, err := NewTxStreamFeeder(config, backpressure)
	if err != nil {
		t.Fatalf("Failed to create TxStreamFeeder: %v", err)
	}

	if feeder == nil {
		t.Fatal("TxStreamFeeder is nil")
	}

	if feeder.IsRunning() {
		t.Error("TxStreamFeeder should not be running initially")
	}

	// Test stats
	stats := feeder.GetStats()
	if stats["is_running"].(bool) != false {
		t.Error("Stats should show not running")
	}
}

func TestStreamPositionValidation(t *testing.T) {
	tests := []struct {
		name     string
		position domain.StreamPosition
		valid    bool
	}{
		{
			name:     "Valid position",
			position: domain.NewStreamPosition("2023-01-01", 0),
			valid:    true,
		},
		{
			name:     "Valid position with index",
			position: domain.NewStreamPosition("2023-12-31", 1000),
			valid:    true,
		},
		{
			name:     "Invalid date format",
			position: domain.NewStreamPosition("2023-1-1", 0),
			valid:    false,
		},
		{
			name:     "Empty date",
			position: domain.NewStreamPosition("", 0),
			valid:    false,
		},
		{
			name:     "Negative index",
			position: domain.NewStreamPosition("2023-01-01", -1),
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.position.IsValid(); got != tt.valid {
				t.Errorf("position.IsValid() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestStreamTargetValidation(t *testing.T) {
	tests := []struct {
		name   string
		target domain.StreamTarget
		valid  bool
	}{
		{
			name:   "Valid range same day",
			target: domain.NewStreamTarget("2023-01-01", "2023-01-01"),
			valid:  true,
		},
		{
			name:   "Valid range multiple days",
			target: domain.NewStreamTarget("2023-01-01", "2023-01-31"),
			valid:  true,
		},
		{
			name:   "Invalid range - start after end",
			target: domain.NewStreamTarget("2023-01-31", "2023-01-01"),
			valid:  false,
		},
		{
			name:   "Invalid date format",
			target: domain.NewStreamTarget("2023-1-1", "2023-1-31"),
			valid:  false,
		},
		{
			name:   "Empty dates",
			target: domain.NewStreamTarget("", ""),
			valid:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.target.IsValid(); got != tt.valid {
				t.Errorf("target.IsValid() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestStreamTargetDateRange(t *testing.T) {
	target := domain.NewStreamTarget("2023-01-01", "2023-01-03")

	dates, err := target.GetDateRange()
	if err != nil {
		t.Fatalf("GetDateRange() error = %v", err)
	}

	expected := []string{"2023-01-01", "2023-01-02", "2023-01-03"}
	if len(dates) != len(expected) {
		t.Fatalf("GetDateRange() returned %d dates, want %d", len(dates), len(expected))
	}

	for i, date := range dates {
		if date != expected[i] {
			t.Errorf("GetDateRange()[%d] = %s, want %s", i, date, expected[i])
		}
	}
}

func TestStreamTargetContains(t *testing.T) {
	target := domain.NewStreamTarget("2023-01-01", "2023-01-03")

	tests := []struct {
		date     string
		contains bool
	}{
		{"2022-12-31", false},
		{"2023-01-01", true},
		{"2023-01-02", true},
		{"2023-01-03", true},
		{"2023-01-04", false},
		{"invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			if got := target.Contains(tt.date); got != tt.contains {
				t.Errorf("target.Contains(%s) = %v, want %v", tt.date, got, tt.contains)
			}
		})
	}
}
