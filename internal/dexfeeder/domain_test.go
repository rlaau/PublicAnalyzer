package dexfeeder

import (
	"testing"
	"time"
)

func TestStreamPositionIsValid(t *testing.T) {
	tests := []struct {
		name     string
		pos      StreamPosition
		expected bool
	}{
		{
			name:     "valid position",
			pos:      NewStreamPosition("2023-09-01", 0),
			expected: true,
		},
		{
			name:     "valid position with index",
			pos:      NewStreamPosition("2023-12-31", 12345),
			expected: true,
		},
		{
			name:     "empty date",
			pos:      NewStreamPosition("", 0),
			expected: false,
		},
		{
			name:     "negative index",
			pos:      NewStreamPosition("2023-09-01", -1),
			expected: false,
		},
		{
			name:     "invalid date format",
			pos:      NewStreamPosition("2023/09/01", 0),
			expected: false,
		},
		{
			name:     "invalid date format 2",
			pos:      NewStreamPosition("09-01-2023", 0),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.pos.IsValid()
			if result != tt.expected {
				t.Errorf("IsValid() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestStreamTargetIsValid(t *testing.T) {
	tests := []struct {
		name      string
		startDate time.Time
		endDate   time.Time
		expected  bool
	}{
		{
			name:      "valid range",
			startDate: mustParseDate("2023-09-01"),
			endDate:   mustParseDate("2023-12-31"),
			expected:  true,
		},
		{
			name:      "same date",
			startDate: mustParseDate("2023-09-01"),
			endDate:   mustParseDate("2023-09-01"),
			expected:  true,
		},
		{
			name:      "start after end",
			startDate: mustParseDate("2023-12-31"),
			endDate:   mustParseDate("2023-09-01"),
			expected:  false,
		},
		{
			name:      "zero time",
			startDate: time.Time{},
			endDate:   mustParseDate("2023-12-31"),
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, ok := NewStreamTarget(tt.startDate, tt.endDate)
			if ok != tt.expected {
				t.Errorf("NewStreamTarget().ok = %v, expected %v", ok, tt.expected)
			}
			if result := target.IsValid(); result != tt.expected {
				t.Errorf("IsValid() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestStreamTargetContains(t *testing.T) {
	target, _ := NewStreamTarget(
		mustParseDate("2023-09-01"),
		mustParseDate("2023-09-30"),
	)

	tests := []struct {
		name     string
		date     string
		expected bool
	}{
		{"start date", "2023-09-01", true},
		{"end date", "2023-09-30", true},
		{"middle date", "2023-09-15", true},
		{"before start", "2023-08-31", false},
		{"after end", "2023-10-01", false},
		{"invalid format", "2023/09/15", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := target.Contains(tt.date)
			if result != tt.expected {
				t.Errorf("Contains(%s) = %v, expected %v", tt.date, result, tt.expected)
			}
		})
	}
}

func TestStreamTargetGetDateRange(t *testing.T) {
	tests := []struct {
		name          string
		startDate     time.Time
		endDate       time.Time
		expectedCount int
		firstDate     string
		lastDate      string
	}{
		{
			name:          "single day",
			startDate:     mustParseDate("2023-09-01"),
			endDate:       mustParseDate("2023-09-01"),
			expectedCount: 1,
			firstDate:     "2023-09-01",
			lastDate:      "2023-09-01",
		},
		{
			name:          "one week",
			startDate:     mustParseDate("2023-09-01"),
			endDate:       mustParseDate("2023-09-07"),
			expectedCount: 7,
			firstDate:     "2023-09-01",
			lastDate:      "2023-09-07",
		},
		{
			name:          "across month boundary",
			startDate:     mustParseDate("2023-09-28"),
			endDate:       mustParseDate("2023-10-03"),
			expectedCount: 6,
			firstDate:     "2023-09-28",
			lastDate:      "2023-10-03",
		},
		{
			name:          "full year",
			startDate:     mustParseDate("2023-01-01"),
			endDate:       mustParseDate("2023-12-31"),
			expectedCount: 365,
			firstDate:     "2023-01-01",
			lastDate:      "2023-12-31",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, ok := NewStreamTarget(tt.startDate, tt.endDate)
			if !ok {
				t.Fatalf("NewStreamTarget() failed")
			}

			dates, err := target.GetDateRange()
			if err != nil {
				t.Fatalf("GetDateRange() error = %v", err)
			}

			if len(dates) != tt.expectedCount {
				t.Errorf("GetDateRange() count = %d, expected %d", len(dates), tt.expectedCount)
			}

			if len(dates) > 0 {
				if dates[0] != tt.firstDate {
					t.Errorf("first date = %s, expected %s", dates[0], tt.firstDate)
				}
				if dates[len(dates)-1] != tt.lastDate {
					t.Errorf("last date = %s, expected %s", dates[len(dates)-1], tt.lastDate)
				}
			}
		})
	}
}

func TestGetNextDate(t *testing.T) {
	sb := &DexStreamBus{}

	tests := []struct {
		name     string
		date     string
		expected string
		wantErr  bool
	}{
		{
			name:     "normal date",
			date:     "2023-09-01",
			expected: "2023-09-02",
			wantErr:  false,
		},
		{
			name:     "end of month",
			date:     "2023-09-30",
			expected: "2023-10-01",
			wantErr:  false,
		},
		{
			name:     "end of year",
			date:     "2023-12-31",
			expected: "2024-01-01",
			wantErr:  false,
		},
		{
			name:     "invalid format",
			date:     "2023/09/01",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sb.getNextDate(tt.date)
			if (err != nil) != tt.wantErr {
				t.Errorf("getNextDate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("getNextDate() = %s, expected %s", result, tt.expected)
			}
		})
	}
}

// Helper function
func mustParseDate(date string) time.Time {
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		panic(err)
	}
	return t
}
