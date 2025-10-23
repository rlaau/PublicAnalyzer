package domain

import (
	"fmt"
	"time"
)

// StreamPosition represents the current streaming position
type StreamPosition struct {
	Date  string `json:"date"`  // YYYY-MM-DD format
	Index int64  `json:"index"` // Semi-open interval: next record to fetch
}

// NewStreamPosition creates a new StreamPosition
func NewStreamPosition(date string, index int64) StreamPosition {
	return StreamPosition{
		Date:  date,
		Index: index,
	}
}

// String returns string representation of position
func (p StreamPosition) String() string {
	return fmt.Sprintf("Date:%s Index:%d", p.Date, p.Index)
}

// IsValid checks if position is valid
func (p StreamPosition) IsValid() bool {
	if p.Date == "" || p.Index < 0 {
		return false
	}

	// Check date format YYYY-MM-DD
	_, err := time.Parse("2006-01-02", p.Date)
	return err == nil
}

// StreamTarget represents the target range for streaming
// * 반닫힌 구간이 아닌, 모두 다 포함하는 inclusive임
type StreamTarget struct {
	StartDate string // YYYY-MM-DD format (inclusive)
	EndDate   string // YYYY-MM-DD format (inclusive)
}

// NewStreamTarget creates a new StreamTarget
func NewStreamTarget(startDate, endDate string) StreamTarget {
	return StreamTarget{
		StartDate: startDate,
		EndDate:   endDate,
	}
}

// IsValid checks if target is valid
func (t StreamTarget) IsValid() bool {
	if t.StartDate == "" || t.EndDate == "" {
		return false
	}

	// Parse dates
	start, err1 := time.Parse("2006-01-02", t.StartDate)
	end, err2 := time.Parse("2006-01-02", t.EndDate)

	if err1 != nil || err2 != nil {
		return false
	}

	// Start should be <= End
	return !start.After(end)
}

// Contains checks if given date is within target range
func (t StreamTarget) Contains(date string) bool {
	if !t.IsValid() {
		return false
	}

	checkDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return false
	}

	start, _ := time.Parse("2006-01-02", t.StartDate)
	end, _ := time.Parse("2006-01-02", t.EndDate)
	// start이상, end 이하.
	//* 이건 반닫힌 구간이 아니네.
	return !checkDate.Before(start) && !checkDate.After(end)
}

// GetDateRange returns all dates in the target range
func (t StreamTarget) GetDateRange() ([]string, error) {
	if !t.IsValid() {
		return nil, fmt.Errorf("invalid stream target: %v", t)
	}

	start, _ := time.Parse("2006-01-02", t.StartDate)
	end, _ := time.Parse("2006-01-02", t.EndDate)

	var dates []string
	current := start

	for !current.After(end) {
		dates = append(dates, current.Format("2006-01-02"))
		current = current.AddDate(0, 0, 1)
	}

	return dates, nil
}
