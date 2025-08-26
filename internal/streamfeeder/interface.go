package streamfeeder

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/domain"
)

// StreamPositionRepository interface for persisting stream position
type StreamPositionRepository interface {
	// SavePosition saves the current stream position
	SavePosition(position domain.StreamPosition) error
	
	// LoadPosition loads the current stream position
	LoadPosition() (domain.StreamPosition, error)
	
	// HasPosition checks if position exists
	HasPosition() bool
}

// TxStreamFeederRepository interface for streamfeeder dependencies
type TxStreamFeederRepository interface {
	StreamPositionRepository
}