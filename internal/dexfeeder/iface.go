package dexfeeder

// StreamPositionRepository interface for persisting stream position
type StreamPositionRepository interface {
	// SavePosition saves the current stream position
	SavePosition(position StreamPosition) error

	// LoadPosition loads the current stream position
	LoadPosition() (StreamPosition, error)

	// HasPosition checks if position exists
	HasPosition() bool
}

// TxStreamFeederRepository interface for streamfeeder dependencies
type TxStreamFeederRepository interface {
	StreamPositionRepository
}
