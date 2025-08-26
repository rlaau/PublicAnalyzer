package streamfeeder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/domain"
)

// FileStreamPositionRepository implements StreamPositionRepository using file storage
type FileStreamPositionRepository struct {
	filePath string
	mutex    sync.RWMutex
}

// NewFileStreamPositionRepository creates a new file-based position repository
func NewFileStreamPositionRepository(dataDir string) *FileStreamPositionRepository {
	// Create data directory if not exists
	os.MkdirAll(dataDir, 0755)
	
	return &FileStreamPositionRepository{
		filePath: filepath.Join(dataDir, "streamfeeder_position.json"),
	}
}

// SavePosition saves the current stream position to file
func (r *FileStreamPositionRepository) SavePosition(position domain.StreamPosition) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	if !position.IsValid() {
		return fmt.Errorf("invalid stream position: %v", position)
	}
	
	data, err := json.MarshalIndent(position, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal position: %w", err)
	}
	
	// Write to temporary file first, then rename for atomic operation
	tempPath := r.filePath + ".tmp"
	
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	
	if err := os.Rename(tempPath, r.filePath); err != nil {
		os.Remove(tempPath) // cleanup temp file
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	
	return nil
}

// LoadPosition loads the current stream position from file
func (r *FileStreamPositionRepository) LoadPosition() (domain.StreamPosition, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	if !r.hasPositionUnlocked() {
		return domain.StreamPosition{}, fmt.Errorf("no position file found")
	}
	
	data, err := os.ReadFile(r.filePath)
	if err != nil {
		return domain.StreamPosition{}, fmt.Errorf("failed to read position file: %w", err)
	}
	
	var position domain.StreamPosition
	if err := json.Unmarshal(data, &position); err != nil {
		return domain.StreamPosition{}, fmt.Errorf("failed to unmarshal position: %w", err)
	}
	
	if !position.IsValid() {
		return domain.StreamPosition{}, fmt.Errorf("invalid position in file: %v", position)
	}
	
	return position, nil
}

// HasPosition checks if position file exists
func (r *FileStreamPositionRepository) HasPosition() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	return r.hasPositionUnlocked()
}

// hasPositionUnlocked checks if position file exists (unlocked version)
func (r *FileStreamPositionRepository) hasPositionUnlocked() bool {
	if _, err := os.Stat(r.filePath); os.IsNotExist(err) {
		return false
	}
	return true
}

// InMemoryStreamPositionRepository implements StreamPositionRepository in memory (for testing)
type InMemoryStreamPositionRepository struct {
	position *domain.StreamPosition
	mutex    sync.RWMutex
}

// NewInMemoryStreamPositionRepository creates a new in-memory position repository
func NewInMemoryStreamPositionRepository() *InMemoryStreamPositionRepository {
	return &InMemoryStreamPositionRepository{}
}

// SavePosition saves the position in memory
func (r *InMemoryStreamPositionRepository) SavePosition(position domain.StreamPosition) error {
	if !position.IsValid() {
		return fmt.Errorf("invalid stream position: %v", position)
	}
	
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	r.position = &position
	return nil
}

// LoadPosition loads the position from memory
func (r *InMemoryStreamPositionRepository) LoadPosition() (domain.StreamPosition, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	if r.position == nil {
		return domain.StreamPosition{}, fmt.Errorf("no position found")
	}
	
	return *r.position, nil
}

// HasPosition checks if position exists in memory
func (r *InMemoryStreamPositionRepository) HasPosition() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	return r.position != nil
}