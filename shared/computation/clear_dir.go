package computation

import (
	"fmt"
	"os"
	"path/filepath"
)

func SetCleanedDir(path string) error {
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		// 디렉토리가 없다면 새로 만들기
		if os.IsNotExist(err) {
			return os.MkdirAll(path, 0o755)
		}
		return err
	}
	for _, entry := range dirEntries {
		entryPath := filepath.Join(path, entry.Name())
		if err := os.RemoveAll(entryPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", entryPath, err)
		}
	}
	return nil
}
