package computation

import (
	"os"
	"path/filepath"
)

func GetModuleRoot() string {
	cwd, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(cwd, "go.mod")); err == nil {
			return cwd
		}
		parent := filepath.Dir(cwd)
		if parent == cwd { // 루트까지 왔는데 못 찾으면 중단
			break
		}
		cwd = parent
	}
	return ""
}
