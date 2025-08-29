package eventbus

import (
	"fmt"
	"path/filepath"
	"strings"
)

func DotJsonl(s string) string {
	return fmt.Sprintf("%s.jsonl", s)
}

// .jsonl 확장자 강제
func ensureJSONLExt(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".jsonl" {
		return strings.TrimSuffix(path, ext) + ".jsonl"
	}
	return path
}
