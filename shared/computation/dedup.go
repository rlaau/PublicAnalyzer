package computation

// 1) T가 comparable일 때: 가장 단순하고 빠름 (map[T]struct{} 사용)
func DedupAppend[T comparable](base, inc []T) []T {
	if len(inc) == 0 {
		return base
	}
	out := make([]T, 0, len(base)+len(inc))
	seen := make(map[T]struct{}, len(base)+len(inc))

	for _, x := range base {
		if _, ok := seen[x]; ok {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	for _, y := range inc {
		if _, ok := seen[y]; ok {
			continue
		}
		seen[y] = struct{}{}
		out = append(out, y)
	}
	return out
}
