package app

// undirected pair key
func undirectedPair(a, b string) string {
	if b < a {
		a, b = b, a
	}
	return a + "|" + b
}

// 병렬 메타 주석화
func annotateParallel(edges []EdgeInfo) []EdgeInfo {
	groups := map[string][]int{} // pair -> indices
	for i := range edges {
		p := edges[i].Pair
		if p == "" {
			lo, hi := edges[i].Source, edges[i].Target
			if hi < lo {
				lo, hi = hi, lo
			}
			p = lo + "|" + hi
			edges[i].Pair = p
		}
		groups[p] = append(groups[p], i)
	}
	for _, idxs := range groups {
		n := len(idxs)
		for k, i := range idxs {
			edges[i].ParallelIndex = k
			edges[i].ParallelCount = n
		}
	}
	return edges
}
