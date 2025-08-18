package fp

func Map[T, R any](xs []T, f func(T) R) []R {
	out := make([]R, len(xs))
	for i, x := range xs {
		out[i] = f(x)
	}
	return out
}
func MapOrError[T, R any](xs []T, f func(T) (R, error)) ([]R, error) {
	out := make([]R, len(xs))
	var err error
	for i, x := range xs {
		out[i], err = f(x)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
