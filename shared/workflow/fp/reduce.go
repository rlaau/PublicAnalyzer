package fp

func Reduce[E, A any](xs []E, init A, f func(A, E) A) A {
	acc := init
	for _, e := range xs {
		acc = f(acc, e)
	}
	return acc
}

func ReduceMonoid[T any](xs []T, M Monoid[T]) T {
	acc := M.Empty()
	for _, x := range xs {
		acc = M.Combine(acc, x)
	}
	return acc
}
