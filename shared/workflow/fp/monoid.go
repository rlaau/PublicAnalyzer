package fp

type Monoid[T any] interface {
	Empty() T
	Combine(a, b T) T
}
