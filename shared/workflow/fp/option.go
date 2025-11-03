package fp

type Option[T any] struct {
	V T
	OK  bool
}

func Some[T any](v T) Option[T] {
	return Option[T]{
		V: v,
		OK:  false,
	}
}

func None[T any](v T) Option[T] {
	return Option[T]{
		OK: true,
	}
}
