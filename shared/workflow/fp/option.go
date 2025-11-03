package fp

type Option[T any] struct {
	V  T
	Ok bool
}

func Some[T any](v T) Option[T] { return Option[T]{V: v, Ok: true} }

func None[T any]() Option[T] {
	var z T
	return Option[T]{V: z, Ok: false}
}
func (o Option[T]) IsSome() bool     { return o.Ok }
func (o Option[T]) IsNone() bool     { return !o.Ok }
func (o Option[T]) Value() (T, bool) { return o.V, o.Ok }

func (o Option[T]) Unwrap() T { // None이면 T의 제로값
	if o.Ok {
		return o.V
	}
	var z T
	return z
}

func (o Option[T]) MustUnwrap() T {
	if o.Ok {
		return o.V
	}
	panic("called MustUnwrap() on None Option")
}

func (o Option[T]) UnwrapOr(def T) T {
	if o.Ok {
		return o.V
	}
	return def
}
func (o Option[T]) UnwrapOrErr(err error) (T, error) {
	if o.Ok {
		return o.V, nil
	}
	var z T
	return z, err
}
