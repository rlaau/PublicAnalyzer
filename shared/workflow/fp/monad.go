package fp

type Monad[T any] struct {
	Val T
	Err error
}

func Ok[T any](v T) Monad[T] {
	return Monad[T]{Val: v}
}

func Err[T any](e error) Monad[T] {
	return Monad[T]{Err: e}
}

// 에러 무시 & 디폴트 반환
func (r Monad[T]) UnwrapOr(defaultVal T) T {
	if r.Err != nil {
		return defaultVal
	}
	return r.Val
}

// 명시적 언래핑
func (r Monad[T]) Unwrap() (T, error) {
	return r.Val, r.Err
}
