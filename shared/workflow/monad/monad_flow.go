package monad

type MonadFlow[T any] struct {
	steps []func(Monad[T]) Monad[T]
}

// 체인 추가 (step은 T → Result[T])
func (f *MonadFlow[T]) Then(fn func(Monad[T]) Monad[T]) *MonadFlow[T] {
	f.steps = append(f.steps, fn)
	return f
}

// 실행 (결과도 Result[T])
func (f *MonadFlow[T]) Run(input Monad[T]) Monad[T] {
	out := input
	for _, fn := range f.steps {
		out = fn(out)

	}
	return out

}

// 생성자
func NewMonoFlow[T any]() *MonadFlow[T] {
	return &MonadFlow[T]{}
}
