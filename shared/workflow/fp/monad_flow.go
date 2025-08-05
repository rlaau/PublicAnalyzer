package fp

import "errors"

type MonadFlow[T any] struct {
	steps []func(Monad[T]) Monad[T]
	input T
}

// 생성자
func NewMonadFlow[T any]() *MonadFlow[T] {
	return &MonadFlow[T]{}
}

func (f *MonadFlow[T]) RegisterInput(input T) *MonadFlow[T] {
	f.input = input
	return f

}

// ErrorSkipStep은 진짜 에러 아님에도 모나드 스텝을 스킵하는 용도로만 쓰는 에러.
// 모나드 체이닝 시엔 에러로 여겨져서 스킵하지만, Run에서 최종 Unwrap시엔 에러로 처리하지 않음.
var ErrorSkipStep = errors.New("불필요한 처리 생략 위한 조기 종료용 에러임. 런타임 에러가 아니며, 정상 작동입니다.")

func IsSkipMonadStep(err error) bool {
	return errors.Is(err, ErrorSkipStep)
}

// HandleErrOrNilToSkipStep는 에러가 있건 없건 적절한 에러 부여해서 모나드 체인 스킵해주는 함수임.
// nil시엔 ErrorSkipStep으로 이를 변환하여, 모나드 체인이 과정에선 에러로 처리, 결과에선 정상 작동으로 처리시킴.
func HandleErrOrNilToSkipStep(errOrNil error) error {
	if errOrNil != nil {
		err := errOrNil
		return err
	}
	//에러가 없지만 조기 종료시킬 경우 커스텀 에러 사용
	return ErrorSkipStep
}

// MonadStep은 모나드 체인에 쓰일 스텝의 타입
type MonadStep[T any] func(input Monad[T]) Monad[T]

// RawStep은 실전에서 쓰일 실제 함수를 말함
// 래핑되지 않은 날것의 함수는 모나드를 고려하지 않고 자신의 로직만을 책임짐
type RawStep[T any] func(input T) (T, error)

// AsStep은 일반 함수를 모나드 스텝용 함수로 변환해주는 함수임.
// 에러가 있을 시 에러를 그대로 넘기고, 정상 값일 때만 체이닝 이어감
func AsStep[T any](fn RawStep[T]) MonadStep[T] {
	return func(m Monad[T]) Monad[T] {
		v, err := m.Unwrap()
		if err != nil {
			return m
		}
		ok, err := fn(v)
		if err != nil {
			return Err[T](err)
		}
		return Ok(ok)
	}
}

// Then은 RawStep함수를 AsStep함수로 래핑해서 자신의 모나드 스텝에 추가함
func (f *MonadFlow[T]) Then(fn RawStep[T]) *MonadFlow[T] {
	f.steps = append(f.steps, AsStep(fn))
	return f
}

// 실행. 결과는 unwrap당하며, 스킵용 에러는 no-error로 처리
func (f *MonadFlow[T]) Run() (T, error) {
	// Run시엔 애초에 최초 입력이 타당함을 강제함
	// Err입력이면 입력을 이전에 필터링 하고 이후 옳은 경우 모나드체인 실행하기를 강제.
	out := Ok(f.input)
	for _, fn := range f.steps {
		out = fn(out)
	}
	result, err := out.Unwrap()
	if err == ErrorSkipStep {
		//  ErrSkipMonadStep은 모나드 흐름 제어용 얼리 리턴 신호일 뿐, 진짜 에러는 아님
		err = nil
	}
	return result, err

}
