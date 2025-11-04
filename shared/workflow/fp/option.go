package fp

import (
	"bytes"
	"encoding/json"
)

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

// MarshalJson은 Option객체에 대해 Some->T, None->"null"로 직렬화 함
// 자동 마셜링 시에는 이게 {OK:T, V:bool}로 직렬화되는데,
// GPT랑 논의해 본 결과 명시적으로 null주는게 더 일반적인 관례라 했기에, 여기서 강제함.
func (o Option[T]) MarshalJSON() ([]byte, error) {
	if !o.Ok {
		return []byte("null"), nil
	}
	return json.Marshal(o.V) // T 규칙 그대로
}

func (o *Option[T]) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) {
		*o = None[T]()
		return nil
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*o = Some(v)
	return nil
}
