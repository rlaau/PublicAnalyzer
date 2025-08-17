package domain

// ! 현재 id는 사용하지 않는 도메인임.
type IsContract bool

// ID는 1부터 부여받음
type ID uint32

type IDInfo struct {
	ID         ID
	IsContract IsContract
}
