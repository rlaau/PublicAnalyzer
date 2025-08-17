package domain

type IsContract bool

// ID는 1부터 부여받음
type ID uint32

type IDInfo struct {
	ID         ID
	IsContract IsContract
}
