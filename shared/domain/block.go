package domain

import "time"

type BlockNumber uint64

// Block은 이더리움 블록의 도메인 모델입니다
type Block struct {
	Number       BlockNumber
	Hash         string
	Timestamp    time.Time
	Transactions []MarkedTransaction
}
