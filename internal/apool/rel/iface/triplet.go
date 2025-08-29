package iface

import (
	"context"
	"io"

	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type TripletEventMsg any
type TripletPort interface {
	// 분석기 생명주기 관리
	Start(ctx context.Context) error
	Stop() error

	// 트랜잭션 처리
	ProcessTransaction(tx *shareddomain.MarkedTransaction) error
	ProcessTransactions(txs []*shareddomain.MarkedTransaction) error

	// 상태 조회
	GetStatistics() map[string]any
	IsHealthy() bool
	GetChannelStatus() (usage int, capacity int)

	GetRopeDBStats() map[string]any

	// 리소스 관리
	io.Closer
}
