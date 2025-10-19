package cface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type TripletEventMsg any
type TripletPort interface {
	gface.Child
	gface.Module
	// 분석기 생명주기 관리
	// Stop() error

	// 트랜잭션 처리
	//ProcessTransaction(tx *domain.MarkedTransaction) error
	//ProcessTransactions(txs []*domain.MarkedTransaction) error
	GetMyTx() <-chan domain.MarkedTransaction
	// 상태 조회
	GetStatistics() map[string]any
	IsHealthy() bool
	GetChannelStatus() (usage int, capacity int)

	GetRopeDBStats() map[string]any
}

type CreationPort interface {
	gface.Child
	gface.Module
}

type CreationEventMsg any
