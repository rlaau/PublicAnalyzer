package cface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type TripletEventMsg any
type TripletPort interface {
	gface.Child
	gface.Module

	//ProcessTransactions(txs []*domain.MarkedTransaction) error
	// 상태 조회
	GetStatistics() map[string]any
	IsHealthy() bool
	GetJobChannelStatus() (usage int, capacity int)

	GetRopeDBStats() map[string]any
}

type CreationPort interface {
	gface.Child
	gface.Module
}

type CreationEventMsg any
