package cface

import (
	"context"
	"io"

	nsface "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/cface"
	rsface "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/cface"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type RelPort interface {
	//ClusterDB() rsface.ClusterDB
	DispatchTraitEvent(tv ropedomain.TraitEvent) error
	GetGraphStats() map[string]any
	GetTripletPort() rsface.TripletPort
	GetCreationPort() rsface.CreationPort
	EnqueueToTriplet(v rsface.TripletEventMsg) error
	EnqueueToCreation(v rsface.CreationEventMsg) error
	DequeueTriplet() <-chan rsface.TripletEventMsg
	DequeueCreation() <-chan rsface.CreationEventMsg
	ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction
	ConsumeCreationTxByFanout() <-chan domain.MarkedTransaction
	Start(ctx context.Context) error
	io.Closer
}

type RelMsg any

type NodPort interface {
	GetCoPort() nsface.CoPort
	GetEoPort() nsface.EoPort
	// TODO 수직 슬라이스 테스트를 위해 어쩔 수 없이 만든 함수임.
	// TODO 추후 정식 생성자 통해서 모든 것을 제대로 초기화 할 것!!
	SetCoPort(nsface.CoPort)
	SetEoPort(nsface.EoPort)
	Start(ctx context.Context) error
	io.Closer
}

type NodMsg any
