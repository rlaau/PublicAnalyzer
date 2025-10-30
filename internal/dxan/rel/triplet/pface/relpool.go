package pface

import (
	"context"

	shared "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/pface"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type RelPort interface {
	GetDxanPort() pface.DxanPort
	DispatchTraitEvent(ropedomain.TraitEvent) error
	GetGraphStats() map[string]any
	//형제 모듈에 접근
	GetCreationPort() shared.CreationPort
	EnqueueToCreation(ctx context.Context, v shared.CreationEventMsg) error
	//자신 모듈을 소비
	DequeueTriplet() <-chan shared.TripletEventMsg
	ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction
}
