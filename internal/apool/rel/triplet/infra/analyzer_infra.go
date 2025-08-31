package infra

import (
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletAndDualManagerInfra struct {
	PendingRelationRepo PendingRelationRepo
	TimeBucketManager   *TimeBucketManager
	TxJobBus            *eventbus.EventBus[workerpool.Job]
	WorkerPool          *workerpool.Pool
	//TODO 추후 이것도 지울 것!! 얘가 직적 컨슈머를 왜 받누...
	TxBus *eventbus.EventBus[*shareddomain.MarkedTransaction]
}

func NewTripletInfra(
	txJobChannel *eventbus.EventBus[workerpool.Job], workerPool *workerpool.Pool,
	batchBus *eventbus.EventBus[*shareddomain.MarkedTransaction], pendingRelationRepo PendingRelationRepo,
	TimeBucketManager *TimeBucketManager,
) *TripletAndDualManagerInfra {
	return &TripletAndDualManagerInfra{
		TxJobBus:            txJobChannel,
		WorkerPool:          workerPool,
		TxBus:               batchBus,
		PendingRelationRepo: pendingRelationRepo,
		TimeBucketManager:   TimeBucketManager,
	}
}
