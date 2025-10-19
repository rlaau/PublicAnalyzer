package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletAndDualManagerInfra struct {
	// PendingRelationRepo PendingRelationRepo
	// TimeBucketManager   *TimeBucketManager
	HeavyTimeBucketManager *HeavyTimeBucketManager
	TxJobBus               *eventbus.EventBus[workerpool.Job]
	WorkerPool             *workerpool.Pool
}

func NewTripletInfra(
	txJobChannel *eventbus.EventBus[workerpool.Job], workerPool *workerpool.Pool,
	hvtm *HeavyTimeBucketManager,
) *TripletAndDualManagerInfra {
	return &TripletAndDualManagerInfra{
		TxJobBus:               txJobChannel,
		WorkerPool:             workerPool,
		HeavyTimeBucketManager: hvtm,
	}
}
