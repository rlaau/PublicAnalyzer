package infra

import (
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletAndDualManagerInfra struct {
	PendingRelationRepo PendingRelationRepo
	TimeBucketManager   *TimeBucketManager
	TxJobBus            *eventbus.EventBus[workerpool.Job]
	WorkerPool          *workerpool.Pool
	//TODO 추후 이것도 지울 것!! 얘가 직적 컨슈머를 왜 받누...
	BatchConsumer *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction]
}

func NewTripletInfra(
	txJobChannel *eventbus.EventBus[workerpool.Job], workerPool *workerpool.Pool,
	batchConsumer *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction], pendingRelationRepo PendingRelationRepo,
	TimeBucketManager *TimeBucketManager,
) *TripletAndDualManagerInfra {
	return &TripletAndDualManagerInfra{
		TxJobBus:            txJobChannel,
		WorkerPool:          workerPool,
		BatchConsumer:       batchConsumer,
		PendingRelationRepo: pendingRelationRepo,
		TimeBucketManager:   TimeBucketManager,
	}
}
