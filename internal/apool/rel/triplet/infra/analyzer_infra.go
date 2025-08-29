package infra

import (
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TotalEOAAnalyzerInfra struct {
	GroundKnowledge     *DomainKnowledge
	PendingRelationRepo PendingRelationRepo
	TxJobBus            *eventbus.EventBus[workerpool.Job]
	WorkerPool          *workerpool.Pool
	BatchConsumer       *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction]
}

func NewTripletInfra(domainKnowledge *DomainKnowledge,
	txJobChannel *eventbus.EventBus[workerpool.Job], workerPool *workerpool.Pool,
	batchConsumer *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction], pendingRelationRepo PendingRelationRepo,
) *TotalEOAAnalyzerInfra {
	return &TotalEOAAnalyzerInfra{
		GroundKnowledge:     domainKnowledge,
		TxJobBus:            txJobChannel,
		WorkerPool:          workerPool,
		BatchConsumer:       batchConsumer,
		PendingRelationRepo: pendingRelationRepo,
	}
}
