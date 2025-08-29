package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TotalEOAAnalyzerInfra struct {
	GroundKnowledge     *DomainKnowledge
	GraphRepo           app.RopeDB
	PendingRelationRepo PendingRelationRepo
	TxJobChannel        chan workerpool.Job
	WorkerPool          *workerpool.Pool
	BatchConsumer       *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction]
}

func NewEOAInfra(domainKnowledge *DomainKnowledge, graphRepo app.RopeDB,
	txJobChannel chan workerpool.Job, workerPool *workerpool.Pool,
	batchConsumer *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction], pendingRelationRepo PendingRelationRepo,
) *TotalEOAAnalyzerInfra {
	return &TotalEOAAnalyzerInfra{
		GroundKnowledge:     domainKnowledge,
		GraphRepo:           graphRepo,
		TxJobChannel:        txJobChannel,
		WorkerPool:          workerPool,
		BatchConsumer:       batchConsumer,
		PendingRelationRepo: pendingRelationRepo,
	}
}
