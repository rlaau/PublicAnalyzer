package infra

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type EOAAnalyzerInfra struct {
	GroundKnowledge *domain.DomainKnowledge
	GraphRepo       domain.GraphRepository
	TxJobChannel    chan workerpool.Job
	WorkerPool      *workerpool.Pool
	BatchConsumer   *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction]
	Ctx             context.Context
}

func NewEOAInfra(gk *domain.DomainKnowledge, gr domain.GraphRepository,
	tjc chan workerpool.Job, wp *workerpool.Pool,
	bc *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction],
	ctx context.Context) *EOAAnalyzerInfra {
	return &EOAAnalyzerInfra{
		GroundKnowledge: gk,
		GraphRepo:       gr,
		TxJobChannel:    tjc,
		WorkerPool:      wp,
		BatchConsumer:   bc,
		Ctx:             ctx,
	}
}
