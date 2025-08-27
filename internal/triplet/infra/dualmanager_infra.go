package infra

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"

type DualManagerInfra struct {
	GroundKnowledge *DomainKnowledge
	GraphRepo       app.RopeDB
	// Persistent KV storage for to->[]from mappings (대규모 데이터 처리용)
	PendingRelationRepo PendingRelationRepo
}

func NewDualManagerInfra(domainKnowledge *DomainKnowledge, graphRepo app.RopeDB, pendingRelationRepo PendingRelationRepo) *DualManagerInfra {
	return &DualManagerInfra{
		GroundKnowledge:     domainKnowledge,
		GraphRepo:           graphRepo,
		PendingRelationRepo: pendingRelationRepo,
	}
}
