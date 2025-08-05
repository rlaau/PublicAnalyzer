package infra

type DualManagerInfra struct {
	GroundKnowledge *GroundKnowledge
	GraphRepo       GraphRepository
	// Persistent KV storage for to->[]from mappings (대규모 데이터 처리용)
	PendingRelationRepo PendingRelationRepo
}

func NewDualManagerInfra(domainKnowledge *GroundKnowledge, graphRepo GraphRepository, pendingRelationRepo PendingRelationRepo) *DualManagerInfra {
	return &DualManagerInfra{
		GroundKnowledge:     domainKnowledge,
		GraphRepo:           graphRepo,
		PendingRelationRepo: pendingRelationRepo,
	}
}
