package infra

type DualManagerInfra struct {
	GroundKnowledge *DomainKnowledge
	// Persistent KV storage for to->[]from mappings (대규모 데이터 처리용)
	PendingRelationRepo PendingRelationRepo
}

func NewDualManagerInfra(domainKnowledge *DomainKnowledge, pendingRelationRepo PendingRelationRepo) *DualManagerInfra {
	return &DualManagerInfra{
		GroundKnowledge:     domainKnowledge,
		PendingRelationRepo: pendingRelationRepo,
	}
}
