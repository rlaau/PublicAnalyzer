package infra

type DualManagerInfra struct {
	//GroundKnowledge *DomainKnowledge
	// Persistent KV storage for to->[]from mappings (대규모 데이터 처리용)
	PendingRelationRepo PendingRelationRepo
	TimeBucketManager   *TimeBucketManager
}

func NewDualManagerInfra(pendingRelationRepo PendingRelationRepo, timeBuckMgr *TimeBucketManager) *DualManagerInfra {
	return &DualManagerInfra{
		PendingRelationRepo: pendingRelationRepo,
		TimeBucketManager:   timeBuckMgr,
	}
}
