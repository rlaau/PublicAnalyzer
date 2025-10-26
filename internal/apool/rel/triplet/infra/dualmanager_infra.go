package infra

type DualManagerInfra struct {
	//GroundKnowledge *DomainKnowledge
	// 통합된 메모리 기반 매니저 (TimeBucketManager + PendingRelationRepo 기능 통합)
	HeavyTimeBucketManager *HeavyTimeBucketManager
	//TODO 궁극적으론 DB기반이 나을 수 있지만.... 너무 너무 너무 너무 느렸음.
	// // 레거시 호환성을 위해 유지 (새로운 구현에서는 사용되지 않음)
	// // 향후 제거 예정
	// PendingRelationRepo PendingRelationRepo
	// TimeBucketManager   *TimeBucketManager
}

// NewDualManagerInfra 새로운 HeavyTimeBucketManager 기반 생성자
func NewDualManagerInfra(heavyTimeBucketMgr *HeavyTimeBucketManager) *DualManagerInfra {
	return &DualManagerInfra{
		HeavyTimeBucketManager: heavyTimeBucketMgr,
	}
}
