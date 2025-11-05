package app

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"

type ropeDbModel interface {
	// 이벤트 입력(내부적으로 Vertex 동기 갱신 + Rope/Trait Upsert 큐잉)
	HandleTraitEvent(ev domain.TraitEvent) error
}
