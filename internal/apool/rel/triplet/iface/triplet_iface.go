package iface

import (
	ldomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletInfraPort interface {
	GetTimeBucketManager() TimeBuckerManager
	GetWorkerPool() *workerpool.Pool
}
type TimeBuckerManager interface {
	Load() error
	Close() error
	AddIfAbsent(to domain.Address, fromScala ldomain.FromScala) (bool, int)
	GetFromScalas(toAddr domain.Address) ldomain.FromScalas
}
