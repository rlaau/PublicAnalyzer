package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletInfra struct {
	HeavyTimeBucketManager *HeavyTimeBucketManager
	WorkerPool             *workerpool.Pool
}

func (t *TripletInfra) GetTimeBucketManager() iface.TimeBuckerManagerPort {
	return t.HeavyTimeBucketManager
}

func (t TripletInfra) GetWorkerPool() *workerpool.Pool {
	return t.WorkerPool
}

func NewTripletInfra(
	workerPool *workerpool.Pool,
	hvtm *HeavyTimeBucketManager,
) *TripletInfra {
	return &TripletInfra{
		WorkerPool:             workerPool,
		HeavyTimeBucketManager: hvtm,
	}
}
