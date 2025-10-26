package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletAndDualManagerInfra struct {
	HeavyTimeBucketManager *HeavyTimeBucketManager
	WorkerPool             *workerpool.Pool
}

func NewTripletInfra(
	workerPool *workerpool.Pool,
	hvtm *HeavyTimeBucketManager,
) *TripletAndDualManagerInfra {
	return &TripletAndDualManagerInfra{
		WorkerPool:             workerPool,
		HeavyTimeBucketManager: hvtm,
	}
}
