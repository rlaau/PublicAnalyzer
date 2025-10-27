package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletInfra struct {
	HeavyTimeBucketManager *HeavyTimeBucketManager
	WorkerPool             *workerpool.Pool
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
