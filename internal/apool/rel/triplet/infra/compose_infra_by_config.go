package infra

import (
	"fmt"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

type TripletInfraConfig struct {
	Mode        mode.ProcessingMode
	WorkerCount int
}

func NewTripletInfraByConfig(config TripletInfraConfig) *TripletInfra {
	workerPool := workerpool.New(config.WorkerCount, "TripletWorkerPool")
	log.Printf("🔧 WorkerPool initialized with %d workers", config.WorkerCount)
	timeBucketManager, err := NewHeavyTimeBucketManager(config.Mode)
	if err != nil {
		panic("타임버킷 메니져 로드 중 에러 발생")
	}
	fmt.Printf("Triplet 인프라 전부 로드 완료")
	return NewTripletInfra(workerPool, timeBucketManager)

}
