package app

import (
	"fmt"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

func NewInfraByConfig(config TripletConfig) *infra.TripletInfra {
	log.Printf("EOA Analyzer의 Infra 세팅 중: %s (Mode: %s)", config.Name, config.Mode)
	workerPool := workerpool.New(config.WorkerCount, "TripletWorkerPool")
	log.Printf("🔧 WorkerPool initialized with %d workers", config.WorkerCount)
	timeBucketManager, err := infra.NewHeavyTimeBucketManager(config.Mode)
	if err != nil {
		panic("타임버킷 메니져 로드 중 에러 발생")
	}
	fmt.Printf("Triplet 인프라 전부 로드 완료")
	return infra.NewTripletInfra(workerPool, timeBucketManager)

}
