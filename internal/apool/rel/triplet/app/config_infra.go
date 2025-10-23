package app

import (
	"fmt"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

func NewInfraByConfig(config *TripletConfig) *infra.TripletAndDualManagerInfra {

	log.Printf("EOA Analyzer의 Infra 세팅 중: %s (Mode: %s)", config.Name, config.Mode)

	// 워커 풀에 쓸 채널 생성 (메모리 전용 - 인터페이스 타입이므로 JSONL 불가)
	txJobBus := eventbus.NewInMemory[workerpool.Job](config.BusCapLimit, "TxJobBus")
	//* 워커풀 생성 및 채널 등록
	//*여기선 ctx를 그냥 백그라운드로 등록. 부모 컨텍스트는 Start시 기존 것 shuDown후 등록
	workerPool := workerpool.New(config.WorkerCount, txJobBus, "TripletWorkerPool")
	log.Printf("🔧 WorkerPool initialized with %d workers", config.WorkerCount)
	// pendingDB, err := infra.NewBadgerPendingRelationRepo(isolateColusre("pending"))
	// if err != nil {
	// 	panic("펜딜 레포지토리를 열지 못함.")
	// }
	timeBucketManager, err := infra.NewHeavyTimeBucketManager(config.Mode)
	if err != nil {
		panic("타임버킷 메니져 로드 중 에러 발생")
	}
	fmt.Printf("Triplet 인프라 전부 로드 완료")
	return infra.NewTripletInfra(txJobBus, workerPool, timeBucketManager)

}
