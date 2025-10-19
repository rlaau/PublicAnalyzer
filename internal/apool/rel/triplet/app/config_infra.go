package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

func NewInfraByConfig(config *TripletConfig) *infra.TripletAndDualManagerInfra {
	var isolateColusre = computation.ComputeRelClosure(config.IsolatedDBPath)
	log.Printf("EOA Analyzer의 Infra 세팅 중: %s (Mode: %s)", config.Name, config.Mode)

	// Ensure the isolated DB path exists
	if err := os.MkdirAll(config.IsolatedDBPath, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create isolated DB directory: %v", err))
	}

	// 워커 풀에 쓸 채널 생성 (메모리 전용 - 인터페이스 타입이므로 JSONL 불가)
	txJobBus := eventbus.NewInMemory[workerpool.Job](config.BusCapLimit)
	//* 워커풀 생성 및 채널 등록
	//*여기선 ctx를 그냥 백그라운드로 등록. 부모 컨텍스트는 Start시 기존 것 shuDown후 등록
	ctx := context.Background()
	workerPool := workerpool.New(ctx, config.WorkerCount, txJobBus)
	log.Printf("🔧 WorkerPool initialized with %d workers", config.WorkerCount)
	// pendingDB, err := infra.NewBadgerPendingRelationRepo(isolateColusre("pending"))
	// if err != nil {
	// 	panic("펜딜 레포지토리를 열지 못함.")
	// }
	timeBucketManager, err := infra.NewHeavyTimeBucketManager(isolateColusre("timebucket"))
	if err != nil {
		panic("타임버킷 메니져 로드 중 에러 발생")
	}
	fmt.Printf("Triplet 인프라 전부 로드 완료")
	return infra.NewTripletInfra(txJobBus, workerPool, timeBucketManager)

}

func loadKafkaBatchConsumer(mode mode.ProcessingMode, name string) *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction] {
	// Transaction Consumer 초기화 - 모드에 따라 다른 토픽 사용
	kafkaBrokers := []string{kafka.DefaultKafkaPort}
	isTestMode := mode.IsTest()
	groupID := fmt.Sprintf("triplet-analyzer-%s", strings.ReplaceAll(name, " ", "-"))
	// 배치 모드 Consumer 초기화 (고성능)
	batchSize := 100                      // 100개씩 배치 처리
	batchTimeout := 20 * time.Millisecond // 20ms 타임아웃
	var topic string
	if isTestMode {
		topic = kafka.TestFedTxTopic // 테스트용 토픽
	} else {
		topic = kafka.ProductionTxTopic // 프로덕션용 토픽
	}

	consumerConfig := kafka.KafkaBatchConfig{
		Brokers:      kafkaBrokers,
		Topic:        topic,
		GroupID:      groupID,
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
	}
	batchConsumer := kafka.NewKafkaBatchConsumer[*shareddomain.MarkedTransaction](consumerConfig)
	log.Printf("📡 Batch consumer initialized (test mode: %v, batch size: %d)", isTestMode, batchSize)
	return batchConsumer
}

// func loadCEXSet(cexFilePath string) (*shareddomain.CEXSet, error) {
// 	if cexFilePath == "" {
// 		// 기본 경로 사용 (후방 호환성)
// 		cexFilePath = "internal/triplet/cex.txt"
// 	}
// 	cexRepo := infra.NewFileCEXRepository(cexFilePath)
// 	cexSet, err := cexRepo.LoadCEXSet()
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to load CEX set from %s: %w", cexFilePath, err)
// 	}
// 	return cexSet, nil
// }
// func loadDetectedDepositSet(fileDBPath string, mode mode.ProcessingMode) (*infra.FileDepositRepository, error) {
// 	// 데이터 디렉토리 생성
// 	if err := os.MkdirAll(fileDBPath, 0755); err != nil {
// 		return nil, fmt.Errorf("failed to create data directory: %w", err)
// 	}
// 	// Deposit 저장소 초기화 - 모드에 따른 경로 설정
// 	var detectedDepositFilePath string

// 	if mode.IsTest() {
// 		detectedDepositFilePath = fileDBPath + "/test_detected_deposits.csv"
// 	} else {
// 		detectedDepositFilePath = fileDBPath + "/production_detected_deposits.csv"
// 	}
// 	depositRepo := infra.NewFileDepositRepository(detectedDepositFilePath)
// 	return depositRepo, nil
// }
