package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/infra"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

// TODO 이것도 추후 수정. 컨텍스트가 아오 너무 많자나
func NewInfraByConfig(config *EOAAnalyzerConfig, ctx context.Context) infra.TotalEOAAnalyzerInfra {
	log.Printf("EOA Analyzer의 Infra 세팅 중: %s (Mode: %s)", config.Name, config.Mode)
	cexSet, err := loadCEXSet(config.CEXFilePath)
	if err != nil {
		fmt.Printf("CEX set로딩 중 에러남.")
		panic("더 이상 작업 불가")
	}
	fmt.Printf("CEX 로드 완료")
	depositRepo, err := loadDetectedDepositSet(config.FileDBPath, config.Mode)
	if err != nil {
		fmt.Printf("디포짓 로딩 실패. (파일 경로: %s)", config.FileDBPath)
	}
	groundKnowledge := infra.NewDomainKnowledge(cexSet, depositRepo)
	if err := groundKnowledge.Load(); err != nil {
		panic("그라운드 놀리지를 파일->(메모리,파일)로 로드하지 못함")
	}
	log.Printf("🧠 Ground knowledge loaded")
	graphRepo, err := infra.NewBadgerGraphRepository(config.GraphDBPath)
	if err != nil {
		panic("그래프DB로드 실패")
	}
	log.Printf("🗂️  Graph repository at: %s", config.GraphDBPath)
	batchConsumer := loadKafkaBatchConsumer(config.Mode, config.Name)
	//* 워커 풀에 쓸 채널 생성
	txJobChannel := make(chan workerpool.Job, config.ChannelBufferSize)
	//* 워커풀 생성 및 채널 등록
	workerPool := workerpool.New(ctx, config.WorkerCount, txJobChannel)
	log.Printf("🔧 WorkerPool initialized with %d workers", config.WorkerCount)
	pendingDB, err := infra.NewBadgerPendingRelationRepo(config.PendingDBPath)
	if err != nil {
		panic("펜딜 레포지토리를 열지 못함.")
	}
	return *infra.NewEOAInfra(groundKnowledge, graphRepo, txJobChannel, workerPool, batchConsumer, pendingDB)

}

func loadKafkaBatchConsumer(mode AnalyzerMode, name string) *kafka.KafkaBatchConsumer[*shareddomain.MarkedTransaction] {
	// Transaction Consumer 초기화 - 모드에 따라 다른 토픽 사용
	kafkaBrokers := []string{kafka.DefaultKafkaPort}
	isTestMode := (mode == TestingMode)
	groupID := fmt.Sprintf("ee-analyzer-%s", strings.ReplaceAll(name, " ", "-"))
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
func loadCEXSet(cexFilePath string) (*shareddomain.CEXSet, error) {
	if cexFilePath == "" {
		// 기본 경로 사용 (후방 호환성)
		cexFilePath = "internal/ee/cex.txt"
	}
	cexRepo := infra.NewFileCEXRepository(cexFilePath)
	cexSet, err := cexRepo.LoadCEXSet()
	if err != nil {
		return nil, fmt.Errorf("failed to load CEX set from %s: %w", cexFilePath, err)
	}
	return cexSet, nil
}
func loadDetectedDepositSet(fileDBPath string, mode AnalyzerMode) (*infra.FileDepositRepository, error) {
	// 데이터 디렉토리 생성
	if err := os.MkdirAll(fileDBPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	// Deposit 저장소 초기화 - 모드에 따른 경로 설정
	var detectedDepositFilePath string
	//TODO 로직은 그럴듯 하지만, FileDBPath자체가 Isolated 폴더 내부라 실은 효용이 없음. 추후 isolated관련 feed_XX_XX.go수정 필요.
	//TODO 테스트 시에만 isolated되게 해야 함.
	if mode == TestingMode {
		detectedDepositFilePath = fileDBPath + "/test_detected_deposits.csv"
	} else {
		detectedDepositFilePath = fileDBPath + "/production_detected_deposits.csv"
	}
	depositRepo := infra.NewFileDepositRepository(detectedDepositFilePath)
	return depositRepo, nil
}
