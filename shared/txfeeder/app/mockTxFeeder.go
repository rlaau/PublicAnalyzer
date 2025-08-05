package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sharedDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

// EnvironmentConfig 환경 설정을 위한 구조체 (기존 호환성용)
type EnvironmentConfig struct {
	BaseDir         string
	IsolatedDir     string
	CEXFilePath     string
	MockDepositFile string
	GraphDBPath     string
	PendingDBPath   string

	// Pipeline 설정
	ChannelBufferSize int
	TestDuration      time.Duration
	TotalTransactions int
	GenerationRate    int
	AnalysisWorkers   int
}

// AdditionalDataConfig 추가 데이터 설정을 위한 구조체 (향후 확장용)
type AdditionalDataConfig struct {
	// 향후 추가 데이터 관련 설정들이 들어갈 예정
	// 일단 비워두고 나중에 필요할 때 확장
}

// TxFeederConfig 통합된 TxFeeder 설정
type TxFeederConfig struct {
	// 트랜잭션 생성 설정
	GenConfig *domain.TxGeneratorConfig

	// 환경 설정
	EnvConfig *EnvironmentConfig

	// 추가 데이터 설정 (현재는 사용하지 않음)
	AdditionalDataConfig *AdditionalDataConfig

	// 배치 모드 설정
	BatchMode    bool          // 배치 모드 활성화 여부
	BatchSize    int           // 배치 크기 (기본값: 100)
	BatchTimeout time.Duration // 배치 타임아웃 (기본값: 50ms)
}

// PipelineStats 파이프라인 통계
type PipelineStats struct {
	Generated   int64 // 생성된 트랜잭션 수
	Transmitted int64 // 전송된 트랜잭션 수
	Processed   int64 // 처리된 트랜잭션 수
	Dropped     int64 // 드롭된 트랜잭션 수 (채널 풀)
	StartTime   time.Time
}

// DebugStats 디버깅용 통계
type DebugStats struct {
	CexToAddresses     int64 // CEX를 to로 하는 트랜잭션
	DepositToAddresses int64 // Deposit을 to로 하는 트랜잭션
	RandomTransactions int64 // 랜덤 트랜잭션
	MatchFailures      int64 // 매칭 실패
}

// TxFeeder generates transactions for testing EE module with pipeline management
type TxFeeder struct {
	config           *domain.TxGeneratorConfig
	state            *domain.TxGeneratorState
	mockDepositAddrs *domain.MockDepositAddressSet
	cexSet           *sharedDomain.CEXSet

	// Channels for transaction output (backward compatibility)
	markedTxChannel chan sharedDomain.MarkedTransaction

	// Kafka producer for fed-tx topic
	kafkaProducer *kafka.KafkaProducer[*sharedDomain.MarkedTransaction]
	batchProducer *kafka.KafkaBatchProducer[*sharedDomain.MarkedTransaction] // 배치 모드용 프로듀서 (제너릭)
	kafkaBrokers  []string
	batchMode     bool          // 배치 모드 활성화 여부
	batchSize     int           // 배치 크기
	batchTimeout  time.Duration // 배치 타임아웃

	// Control channels
	stopChannel chan struct{}
	doneChannel chan struct{}

	// Synchronization
	mutex    sync.RWMutex
	stopOnce sync.Once // 중복 stop 방지

	// Environment management
	baseDir     string
	isolatedDir string

	// Pipeline management - 출력 채널 등록 방식 (backward compatibility)
	requestedOutputChannels []chan<- *sharedDomain.MarkedTransaction // 등록된 출력 채널들

	// Pipeline 통계 및 상태 관리
	stats       PipelineStats
	debugStats  DebugStats
	wg          sync.WaitGroup
	channelOnce sync.Once // 트랜잭션 채널 중복 닫기 방지
}

// NewTxFeeder 간단한 TxFeeder 생성을 위한 헬퍼 함수
func NewTxFeeder(genConfig *domain.TxGeneratorConfig, envConfig *EnvironmentConfig) (*TxFeeder, error) {
	config := &TxFeederConfig{
		GenConfig:            genConfig,
		EnvConfig:            envConfig,
		AdditionalDataConfig: nil, // 현재 사용하지 않음
	}

	return NewTxFeederWithComplexConfig(config)
}

// NewTxFeederWithComplexConfig 통합된 설정으로 TxFeeder를 생성하고 모든 초기화를 완료
func NewTxFeederWithComplexConfig(config *TxFeederConfig) (*TxFeeder, error) {
	// 1. 기본 TxFeeder 생성 (빈 CEXSet으로 시작)
	emptyCexSet := sharedDomain.NewCEXSet()
	feeder := GetRawTxFeeder(config.GenConfig, emptyCexSet)

	// 2. 배치 모드 설정 적용
	if config.BatchMode {
		feeder.EnableBatchMode(config.BatchSize, config.BatchTimeout)
	}

	// 3. 환경 설정이 있으면 실행
	if config.EnvConfig != nil {
		if err := feeder.SetupEnvironment(config.EnvConfig); err != nil {
			return nil, fmt.Errorf("failed to setup environment: %w", err)
		}

		// CEX Set 로딩
		if _, err := feeder.LoadCEXSetFromFile(config.EnvConfig.CEXFilePath); err != nil {
			return nil, fmt.Errorf("failed to load CEX set: %w", err)
		}

		// Mock Deposit 주소 로딩
		if err := feeder.LoadMockDepositAddresses(config.EnvConfig.MockDepositFile); err != nil {
			return nil, fmt.Errorf("failed to load mock deposits: %w", err)
		}
	}

	// 4. AdditionalDataConfig는 현재 무시 (향후 확장용)
	// if config.AdditionalDataConfig != nil {
	//     // 향후 추가 데이터 설정 처리
	// }

	return feeder, nil
}

// GetRawTxFeeder creates a new transaction generator with pipeline capabilities (기존 호환성용)
func GetRawTxFeeder(config *domain.TxGeneratorConfig, cexSet *sharedDomain.CEXSet) *TxFeeder {
	// Kafka 브로커 설정 (기본값: localhost:9092)
	kafkaBrokers := []string{"localhost:9092"}

	// Kafka Producer 초기화 (기본값: 단건 모드)
	kafkaConfig := kafka.KafkaBatchConfig{
		Brokers: kafkaBrokers,
		Topic:   kafka.TestFedTxTopic}
	kafkaProducer := kafka.NewKafkaProducer[*sharedDomain.MarkedTransaction](kafkaConfig)

	return &TxFeeder{
		config:                  config,
		state:                   domain.NewTxGeneratorState(config.StartTime, config.TimeIncrementDuration, config.TransactionsPerTimeIncrement),
		mockDepositAddrs:        domain.NewMockDepositAddressSet(),
		cexSet:                  cexSet,
		markedTxChannel:         make(chan sharedDomain.MarkedTransaction, 100_000), // Buffer for 10k transactions
		kafkaProducer:           kafkaProducer,
		batchProducer:           nil, // 기본값: 배치 모드 비활성화
		kafkaBrokers:            kafkaBrokers,
		batchMode:               false,                 // 기본값: 단건 모드
		batchSize:               100,                   // 기본 배치 크기
		batchTimeout:            50 * time.Millisecond, // 기본 배치 타임아웃
		stopChannel:             make(chan struct{}),
		doneChannel:             make(chan struct{}),
		requestedOutputChannels: make([]chan<- *sharedDomain.MarkedTransaction, 0),
		stats: PipelineStats{
			StartTime: time.Now(),
		},
		debugStats: DebugStats{},
	}
}

// EnableBatchMode 배치 모드 활성화
func (g *TxFeeder) EnableBatchMode(batchSize int, batchTimeout time.Duration) error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	// 배치 설정 적용 (기본값 처리)
	if batchSize <= 0 {
		batchSize = 100 // 기본 배치 크기
	}
	if batchTimeout <= 0 {
		batchTimeout = 50 * time.Millisecond // 기본 배치 타임아웃
	}

	g.batchMode = true
	g.batchSize = batchSize
	g.batchTimeout = batchTimeout

	// BatchProducer 초기화 (제너릭 타입 사용)
	config := kafka.KafkaBatchConfig{
		Brokers:      g.kafkaBrokers,
		Topic:        kafka.TestFedTxTopic,
		GroupID:      "tx-feeder-batch-group",
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
	}
	g.batchProducer = kafka.NewKafkaBatchProducer[*sharedDomain.MarkedTransaction](config)

	fmt.Printf("🚀 Batch mode enabled: batchSize=%d, timeout=%v\n", batchSize, batchTimeout)
	return nil
}

// DisableBatchMode 배치 모드 비활성화 (단건 모드로 복귀)
func (g *TxFeeder) DisableBatchMode() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.batchProducer != nil {
		g.batchProducer.Close()
		g.batchProducer = nil
	}

	g.batchMode = false
	fmt.Println("🔄 Switched to single-message mode")
	return nil
}

// IsBatchMode 현재 배치 모드 여부 확인
func (g *TxFeeder) IsBatchMode() bool {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	return g.batchMode
}

// LoadMockDepositAddresses loads mock deposit addresses from file
func (g *TxFeeder) LoadMockDepositAddresses(filePath string) error {
	return g.mockDepositAddrs.LoadFromFile(filePath)
}

// Start begins transaction generation
func (g *TxFeeder) Start(ctx context.Context) error {
	// Kafka 연결 상태 확인
	if err := kafka.EnsureKafkaConnection(g.kafkaBrokers); err != nil {
		return fmt.Errorf("kafka connection failed: %w", err)
	}

	// fed-tx 토픽 존재 확인 및 생성
	if err := kafka.CreateTopicIfNotExists(g.kafkaBrokers, kafka.TestFedTxTopic, 1, 1); err != nil {
		return fmt.Errorf("failed to ensure fed-tx topic: %w", err)
	}

	// 모드에 따라 다른 생성 로직 실행
	if g.IsBatchMode() {
		go g.generateTransactionsBatch(ctx)
	} else {
		go g.generateTransactions(ctx)
	}
	return nil
}

// Stop stops transaction generation (safe for multiple calls)
func (g *TxFeeder) Stop() {
	g.stopOnce.Do(func() {
		close(g.stopChannel)
	})
	<-g.doneChannel
}

// GetTxChannel returns the channel for receiving generated transactions
func (g *TxFeeder) GetTxChannel() <-chan sharedDomain.MarkedTransaction {
	return g.markedTxChannel
}

// GetGeneratedCount returns the current count of generated transactions
func (g *TxFeeder) GetGeneratedCount() int64 {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	return g.state.GeneratedCount
}

// RegisterOutputChannel registers a channel to receive generated transactions
func (g *TxFeeder) RegisterOutputChannel(outputCh chan<- *sharedDomain.MarkedTransaction) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.requestedOutputChannels = append(g.requestedOutputChannels, outputCh)
}

// GetPipelineStats returns current pipeline statistics with TPS
func (g *TxFeeder) GetPipelineStats() PipelineStats {
	generated := atomic.LoadInt64(&g.stats.Generated)
	transmitted := atomic.LoadInt64(&g.stats.Transmitted)

	return PipelineStats{
		Generated:   generated,
		Transmitted: transmitted,
		Processed:   atomic.LoadInt64(&g.stats.Processed),
		Dropped:     atomic.LoadInt64(&g.stats.Dropped),
		StartTime:   g.stats.StartTime,
	}
}

// GetTPS 현재 TPS (초당 트랜잭션 수) 반환
func (g *TxFeeder) GetTPS() float64 {
	generated := atomic.LoadInt64(&g.stats.Generated)
	elapsed := time.Since(g.stats.StartTime).Seconds()
	if elapsed > 0 {
		return float64(generated) / elapsed
	}
	return 0
}

// generateTransactions is the main generation loop running in goroutine
func (g *TxFeeder) generateTransactions(ctx context.Context) {
	defer close(g.doneChannel)
	defer close(g.markedTxChannel)
	fmt.Printf("Starting transaction generation: %d total transactions at %d tx/sec\n",
		g.config.TotalTransactions, g.config.TransactionsPerSecond)
	// Calculate dynamic interval based on config.TPS
	interval := time.Second / time.Duration(g.config.TransactionsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Transaction generation stopped by context")
			return
		case <-g.stopChannel:
			fmt.Println("Transaction generation stopped by stop signal")
			return
		case <-ticker.C:
			// Check if we've generated enough transactions
			g.mutex.RLock()
			currentCount := g.state.GeneratedCount
			g.mutex.RUnlock()

			if currentCount >= int64(g.config.TotalTransactions) {
				fmt.Printf("Generated all %d transactions. Stopping.\n", g.config.TotalTransactions)
				return
			}

			// Generate transaction
			tx := g.generateSingleTransaction()
			atomic.AddInt64(&g.stats.Generated, 1)

			// Send to legacy channel (for backward compatibility)
			select {
			case g.markedTxChannel <- tx:
			case <-ctx.Done():
				return
			case <-g.stopChannel:
				return
			default:
				atomic.AddInt64(&g.stats.Dropped, 1)
			}

			// Send to all registered output channels (backward compatibility)
			g.sendToOutputChannels(&tx, ctx)

			// Send to Kafka topic
			g.sendToKafka(&tx, ctx)
		}
	}
}

// generateTransactionsBatch 배치 모드 트랜잭션 생성 (진정한 배칭)
func (g *TxFeeder) generateTransactionsBatch(ctx context.Context) {
	defer close(g.doneChannel)
	defer close(g.markedTxChannel)

	fmt.Printf("🚀 Starting BATCH transaction generation: %d total transactions at %d tx/sec (batch size: %d)\n",
		g.config.TotalTransactions, g.config.TransactionsPerSecond, g.batchSize)

	// 배치 간격 계산: 배치 크기만큼 생성하는데 걸리는 시간
	batchInterval := time.Duration(g.batchSize) * time.Second / time.Duration(g.config.TransactionsPerSecond)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Batch transaction generation stopped by context")
			return
		case <-g.stopChannel:
			fmt.Println("Batch transaction generation stopped by stop signal")
			return
		case <-ticker.C:
			// 생성할 배치 크기 결정 (남은 트랜잭션 수 고려)
			g.mutex.RLock()
			currentCount := g.state.GeneratedCount
			g.mutex.RUnlock()

			remainingTx := int64(g.config.TotalTransactions) - currentCount
			if remainingTx <= 0 {
				fmt.Printf("Generated all %d transactions. Stopping batch mode.\n", g.config.TotalTransactions)
				return
			}

			// 실제 배치 크기 결정 (남은 트랜잭션이 배치 크기보다 작으면 조정)
			actualBatchSize := g.batchSize
			if remainingTx < int64(g.batchSize) {
				actualBatchSize = int(remainingTx)
			}

			// 배치 생성 및 전송
			if err := g.generateAndSendBatch(ctx, actualBatchSize); err != nil {
				fmt.Printf("⚠️ Batch generation error: %v\n", err)
				continue
			}
		}
	}
}

// generateAndSendBatch 배치 생성 및 전송 (진정한 배칭)
func (g *TxFeeder) generateAndSendBatch(ctx context.Context, batchSize int) error {
	// 1. 배치 크기만큼 트랜잭션 생성
	transactions := make([]*sharedDomain.MarkedTransaction, 0, batchSize)
	messages := make([]kafka.Message[*sharedDomain.MarkedTransaction], 0, batchSize)

	for i := 0; i < batchSize; i++ {
		// 트랜잭션 생성
		tx := g.generateSingleTransaction()
		transactions = append(transactions, &tx)

		// 타입화된 Kafka 메시지 생성 (직접 MarkedTransaction 전송)
		messages = append(messages, kafka.Message[*sharedDomain.MarkedTransaction]{
			Key:   []byte("tx"),
			Value: &tx, // 직접 MarkedTransaction 포인터 전송
		})
	}

	// 2. 통계 업데이트
	atomic.AddInt64(&g.stats.Generated, int64(len(transactions)))

	// 3. Legacy 채널들에 전송 (backward compatibility)
	g.sendBatchToLegacyChannels(transactions, ctx)

	// 4. Kafka 배치 전송 (진정한 배칭!)
	if err := g.sendBatchToKafka(messages, ctx); err != nil {
		return fmt.Errorf("batch kafka send failed: %w", err)
	}

	return nil
}

// sendBatchToLegacyChannels 배치를 레거시 채널들에 전송
func (g *TxFeeder) sendBatchToLegacyChannels(transactions []*sharedDomain.MarkedTransaction, ctx context.Context) {
	for _, tx := range transactions {
		// Legacy channel (backward compatibility)
		select {
		case g.markedTxChannel <- *tx:
		case <-ctx.Done():
			return
		case <-g.stopChannel:
			return
		default:
			atomic.AddInt64(&g.stats.Dropped, 1)
		}

		// Registered output channels (backward compatibility)
		g.sendToOutputChannels(tx, ctx)
	}
}

// sendBatchToKafka 배치를 Kafka에 전송 (진정한 배칭)
func (g *TxFeeder) sendBatchToKafka(messages []kafka.Message[*sharedDomain.MarkedTransaction], ctx context.Context) error {
	if g.batchProducer == nil {
		return fmt.Errorf("batch producer not initialized")
	}

	// 진정한 배치 전송 (한 번의 네트워크 호출로 모든 메시지 전송)
	if err := g.batchProducer.PublishMessagesBatch(ctx, messages); err != nil {
		// 배치 전체가 실패한 경우
		atomic.AddInt64(&g.stats.Dropped, int64(len(messages)))
		return err
	} else {
		// 배치 전체가 성공한 경우
		atomic.AddInt64(&g.stats.Transmitted, int64(len(messages)))
	}

	return nil
}

// generateSingleTransaction generates a single MarkedTransaction
func (g *TxFeeder) generateSingleTransaction() sharedDomain.MarkedTransaction {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	// Determine transaction type based on patterns
	txType := g.determineTransactionType()

	var tx sharedDomain.MarkedTransaction

	switch txType {
	case DepositToCexTx:
		tx = g.generateDepositToCexTransaction()
		// 처음 5개 특별 케이스는 로깅
		if g.state.GeneratedCount < 5 && txType == DepositToCexTx {
			fmt.Printf("   ✨ Generated Deposit→CEX: From=%s → To=%s\n",
				tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		}
	case RandomToDepositTx:
		tx = g.generateRandomToDepositTransaction()
		if g.state.GeneratedCount < 5 && txType == RandomToDepositTx {
			fmt.Printf("   ✨ Generated Random→Deposit: From=%s → To=%s\n",
				tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		}
	default:
		tx = g.generateRandomTransaction()
	}

	// Update state
	g.state.IncrementTransaction()

	return tx
}

type TransactionType int

const (
	RandomTx          TransactionType = iota
	DepositToCexTx                    // mockedAndHiddenDepositAddress -> CEX
	RandomToDepositTx                 // random address -> mockedAndHiddenDepositAddress
)

func (t TransactionType) String() string {
	switch t {
	case RandomTx:
		return "Random"
	case DepositToCexTx:
		return "Deposit→CEX"
	case RandomToDepositTx:
		return "Random→Deposit"
	default:
		return "Unknown"
	}
}

// determineTransactionType determines what type of transaction to generate
func (g *TxFeeder) determineTransactionType() TransactionType {
	count := int(g.state.GeneratedCount)

	// 디버깅: 처음 10개 트랜잭션의 타입 결정 과정 로깅
	var txType TransactionType
	var reason string

	// 1 in 5 chance for DepositToCex transaction
	if count%g.config.DepositToCexRatio == 0 {
		txType = DepositToCexTx
		reason = fmt.Sprintf("count=%d %% %d == 0", count, g.config.DepositToCexRatio)
	} else if count%g.config.RandomToDepositRatio == 0 {
		// 1 in 8 chance for RandomToDeposit transaction
		txType = RandomToDepositTx
		reason = fmt.Sprintf("count=%d %% %d == 0", count, g.config.RandomToDepositRatio)
	} else {
		txType = RandomTx
		reason = fmt.Sprintf("count=%d, random", count)
	}

	// 처음 10개는 디버깅 출력
	if count < 10 {
		fmt.Printf("   🎲 TX #%d: %v (%s)\n", count, txType, reason)
	}

	return txType
}

// generateDepositToCexTransaction generates mockedDepositAddress -> CEX transaction
func (g *TxFeeder) generateDepositToCexTransaction() sharedDomain.MarkedTransaction {
	fromAddr := g.mockDepositAddrs.GetRandomAddress()
	toAddr := g.getRandomCexAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// generateRandomToDepositTransaction generates random -> mockedDepositAddress transaction
func (g *TxFeeder) generateRandomToDepositTransaction() sharedDomain.MarkedTransaction {
	fromAddr := domain.GenerateRandomAddress()
	toAddr := g.mockDepositAddrs.GetRandomAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// generateRandomTransaction generates a completely random transaction
func (g *TxFeeder) generateRandomTransaction() sharedDomain.MarkedTransaction {
	fromAddr := domain.GenerateRandomAddress()
	toAddr := domain.GenerateRandomAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// createMarkedTransaction creates a MarkedTransaction with given from/to addresses
func (g *TxFeeder) createMarkedTransaction(from, to sharedDomain.Address) sharedDomain.MarkedTransaction {
	txID := domain.GenerateRandomTxID()

	// Generate random value (0.1 to 10 ETH in wei)
	minWei := new(big.Int)
	minWei.SetString("100000000000000000", 10) // 0.1 ETH in wei
	maxWei := new(big.Int)
	maxWei.SetString("10000000000000000000", 10) // 10 ETH in wei
	diff := new(big.Int).Sub(maxWei, minWei)

	// Simple random generation for value
	randomBytes := make([]byte, 8)
	for i := range randomBytes {
		randomBytes[i] = byte(g.state.GeneratedCount >> (i * 8))
	}
	randomValue := new(big.Int).SetBytes(randomBytes)
	randomValue.Mod(randomValue, diff)
	randomValue.Add(randomValue, minWei)

	return sharedDomain.MarkedTransaction{
		BlockTime: g.state.CurrentTime,
		TxID:      txID,
		TxSyntax:  [2]sharedDomain.ContractBoolMark{sharedDomain.EOAMark, sharedDomain.EOAMark}, // Assume EOA-to-EOA for simplicity
		Nonce:     uint64(g.state.GeneratedCount),
		From:      from,
		To:        to,
	}
}

// getRandomCexAddress returns a random CEX address from the loaded set
func (g *TxFeeder) getRandomCexAddress() sharedDomain.Address {
	addresses := g.cexSet.GetAll()
	if len(addresses) == 0 {
		return domain.GenerateRandomAddress() // Fallback to random if no CEX addresses
	}

	// Simple random selection
	idx := int(g.state.GeneratedCount) % len(addresses)

	// Convert string address to Address type
	addr, err := g.parseAddressString(addresses[idx])
	if err != nil {
		return domain.GenerateRandomAddress() // Fallback on parse error
	}

	return addr
}

// parseAddressString converts hex string to Address type
func (g *TxFeeder) parseAddressString(hexStr string) (sharedDomain.Address, error) {
	var addr sharedDomain.Address

	// Remove 0x prefix if present
	if len(hexStr) >= 2 && hexStr[:2] == "0x" {
		hexStr = hexStr[2:]
	}

	if len(hexStr) != 40 {
		return addr, fmt.Errorf("invalid address length: %d", len(hexStr))
	}

	// Convert hex string to bytes
	for i := 0; i < 20; i++ {
		var b byte
		_, err := fmt.Sscanf(hexStr[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return addr, err
		}
		addr[i] = b
	}

	return addr, nil
}

// sendToOutputChannels sends transaction to all registered output channels (backward compatibility)
func (g *TxFeeder) sendToOutputChannels(tx *sharedDomain.MarkedTransaction, ctx context.Context) {
	g.mutex.RLock()
	channels := g.requestedOutputChannels
	g.mutex.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- tx:
			atomic.AddInt64(&g.stats.Transmitted, 1)
		case <-ctx.Done():
			return
		case <-g.stopChannel:
			return
		default:
			// Channel is full, drop the transaction
			atomic.AddInt64(&g.stats.Dropped, 1)
		}
	}
}

// sendToKafka sends transaction to fed-tx Kafka topic (고성능 비동기, 모노리식 최적화)
func (g *TxFeeder) sendToKafka(tx *sharedDomain.MarkedTransaction, ctx context.Context) {
	if g.kafkaProducer == nil {
		return // Kafka producer not initialized
	}

	// 모노리식 환경이므로 키는 단순하게 (파티션 고려 불필요)
	key := []byte("tx")

	// Send MarkedTransaction directly to Kafka (제너릭 타입으로 직접 전송)
	if err := g.kafkaProducer.PublishMessage(ctx, key, tx); err != nil {
		// 비동기에서는 주로 버퍼 풀 에러
		atomic.AddInt64(&g.stats.Dropped, 1)
		// 에러 로그 최소화 (10000개마다)
		if g.stats.Generated%10000 == 0 {
			fmt.Printf("   ⚠️ Kafka buffer full (sample): %v\n", err)
		}
	} else {
		atomic.AddInt64(&g.stats.Transmitted, 1)
	}
}

// SetupEnvironment 격리된 테스트 환경을 설정 (feed_and_analyze.go에서 이동)
func (g *TxFeeder) SetupEnvironment(envConfig *EnvironmentConfig) error {
	fmt.Println("\n2️⃣ Preparing isolated environment...")

	g.baseDir = envConfig.BaseDir
	g.isolatedDir = envConfig.IsolatedDir

	// 기존 디렉토리 제거 후 새로 생성
	os.RemoveAll(g.isolatedDir)
	if err := os.MkdirAll(g.isolatedDir, 0755); err != nil {
		return fmt.Errorf("failed to create isolated directory: %w", err)
	}

	// CEX 데이터 복제
	sourceCEX := filepath.Join(g.baseDir, "shared", "txfeeder", "infra", "real_cex.txt")
	fmt.Printf("   🔍 Source CEX: %s\n", sourceCEX)
	fmt.Printf("   🔍 Target CEX: %s\n", envConfig.CEXFilePath)

	// 소스 파일 존재 확인
	if _, err := os.Stat(sourceCEX); os.IsNotExist(err) {
		return fmt.Errorf("source CEX file does not exist: %s", sourceCEX)
	}

	if err := g.copyFile(sourceCEX, envConfig.CEXFilePath); err != nil {
		return fmt.Errorf("failed to copy CEX file: %w", err)
	}

	// 복사 후 검증
	if copiedData, err := os.ReadFile(envConfig.CEXFilePath); err == nil {
		lines := strings.Split(string(copiedData), "\n")
		nonEmptyLines := 0
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				nonEmptyLines++
			}
		}
		fmt.Printf("   📄 CEX data copied - %d lines, %d addresses\n", len(lines), nonEmptyLines)
	} else {
		fmt.Printf("   ⚠️  CEX data copied but could not verify: %v\n", err)
	}

	// 모의 입금 주소 생성
	if err := g.createMockDeposits(envConfig.MockDepositFile); err != nil {
		return fmt.Errorf("failed to create mock deposits: %w", err)
	}
	fmt.Printf("   📄 Mock deposits created\n")

	fmt.Printf("   ✅ Environment prepared\n")
	return nil
}

// LoadCEXSetFromFile CEX 주소 집합을 파일에서 로드 (ee/infra 기능 이동)
func (g *TxFeeder) LoadCEXSetFromFile(cexFilePath string) (*sharedDomain.CEXSet, error) {
	fmt.Printf("   🔍 CEX file path: %s\n", cexFilePath)

	// 파일 존재 확인
	if _, err := os.Stat(cexFilePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("CEX file does not exist: %s", cexFilePath)
	}

	file, err := os.Open(cexFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CEX file: %w", err)
	}
	defer file.Close()

	cexSet := sharedDomain.NewCEXSet()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		cexAddr := sharedDomain.NewCEXAddress(line)
		if cexAddr.IsValid() {
			cexSet.Add(cexAddr.Address)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read CEX file: %w", err)
	}

	fmt.Printf("   📦 CEX addresses loaded: %d\n", cexSet.Size())

	// CEX 로딩이 실패한 경우 추가 디버깅
	if cexSet.Size() == 0 {
		fmt.Printf("   ❌ CEX loading failed - checking file contents...\n")
		if fileData, err := os.ReadFile(cexFilePath); err == nil {
			lines := strings.Split(string(fileData), "\n")
			nonEmptyLines := 0
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "#") {
					nonEmptyLines++
				}
			}
			fmt.Printf("   📄 File contains %d lines, %d non-empty non-comment lines\n", len(lines), nonEmptyLines)
		}
		return nil, fmt.Errorf("no CEX addresses loaded from file")
	}

	// CEX 주소 샘플 출력 (디버깅)
	cexAddresses := cexSet.GetAll()
	if len(cexAddresses) >= 3 {
		fmt.Printf("   🔍 CEX samples: %s, %s, %s\n",
			cexAddresses[0][:10]+"...",
			cexAddresses[1][:10]+"...",
			cexAddresses[2][:10]+"...")
	}

	g.cexSet = cexSet
	return cexSet, nil
}

// CleanupEnvironment 격리된 환경 정리 (feed_and_analyze.go에서 이동)
func (g *TxFeeder) CleanupEnvironment() {
	if g.isolatedDir == "" {
		return // 격리 디렉토리가 설정되지 않았으면 정리할 것이 없음
	}

	fmt.Println("\n🧹 Cleaning up isolated environment...")

	if err := os.RemoveAll(g.isolatedDir); err != nil {
		fmt.Printf("⚠️ Warning: cleanup failed: %v", err)
	} else {
		fmt.Printf("   ✅ Cleaned: %s\n", g.isolatedDir)
	}

	fmt.Println("🔒 No permanent changes to system")
}

// Close 리소스 정리 (트랜잭션 생성만 중지, 환경 정리는 호출자가 담당)
func (g *TxFeeder) Close() error {
	// Stop을 호출해서 트랜잭션 생성 중지
	g.Stop()

	// Kafka Producer 정리
	if g.kafkaProducer != nil {
		if err := g.kafkaProducer.Close(); err != nil {
			fmt.Printf("   ⚠️ Kafka producer close error: %v\n", err)
		}
	}

	// Batch Producer 정리
	if g.batchProducer != nil {
		if err := g.batchProducer.Close(); err != nil {
			fmt.Printf("   ⚠️ Batch producer close error: %v\n", err)
		}
	}

	return nil
}

// CleanupKafkaTopic 테스트 완료 후 fed-tx 토픽 데이터 정리
func (g *TxFeeder) CleanupKafkaTopic() error {
	if len(g.kafkaBrokers) == 0 {
		return nil
	}

	fmt.Println("🧹 Cleaning up fed-tx Kafka topic...")

	if err := kafka.CleanupTopicComplete(g.kafkaBrokers, kafka.TestFedTxTopic, 1, 1); err != nil {
		fmt.Printf("   ⚠️ Kafka topic cleanup warning: %v\n", err)
		return err
	}

	fmt.Printf("   ✅ Fed-tx topic cleaned up\n")
	return nil
}

// 내부 헬퍼 메서드들 (feed_and_analyze.go에서 이동)
func (g *TxFeeder) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func (g *TxFeeder) createMockDeposits(filePath string) error {
	fmt.Printf("   🔍 Creating mock deposit addresses at %s\n", filePath)
	file, err := os.Create(filePath)

	if err != nil {
		return err
	}
	defer file.Close()

	root := g.findProjectRoot()
	depositFilePath := filepath.Join(root, "shared", "txfeeder", "infra", "mocked_hidden_deposits.txt")
	fmt.Printf("loading mockedAndHiddenDepositAddress.txt from %s\n", depositFilePath)

	deposits, err := os.Open(depositFilePath)
	if err != nil {
		return err
	}
	defer deposits.Close()

	file.WriteString("# Mock Deposit Addresses for Fixed Queue Test\n\n")
	// 한 줄씩 읽어서 복사
	scanner := bufio.NewScanner(deposits)
	totalLength := 0
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		file.WriteString(line + "\n")
		totalLength += len(line)
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading deposit file: %w", err)
	}
	fmt.Printf("   ✅ Copied %d lines (total %d bytes of address strings)\n", lineCount, totalLength)
	return nil
}

// findProjectRoot 프로젝트 루트 찾기 (feed_and_analyze.go에서 이동)
func (g *TxFeeder) findProjectRoot() string {
	currentDir, _ := os.Getwd()

	for currentDir != "/" {
		if strings.HasSuffix(currentDir, "chainAnalyzer") {
			return currentDir
		}

		if data, err := os.ReadFile(filepath.Join(currentDir, "go.mod")); err == nil {
			if strings.Contains(string(data), "chainAnalyzer") {
				return currentDir
			}
		}

		currentDir = filepath.Dir(currentDir)
	}

	workingDir, _ := os.Getwd()
	return filepath.Join(workingDir, "../../../")
}
