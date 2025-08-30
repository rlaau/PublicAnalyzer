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

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	sharedDomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

// DeterministicGraphStructure 결정론적 그래프 구조 정의
type DeterministicGraphStructure struct {
	// 기본 엔티티들
	CEXAddresses     []sharedDomain.Address // 20개 CEX 주소
	DepositAddresses []sharedDomain.Address // 200개 Deposit 주소
	UserAddresses    []sharedDomain.Address // 1000개 User 주소

	// Deposit 카테고리별 분류
	MultiUserDeposits  []sharedDomain.Address // 150개 - 2~20명 User와 상호작용
	SingleUserDeposits []sharedDomain.Address // 50개 - 1명 User와 상호작용

	// User 카테고리별 분류
	MultiDepositUsers  []sharedDomain.Address // 500명 - 2~5개 Deposit과 상호작용
	SingleDepositUsers []sharedDomain.Address // 300명 - 1개 Deposit과 상호작용
	InterUserOnlyUsers []sharedDomain.Address // 200명 - User간만 상호작용

	// 관계 매핑
	DepositToUsers  map[sharedDomain.Address][]sharedDomain.Address // Deposit -> 여러 User
	UserToDeposits  map[sharedDomain.Address][]sharedDomain.Address // User -> 여러 Deposit
	UserToUserPairs [][2]sharedDomain.Address                       // User간 직접 연결 쌍

	// 생성된 트랜잭션들
	PredefinedTransactions []sharedDomain.MarkedTransaction

	// 검증용 통계
	ExpectedCEXCount       int
	ExpectedDepositCount   int
	ExpectedUserCount      int
	TotalTransactions      int
	TransactionsByCategory map[string]int
}

// TxDefineLoader 결정론적 트랜잭션 로더
type TxDefineLoader struct {
	// 기본 설정 (TxGenerator와 공유)
	cexSet           *sharedDomain.CEXSet
	mockDepositAddrs *domain.MockDepositAddressSet
	kafkaProducer    *kafka.KafkaProducer[*sharedDomain.MarkedTransaction]
	batchProducer    *kafka.KafkaBatchProducer[*sharedDomain.MarkedTransaction]
	kafkaBrokers     []string
	batchMode        bool
	batchSize        int
	batchTimeout     time.Duration

	// 결정론적 구조
	graphStructure *DeterministicGraphStructure
	currentIndex   int64 // 현재 전송 중인 트랜잭션 인덱스

	// 제어 채널
	stopChannel chan struct{}
	doneChannel chan struct{}

	// 동기화
	mutex       sync.RWMutex
	stopOnce    sync.Once
	targetTotal int // ✅ 목표 총 트랜잭션 수 (예: 400_000)
	// 통계
	stats PipelineStats
}

// NewTxDefineLoader 새로운 결정론적 트랜잭션 로더 생성
func NewTxDefineLoader(config *TxFeederConfig) (*TxDefineLoader, error) {
	// Kafka 설정
	kafkaBrokers := []string{"localhost:9092"}
	kafkaConfig := kafka.KafkaBatchConfig{
		Brokers: kafkaBrokers,
		Topic:   kafka.TestFedTxTopic,
	}
	kafkaProducer := kafka.NewKafkaProducer[*sharedDomain.MarkedTransaction](kafkaConfig)

	loader := &TxDefineLoader{
		cexSet:           sharedDomain.NewCEXSet(),
		mockDepositAddrs: domain.NewMockDepositAddressSet(),
		kafkaProducer:    kafkaProducer,
		kafkaBrokers:     kafkaBrokers,
		batchMode:        config.BatchMode,
		batchSize:        config.BatchSize,
		batchTimeout:     config.BatchTimeout,
		stopChannel:      make(chan struct{}),
		doneChannel:      make(chan struct{}),
		targetTotal:      config.GenConfig.TotalTransactions,
		stats: PipelineStats{
			StartTime: time.Now(),
		},
	}

	// ✅ GenConfig.TotalTransactions 값을 사용 (있으면 스케일링)
	if config.GenConfig != nil && config.GenConfig.TotalTransactions > 0 {
		loader.targetTotal = config.GenConfig.TotalTransactions
	}

	// 배치 모드 설정
	if config.BatchMode {
		batchConfig := kafka.KafkaBatchConfig{
			Brokers:      kafkaBrokers,
			Topic:        kafka.TestFedTxTopic,
			GroupID:      "tx-define-loader-batch-group",
			BatchSize:    config.BatchSize,
			BatchTimeout: config.BatchTimeout,
		}
		loader.batchProducer = kafka.NewKafkaBatchProducer[*sharedDomain.MarkedTransaction](batchConfig)
	}

	// 환경 설정 (CEX, Deposit 파일 로딩)
	if err := loader.setupEnvironment(config); err != nil {
		return nil, fmt.Errorf("failed to setup environment: %w", err)
	}

	// 결정론적 그래프 구조 생성
	if err := loader.generateDeterministicGraph(); err != nil {
		return nil, fmt.Errorf("failed to generate deterministic graph: %w", err)
	}

	fmt.Printf("✅ TxDefineLoader created with %d predefined transactions\n", len(loader.graphStructure.PredefinedTransactions))
	loader.printGraphSummary()
	return loader, nil
}

// generateDeterministicGraph 결정론적 그래프 구조 생성
func (loader *TxDefineLoader) generateDeterministicGraph() error {
	loader.mutex.Lock()
	defer loader.mutex.Unlock()

	graph := &DeterministicGraphStructure{
		DepositToUsers:         make(map[sharedDomain.Address][]sharedDomain.Address),
		UserToDeposits:         make(map[sharedDomain.Address][]sharedDomain.Address),
		TransactionsByCategory: make(map[string]int),
	}

	// 1. CEX 주소 20개 선택 (실제 CEX Set에서)
	cexAddresses := loader.cexSet.GetAll()
	if len(cexAddresses) < 20 {
		return fmt.Errorf("insufficient CEX addresses: need 20, got %d", len(cexAddresses))
	}

	for i := 0; i < 20; i++ {
		addr, err := loader.parseAddressString(cexAddresses[i])
		if err != nil {
			continue
		}
		graph.CEXAddresses = append(graph.CEXAddresses, addr)
	}

	// 2. Deposit 주소 200개 선택 (MockDepositAddrs에서 결정론적으로)
	allDeposits := make([]sharedDomain.Address, 0, 200)
	if loader.mockDepositAddrs.Size() < 200 {
		return fmt.Errorf("insufficient mock deposit addresses: need 200, got %d", loader.mockDepositAddrs.Size())
	}

	// 결정론적으로 선택 (시드 기반)
	for i := 0; i < 200; i++ {
		// 결정론적 인덱스 계산 (랜덤이 아닌 시드 기반)
		idx := (i * 17) % loader.mockDepositAddrs.Size()
		depositAddr := loader.getDeterministicDepositAddress(idx)
		allDeposits = append(allDeposits, depositAddr)
	}
	graph.DepositAddresses = allDeposits

	// Deposit 분류: 150개(MultiUser) + 50개(SingleUser)
	graph.MultiUserDeposits = allDeposits[:150]
	graph.SingleUserDeposits = allDeposits[150:]

	// 3. User 주소 1000개 생성 (결정론적)
	allUsers := make([]sharedDomain.Address, 0, 1000)
	for i := 0; i < 1000; i++ {
		userAddr := loader.generateDeterministicAddress(i)
		allUsers = append(allUsers, userAddr)
	}
	graph.UserAddresses = allUsers

	// User 분류: 500명(MultiDeposit) + 300명(SingleDeposit) + 200명(InterUserOnly)
	graph.MultiDepositUsers = allUsers[:500]
	graph.SingleDepositUsers = allUsers[500:800]
	graph.InterUserOnlyUsers = allUsers[800:]

	// 4. 관계 매핑 생성
	loader.createDetailedRelationships(graph)

	// 5. 트랜잭션 생성
	loader.generateDetailedTransactions(graph)

	graph.ExpectedCEXCount = len(graph.CEXAddresses)
	graph.ExpectedDepositCount = len(graph.DepositAddresses)
	graph.ExpectedUserCount = len(graph.UserAddresses)
	graph.TotalTransactions = len(graph.PredefinedTransactions)

	loader.graphStructure = graph
	return nil
}

// createDetailedRelationships 상세한 관계 매핑 생성
func (loader *TxDefineLoader) createDetailedRelationships(graph *DeterministicGraphStructure) {
	// 1. MultiUser Deposits (150개) - 각각 2~20명 User와 연결
	userIdx := 0
	for i, deposit := range graph.MultiUserDeposits {
		usersPerDeposit := 2 + (i % 19) // 2~20명
		users := make([]sharedDomain.Address, 0, usersPerDeposit)

		for j := 0; j < usersPerDeposit && userIdx < len(graph.UserAddresses); j++ {
			user := graph.UserAddresses[userIdx]
			users = append(users, user)

			// 역방향 매핑 (User -> Deposit)
			graph.UserToDeposits[user] = append(graph.UserToDeposits[user], deposit)
			userIdx++
		}
		graph.DepositToUsers[deposit] = users
	}

	// 2. SingleUser Deposits (50개) - 각각 1명 User와 연결
	for _, deposit := range graph.SingleUserDeposits {
		if userIdx >= len(graph.UserAddresses) {
			break
		}
		user := graph.UserAddresses[userIdx]
		graph.DepositToUsers[deposit] = []sharedDomain.Address{user}
		graph.UserToDeposits[user] = append(graph.UserToDeposits[user], deposit)
		userIdx++
	}

	// 3. MultiDeposit Users (500명) - 각각 2~5개 Deposit과 추가 연결
	for i, user := range graph.MultiDepositUsers {
		depositsPerUser := 2 + (i % 4) // 2~5개

		// 이미 연결된 Deposit 개수 확인
		existingDeposits := len(graph.UserToDeposits[user])
		additionalNeeded := depositsPerUser - existingDeposits

		if additionalNeeded > 0 {
			// 추가 Deposit 연결
			for j := 0; j < additionalNeeded; j++ {
				depositIdx := (i*7 + j*11) % len(graph.DepositAddresses)
				deposit := graph.DepositAddresses[depositIdx]

				// 중복 방지
				alreadyConnected := false
				for _, existingDeposit := range graph.UserToDeposits[user] {
					if existingDeposit == deposit {
						alreadyConnected = true
						break
					}
				}

				if !alreadyConnected {
					graph.UserToDeposits[user] = append(graph.UserToDeposits[user], deposit)
					graph.DepositToUsers[deposit] = append(graph.DepositToUsers[deposit], user)
				}
			}
		}
	}

	// 4. InterUserOnly Users (200명) - User간 연결 쌍 생성
	for i := 0; i < len(graph.InterUserOnlyUsers)-1; i += 2 {
		user1 := graph.InterUserOnlyUsers[i]
		user2 := graph.InterUserOnlyUsers[i+1]
		graph.UserToUserPairs = append(graph.UserToUserPairs, [2]sharedDomain.Address{user1, user2})
	}
}

// generateDetailedTransactions 상세한 트랜잭션 생성 (수정된 버전: User->Deposit, User<->User, Deposit->CEX)
func (loader *TxDefineLoader) generateDetailedTransactions(graph *DeterministicGraphStructure) {
	startTime, _ := time.Parse("2006-01-02", "2025-01-01")
	currentTime := startTime

	transactions := make([]sharedDomain.MarkedTransaction, 0, 4000)

	for _, user := range graph.MultiDepositUsers {
		deposits := graph.UserToDeposits[user]
		for _, deposit := range deposits {
			tx := loader.createMarkedTransaction(user, deposit, len(transactions), currentTime)
			transactions = append(transactions, tx)
			graph.TransactionsByCategory["user_to_deposit"]++

			currentTime = currentTime.Add(30 * time.Minute)
		}
	}

	// 2. SingleDeposit Users -> Deposit 트랜잭션 (1개 × 300명 = 300개)
	for _, user := range graph.SingleDepositUsers {
		deposits := graph.UserToDeposits[user]
		if len(deposits) > 0 {
			deposit := deposits[0] // 첫 번째 Deposit만
			tx := loader.createMarkedTransaction(user, deposit, len(transactions), currentTime)
			transactions = append(transactions, tx)
			graph.TransactionsByCategory["user_to_deposit"]++

			currentTime = currentTime.Add(30 * time.Minute)
		}
	}

	// 3. User간 직접 연결 트랜잭션 (양방향, 총 200개)
	for _, pair := range graph.UserToUserPairs {
		// User1 -> User2
		tx1 := loader.createMarkedTransaction(pair[0], pair[1], len(transactions), currentTime)
		transactions = append(transactions, tx1)
		graph.TransactionsByCategory["user_to_user"]++
		currentTime = currentTime.Add(30 * time.Minute)

		// User2 -> User1 (양방향)
		tx2 := loader.createMarkedTransaction(pair[1], pair[0], len(transactions), currentTime)
		transactions = append(transactions, tx2)
		graph.TransactionsByCategory["user_to_user"]++
		currentTime = currentTime.Add(30 * time.Minute)
	}

	// 4. Deposit -> CEX 트랜잭션 (각 Deposit에서 무작위 CEX로, 500개)
	for i := 0; i < 500; i++ {
		deposit := graph.DepositAddresses[i%len(graph.DepositAddresses)]
		cex := graph.CEXAddresses[i%len(graph.CEXAddresses)]

		tx := loader.createMarkedTransaction(deposit, cex, len(transactions), currentTime)
		transactions = append(transactions, tx)
		graph.TransactionsByCategory["deposit_to_cex"]++

		currentTime = currentTime.Add(30 * time.Minute)
	}

	graph.PredefinedTransactions = transactions
}

// generateDeterministicAddress 결정론적 주소 생성
func (loader *TxDefineLoader) generateDeterministicAddress(seed int) sharedDomain.Address {
	var addr sharedDomain.Address

	// 시드를 기반으로 결정론적 바이트 생성
	for i := 0; i < 20; i++ {
		addr[i] = byte((seed*7 + i*13) % 256)
	}

	return addr
}

// getDeterministicDepositAddress 결정론적으로 Deposit 주소를 가져옴 (인덱스 기반)
func (loader *TxDefineLoader) getDeterministicDepositAddress(index int) sharedDomain.Address {
	// MockDepositAddressSet에서 특정 인덱스의 주소를 가져오기 위해
	// 임시로 결정론적 방식 사용 (실제로는 파일에서 순서대로 읽어야 함)

	// 결정론적 방식으로 주소 생성 (실제 파일 기반)
	var addr sharedDomain.Address
	for i := 0; i < 20; i++ {
		addr[i] = byte((index*19 + i*29) % 256)
	}
	return addr
}

// createMarkedTransaction MarkedTransaction 생성 헬퍼
func (loader *TxDefineLoader) createMarkedTransaction(from, to sharedDomain.Address, seed int, blockTime time.Time) sharedDomain.MarkedTransaction {
	// 결정론적 TxID 생성
	var txID sharedDomain.TxId
	for i := 0; i < 32; i++ {
		txID[i] = byte((seed*17 + i*23) % 256)
	}

	// 결정론적 값 생성 (0.1-10 ETH)
	minWei := big.NewInt(100000000000000000) // 0.1 ETH
	maxWei := new(big.Int)
	maxWei.SetString("10000000000000000000", 10) // 10 ETH
	diff := new(big.Int).Sub(maxWei, minWei)

	seedBig := big.NewInt(int64(seed))
	value := new(big.Int).Mod(seedBig, diff)
	value.Add(value, minWei)

	chainTime := chaintimer.ChainTime(blockTime)

	return sharedDomain.MarkedTransaction{
		BlockTime: chainTime,
		TxID:      txID,
		TxSyntax:  [2]sharedDomain.ContractBoolMark{sharedDomain.EOAMark, sharedDomain.EOAMark},
		Nonce:     uint64(seed),
		From:      from,
		To:        to,
	}
}

// parseAddressString 주소 문자열을 Address로 변환
func (loader *TxDefineLoader) parseAddressString(hexStr string) (sharedDomain.Address, error) {
	var addr sharedDomain.Address

	if len(hexStr) >= 2 && hexStr[:2] == "0x" {
		hexStr = hexStr[2:]
	}

	if len(hexStr) != 40 {
		return addr, fmt.Errorf("invalid address length: %d", len(hexStr))
	}

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

// printGraphSummary 그래프 구조 요약 출력
func (loader *TxDefineLoader) printGraphSummary() {
	graph := loader.graphStructure
	fmt.Println("\n📊 Deterministic Graph Structure Summary:")
	fmt.Printf("  🏦 CEX Addresses: %d\n", len(graph.CEXAddresses))
	fmt.Printf("  🏪 Deposit Addresses: %d\n", len(graph.DepositAddresses))
	fmt.Printf("    - MultiUser Deposits: %d (2-20 users each)\n", len(graph.MultiUserDeposits))
	fmt.Printf("    - SingleUser Deposits: %d (1 user each)\n", len(graph.SingleUserDeposits))
	fmt.Printf("  👤 User Addresses: %d\n", len(graph.UserAddresses))
	fmt.Printf("    - MultiDeposit Users: %d (2-5 deposits each)\n", len(graph.MultiDepositUsers))
	fmt.Printf("    - SingleDeposit Users: %d (1 deposit each)\n", len(graph.SingleDepositUsers))
	fmt.Printf("    - InterUser Only: %d (user-to-user only)\n", len(graph.InterUserOnlyUsers))
	fmt.Printf("  🔗 User-to-User Pairs: %d\n", len(graph.UserToUserPairs))
	fmt.Printf("  📦 Total Transactions: %d\n", len(graph.PredefinedTransactions))

	if len(graph.TransactionsByCategory) > 0 {
		fmt.Println("  📈 Transactions by Category:")
		for category, count := range graph.TransactionsByCategory {
			fmt.Printf("    - %s: %d\n", category, count)
		}
	}
}

// Start 결정론적 트랜잭션 전송 시작
func (loader *TxDefineLoader) Start(ctx context.Context) error {
	// Kafka 연결 확인
	if err := kafka.EnsureKafkaConnection(loader.kafkaBrokers); err != nil {
		return fmt.Errorf("kafka connection failed: %w", err)
	}

	if err := kafka.CreateTopicIfNotExists(loader.kafkaBrokers, kafka.TestFedTxTopic, 1, 1); err != nil {
		return fmt.Errorf("failed to ensure fed-tx topic: %w", err)
	}

	// 전송 시작
	if loader.batchMode {
		go loader.sendTransactionsBatch(ctx)
	} else {
		go loader.sendTransactions(ctx)
	}

	return nil
}

// sendTransactions 단건 모드로 트랜잭션 전송
func (loader *TxDefineLoader) sendTransactions(ctx context.Context) {
	defer close(loader.doneChannel)

	fmt.Printf("📤 Starting deterministic transaction sending: %d transactions\n", len(loader.graphStructure.PredefinedTransactions))

	interval := 50 * time.Microsecond // 초당 50개 전송
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-loader.stopChannel:
			return
		case <-ticker.C:
			currentIdx := atomic.LoadInt64(&loader.currentIndex)
			if currentIdx >= int64(len(loader.graphStructure.PredefinedTransactions)) {
				fmt.Printf("✅ All %d transactions sent\n", len(loader.graphStructure.PredefinedTransactions))
				return
			}

			tx := &loader.graphStructure.PredefinedTransactions[currentIdx]

			// Kafka 전송
			if err := loader.kafkaProducer.PublishMessage(ctx, []byte("tx"), tx); err != nil {
				atomic.AddInt64(&loader.stats.Dropped, 1)
			} else {
				atomic.AddInt64(&loader.stats.Transmitted, 1)
			}

			atomic.AddInt64(&loader.stats.Generated, 1)
			atomic.AddInt64(&loader.currentIndex, 1)
		}
	}
}

// sendTransactionsBatch 배치 모드로 트랜잭션 전송
func (loader *TxDefineLoader) sendTransactionsBatch(ctx context.Context) {
	defer close(loader.doneChannel)

	fmt.Printf("🚀 Starting deterministic batch transaction sending: %d transactions (batch size: %d)\n",
		len(loader.graphStructure.PredefinedTransactions), loader.batchSize)

	batchInterval := time.Duration(loader.batchSize) * time.Millisecond
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-loader.stopChannel:
			return
		case <-ticker.C:
			currentIdx := atomic.LoadInt64(&loader.currentIndex)
			totalTx := int64(len(loader.graphStructure.PredefinedTransactions))

			if currentIdx >= totalTx {
				fmt.Printf("✅ All %d transactions sent in batch mode\n", len(loader.graphStructure.PredefinedTransactions))
				return
			}

			// 배치 크기 결정
			remaining := totalTx - currentIdx
			batchSize := int64(loader.batchSize)
			if remaining < batchSize {
				batchSize = remaining
			}

			// 배치 생성
			messages := make([]kafka.Message[*sharedDomain.MarkedTransaction], 0, batchSize)
			for i := int64(0); i < batchSize; i++ {
				tx := &loader.graphStructure.PredefinedTransactions[currentIdx+i]
				messages = append(messages, kafka.Message[*sharedDomain.MarkedTransaction]{
					Key:   []byte("tx"),
					Value: tx,
				})
			}

			// 배치 전송
			if err := loader.batchProducer.PublishMessagesBatch(ctx, messages); err != nil {
				atomic.AddInt64(&loader.stats.Dropped, batchSize)
			} else {
				atomic.AddInt64(&loader.stats.Transmitted, batchSize)
			}

			atomic.AddInt64(&loader.stats.Generated, batchSize)
			atomic.AddInt64(&loader.currentIndex, batchSize)
		}
	}
}

// Stop 전송 중지
func (loader *TxDefineLoader) Stop() {
	loader.stopOnce.Do(func() {
		close(loader.stopChannel)
	})
	<-loader.doneChannel
}

// GetGraphStructure 그래프 구조 반환 (검증용)
func (loader *TxDefineLoader) GetGraphStructure() *DeterministicGraphStructure {
	loader.mutex.RLock()
	defer loader.mutex.RUnlock()
	return loader.graphStructure
}

// GetPipelineStats 파이프라인 통계 반환
func (loader *TxDefineLoader) GetPipelineStats() PipelineStats {
	return PipelineStats{
		Generated:   atomic.LoadInt64(&loader.stats.Generated),
		Transmitted: atomic.LoadInt64(&loader.stats.Transmitted),
		Processed:   atomic.LoadInt64(&loader.stats.Processed),
		Dropped:     atomic.LoadInt64(&loader.stats.Dropped),
		StartTime:   loader.stats.StartTime,
	}
}

// GetTPS TPS 계산
func (loader *TxDefineLoader) GetTPS() float64 {
	generated := atomic.LoadInt64(&loader.stats.Generated)
	elapsed := time.Since(loader.stats.StartTime).Seconds()
	if elapsed > 0 {
		return float64(generated) / elapsed
	}
	return 0
}

// Close 리소스 정리
func (loader *TxDefineLoader) Close() error {
	loader.Stop()

	if loader.kafkaProducer != nil {
		if err := loader.kafkaProducer.Close(); err != nil {
			fmt.Printf("⚠️ Kafka producer close error: %v\n", err)
		}
	}

	if loader.batchProducer != nil {
		if err := loader.batchProducer.Close(); err != nil {
			fmt.Printf("⚠️ Batch producer close error: %v\n", err)
		}
	}

	return nil
}

// IsCompleted 모든 트랜잭션 전송 완료 여부
func (loader *TxDefineLoader) IsCompleted() bool {
	currentIdx := atomic.LoadInt64(&loader.currentIndex)
	return currentIdx >= int64(len(loader.graphStructure.PredefinedTransactions))
}

// GetProgress 진행률 반환 (0.0 ~ 1.0)
func (loader *TxDefineLoader) GetProgress() float64 {
	currentIdx := atomic.LoadInt64(&loader.currentIndex)
	total := int64(len(loader.graphStructure.PredefinedTransactions))
	if total == 0 {
		return 1.0
	}
	return float64(currentIdx) / float64(total)
}

// ValidateResults 결과 검증을 위한 분석 함수들
func (loader *TxDefineLoader) ValidateResults() map[string]interface{} {
	graph := loader.graphStructure

	validation := map[string]interface{}{
		"expected_cex_count":       graph.ExpectedCEXCount,
		"expected_deposit_count":   graph.ExpectedDepositCount,
		"expected_user_count":      graph.ExpectedUserCount,
		"total_transactions":       graph.TotalTransactions,
		"transactions_by_category": graph.TransactionsByCategory,
		"multi_user_deposits":      len(graph.MultiUserDeposits),
		"single_user_deposits":     len(graph.SingleUserDeposits),
		"multi_deposit_users":      len(graph.MultiDepositUsers),
		"single_deposit_users":     len(graph.SingleDepositUsers),
		"inter_user_only_users":    len(graph.InterUserOnlyUsers),
		"user_to_user_pairs":       len(graph.UserToUserPairs),
	}

	return validation
}

// setupEnvironment 환경 설정 (TxFeeder와 동일한 방식)
func (loader *TxDefineLoader) setupEnvironment(config *TxFeederConfig) error {
	fmt.Println("\n2️⃣ Setting up TxDefineLoader environment...")

	// 환경 디렉터리 설정
	if config.TargetIsolatedTestingDir != "" {
		// 기존 디렉토리 제거 후 새로 생성
		os.RemoveAll(config.TargetIsolatedTestingDir)
		if err := os.MkdirAll(config.TargetIsolatedTestingDir, 0755); err != nil {
			return fmt.Errorf("failed to create isolated directory: %w", err)
		}
	}

	// CEX 데이터 복제 및 로딩
	if config.TargetIsolatedCEXFilePath != "" && config.ProjectRootDir != "" {
		sourceCEX := filepath.Join(config.ProjectRootDir, "shared", "txfeeder", "infra", "real_cex.txt")
		fmt.Printf("   🔍 Source CEX: %s\n", sourceCEX)
		fmt.Printf("   🔍 Target CEX: %s\n", config.TargetIsolatedCEXFilePath)

		// 소스 파일 존재 확인
		if _, err := os.Stat(sourceCEX); os.IsNotExist(err) {
			return fmt.Errorf("source CEX file does not exist: %s", sourceCEX)
		}

		if err := loader.copyFile(sourceCEX, config.TargetIsolatedCEXFilePath); err != nil {
			return fmt.Errorf("failed to copy CEX file: %w", err)
		}

		// CEX Set 로딩
		if err := loader.loadCEXSetFromFile(config.TargetIsolatedCEXFilePath); err != nil {
			return fmt.Errorf("failed to load CEX set: %w", err)
		}
	}

	// 모의 입금 주소 생성 및 로딩
	if config.TargetIsolatedMockDepositFilePath != "" && config.ProjectRootDir != "" {
		if err := loader.createMockDeposits(config.TargetIsolatedMockDepositFilePath, config.ProjectRootDir); err != nil {
			return fmt.Errorf("failed to create mock deposits: %w", err)
		}

		// Mock Deposit 주소 로딩
		if err := loader.mockDepositAddrs.LoadFromFile(config.TargetIsolatedMockDepositFilePath); err != nil {
			return fmt.Errorf("failed to load mock deposits: %w", err)
		}
	}

	fmt.Printf("   ✅ TxDefineLoader environment prepared\n")
	return nil
}

// loadCEXSetFromFile CEX 주소 집합을 파일에서 로드
func (loader *TxDefineLoader) loadCEXSetFromFile(cexFilePath string) error {
	fmt.Printf("   🔍 Loading CEX file: %s\n", cexFilePath)

	// 파일 존재 확인
	if _, err := os.Stat(cexFilePath); os.IsNotExist(err) {
		return fmt.Errorf("CEX file does not exist: %s", cexFilePath)
	}

	file, err := os.Open(cexFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CEX file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		cexAddr := sharedDomain.NewCEXAddress(line)
		if cexAddr.IsValid() {
			loader.cexSet.Add(cexAddr.Address)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read CEX file: %w", err)
	}

	fmt.Printf("   📦 CEX addresses loaded: %d\n", loader.cexSet.Size())

	// CEX 로딩이 실패한 경우 추가 디버깅
	if loader.cexSet.Size() == 0 {
		return fmt.Errorf("no CEX addresses loaded from file")
	}

	// CEX 주소 샘플 출력 (디버깅)
	cexAddresses := loader.cexSet.GetAll()
	if len(cexAddresses) >= 3 {
		fmt.Printf("   🔍 CEX samples: %s, %s, %s\n",
			cexAddresses[0][:10]+"...",
			cexAddresses[1][:10]+"...",
			cexAddresses[2][:10]+"...")
	}

	return nil
}

// createMockDeposits 모의 입금 주소 생성
func (loader *TxDefineLoader) createMockDeposits(filePath, projectRoot string) error {
	fmt.Printf("   🔍 Creating mock deposit addresses at %s\n", filePath)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	depositFilePath := filepath.Join(projectRoot, "shared", "txfeeder", "infra", "mocked_hidden_deposits.txt")
	fmt.Printf("   📄 Loading from %s\n", depositFilePath)

	deposits, err := os.Open(depositFilePath)
	if err != nil {
		return err
	}
	defer deposits.Close()

	file.WriteString("# Mock Deposit Addresses for TxDefineLoader\n\n")
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

// copyFile 파일 복사 헬퍼
func (loader *TxDefineLoader) copyFile(src, dst string) error {
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
