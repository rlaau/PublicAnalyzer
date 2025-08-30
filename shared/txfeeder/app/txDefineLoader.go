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

// ===== 유틸 타입 =====

type catSets = map[sharedDomain.Address]struct{}

// ===== 결정론적 그래프 정의 =====

type DeterministicGraphStructure struct {
	// 기본 엔티티
	CEXAddresses     []sharedDomain.Address
	DepositAddresses []sharedDomain.Address
	UserAddresses    []sharedDomain.Address

	// 분류
	MultiUserDeposits  []sharedDomain.Address
	SingleUserDeposits []sharedDomain.Address
	MultiDepositUsers  []sharedDomain.Address
	SingleDepositUsers []sharedDomain.Address
	InterUserOnlyUsers []sharedDomain.Address

	// 관계
	DepositToUsers  map[sharedDomain.Address][]sharedDomain.Address
	UserToDeposits  map[sharedDomain.Address][]sharedDomain.Address
	UserToUserPairs [][2]sharedDomain.Address

	// 트랜잭션
	PredefinedTransactions []sharedDomain.MarkedTransaction

	// 통계
	ExpectedCEXCount       int
	ExpectedDepositCount   int
	ExpectedUserCount      int
	TotalTransactions      int
	TransactionsByCategory map[string]int
}

// ===== 로더 =====

type TxDefineLoader struct {
	// 기본 설정
	cexSet           *sharedDomain.CEXSet
	mockDepositAddrs *domain.MockDepositAddressSet
	kafkaProducer    *kafka.KafkaProducer[*sharedDomain.MarkedTransaction]
	batchProducer    *kafka.KafkaBatchProducer[*sharedDomain.MarkedTransaction]
	kafkaBrokers     []string
	batchMode        bool
	batchSize        int
	batchTimeout     time.Duration

	// 결정 구조
	graphStructure *DeterministicGraphStructure
	currentIndex   int64

	// 제어
	stopChannel chan struct{}
	doneChannel chan struct{}

	// 동기화/통계
	mutex       sync.RWMutex
	stopOnce    sync.Once
	targetTotal int // 목표 총 Tx 수 (예: 400_000)
	stats       PipelineStats
}

// ===== 생성자 =====

func NewTxDefineLoader(config *TxFeederConfig) (*TxDefineLoader, error) {
	kafkaBrokers := []string{"localhost:9092"}
	kafkaConfig := kafka.KafkaBatchConfig{
		Brokers: kafkaBrokers,
		Topic:   kafka.TestFedTxTopic,
	}
	kp := kafka.NewKafkaProducer[*sharedDomain.MarkedTransaction](kafkaConfig)

	loader := &TxDefineLoader{
		cexSet:           sharedDomain.NewCEXSet(),
		mockDepositAddrs: domain.NewMockDepositAddressSet(),
		kafkaProducer:    kp,
		kafkaBrokers:     kafkaBrokers,
		batchMode:        config.BatchMode,
		batchSize:        config.BatchSize,
		batchTimeout:     config.BatchTimeout,
		stopChannel:      make(chan struct{}),
		doneChannel:      make(chan struct{}),
		targetTotal:      0,
		stats:            PipelineStats{StartTime: time.Now()},
	}

	if config.GenConfig != nil && config.GenConfig.TotalTransactions > 0 {
		loader.targetTotal = config.GenConfig.TotalTransactions
	}

	if config.BatchMode {
		loader.batchProducer = kafka.NewKafkaBatchProducer[*sharedDomain.MarkedTransaction](
			kafka.KafkaBatchConfig{
				Brokers:      kafkaBrokers,
				Topic:        kafka.TestFedTxTopic,
				GroupID:      "tx-define-loader-batch-group",
				BatchSize:    config.BatchSize,
				BatchTimeout: config.BatchTimeout,
			},
		)
	}

	// 환경 준비 (CEX/Deposit 파일)
	if err := loader.setupEnvironment(config); err != nil {
		return nil, fmt.Errorf("setup env: %w", err)
	}

	// 베이스 시퀀스 생성
	if err := loader.generateDeterministicGraph(); err != nil {
		return nil, fmt.Errorf("generate graph: %w", err)
	}

	// 목표 총개수로 비율 유지 스케일링
	if loader.targetTotal > 0 {
		if err := loader.scaleTransactionsToTarget(loader.targetTotal); err != nil {
			return nil, fmt.Errorf("scale: %w", err)
		}
	}

	fmt.Printf("✅ TxDefineLoader created with %d predefined transactions (target=%d)\n",
		len(loader.graphStructure.PredefinedTransactions), loader.targetTotal)
	loader.printGraphSummary()
	return loader, nil
}

// ===== 카테고리 세트(로컬) & 판정 =====

type categorySets struct {
	user    map[sharedDomain.Address]struct{}
	deposit map[sharedDomain.Address]struct{}
	cex     map[sharedDomain.Address]struct{}
}

func buildCategorySets(g *DeterministicGraphStructure) categorySets {
	toSet := func(addrs []sharedDomain.Address) map[sharedDomain.Address]struct{} {
		m := make(map[sharedDomain.Address]struct{}, len(addrs))
		for _, a := range addrs {
			m[a] = struct{}{}
		}
		return m
	}
	return categorySets{
		user:    toSet(g.UserAddresses),
		deposit: toSet(g.DepositAddresses),
		cex:     toSet(g.CEXAddresses),
	}
}

func inSet(m map[sharedDomain.Address]struct{}, a sharedDomain.Address) bool {
	_, ok := m[a]
	return ok
}

func txCategoryFor(tx sharedDomain.MarkedTransaction, sets categorySets) string {
	switch {
	case inSet(sets.user, tx.From) && inSet(sets.deposit, tx.To):
		return "user_to_deposit"
	case inSet(sets.deposit, tx.From) && inSet(sets.cex, tx.To):
		return "deposit_to_cex"
	case inSet(sets.user, tx.From) && inSet(sets.user, tx.To):
		return "user_to_user"
	default:
		return "other"
	}
}

// ===== 스케일링 =====

func (loader *TxDefineLoader) scaleTransactionsToTarget(target int) error {
	g := loader.graphStructure
	base := g.PredefinedTransactions
	baseN := len(base)
	if baseN == 0 {
		return fmt.Errorf("no base transactions to scale")
	}
	if target == baseN {
		g.TotalTransactions = baseN
		return nil
	}
	if target < baseN {
		return loader.downscaleToTarget(target)
	}

	sets := buildCategorySets(g)

	// 1) 기본 분포
	baseCat := map[string]int{}
	for _, tx := range base {
		baseCat[txCategoryFor(tx, sets)]++
	}

	// 2) 정수 배수/나머지
	mult := target / baseN
	rem := target % baseN

	// 3) 목표 분포(반올림)
	desiredCat := map[string]int{}
	for k, c := range baseCat {
		desiredCat[k] = int((float64(c) * float64(target) / float64(baseN)) + 0.5)
	}
	// 합 보정
	sum := 0
	for _, v := range desiredCat {
		sum += v
	}
	if sum != target {
		desiredCat["user_to_deposit"] += (target - sum)
	}

	out := make([]sharedDomain.MarkedTransaction, 0, target)

	// 시간 오프셋(사이클 간 겹침 방지)
	baseStart := time.Time(base[0].BlockTime)
	baseEnd := baseStart
	for _, tx := range base {
		bt := time.Time(tx.BlockTime)
		if bt.After(baseEnd) {
			baseEnd = bt
		}
	}
	baseSpan := baseEnd.Sub(baseStart)
	if baseSpan <= 0 {
		baseSpan = time.Second
	}

	cloneCycle := func(cycle int, limit int) int {
		used := 0
		timeOffset := time.Duration(cycle+1) * (baseSpan + time.Hour)
		seedBase := int64(cycle+1) * int64(baseN)

		for i := 0; i < baseN && used < limit; i++ {
			cat := txCategoryFor(base[i], sets)
			if desiredCat[cat] <= 0 {
				continue
			}
			from := base[i].From
			to := base[i].To
			seed := int(seedBase) + i

			orig := time.Time(base[i].BlockTime)
			nt := orig.Add(timeOffset)

			ntx := loader.createMarkedTransaction(from, to, seed, nt)
			out = append(out, ntx)

			desiredCat[cat]--
			used++
		}
		return used
	}

	// 4) 정수 배수 사이클
	for cycle := 0; cycle < mult; cycle++ {
		_ = cloneCycle(cycle, baseN)
	}
	// 5) 나머지
	if rem > 0 {
		_ = cloneCycle(mult, rem)
	}
	// 6) 보충
	if len(out) < target {
		_ = cloneCycle(mult+1, target-len(out))
	}

	// 7) 반영 + 재집계
	g.PredefinedTransactions = out
	g.TransactionsByCategory = map[string]int{}
	for _, tx := range out {
		g.TransactionsByCategory[txCategoryFor(tx, sets)]++
	}
	g.TotalTransactions = len(out)
	return nil
}

func (loader *TxDefineLoader) downscaleToTarget(target int) error {
	g := loader.graphStructure
	base := g.PredefinedTransactions
	baseN := len(base)

	sets := buildCategorySets(g)

	// 1) 기본 분포
	baseCat := map[string]int{}
	for _, tx := range base {
		baseCat[txCategoryFor(tx, sets)]++
	}
	// 2) 목표 분포(반올림)
	want := map[string]int{}
	sum := 0
	for k, c := range baseCat {
		want[k] = int((float64(c) * float64(target) / float64(baseN)) + 0.5)
		sum += want[k]
	}
	if sum != target {
		want["user_to_deposit"] += (target - sum)
	}

	// 3) 선별
	out := make([]sharedDomain.MarkedTransaction, 0, target)
	have := map[string]int{}
	for _, tx := range base {
		if len(out) >= target {
			break
		}
		cat := txCategoryFor(tx, sets)
		if have[cat] < want[cat] {
			out = append(out, tx)
			have[cat]++
		}
	}

	g.PredefinedTransactions = out
	g.TransactionsByCategory = have
	g.TotalTransactions = len(out)
	return nil
}

// ===== 결정 그래프 생성 =====

func (loader *TxDefineLoader) generateDeterministicGraph() error {
	loader.mutex.Lock()
	defer loader.mutex.Unlock()

	graph := &DeterministicGraphStructure{
		DepositToUsers:         make(map[sharedDomain.Address][]sharedDomain.Address),
		UserToDeposits:         make(map[sharedDomain.Address][]sharedDomain.Address),
		TransactionsByCategory: make(map[string]int),
	}

	// 1) CEX 20
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

	// 2) Deposit 200 (결정론)
	if loader.mockDepositAddrs.Size() < 200 {
		return fmt.Errorf("insufficient mock deposit addresses: need 200, got %d", loader.mockDepositAddrs.Size())
	}
	allDeposits := make([]sharedDomain.Address, 0, 200)
	for i := 0; i < 200; i++ {
		idx := (i * 17) % loader.mockDepositAddrs.Size()
		allDeposits = append(allDeposits, loader.getDeterministicDepositAddress(idx))
	}
	graph.DepositAddresses = allDeposits
	graph.MultiUserDeposits = allDeposits[:150]
	graph.SingleUserDeposits = allDeposits[150:]

	// 3) User 1000 (결정론)
	allUsers := make([]sharedDomain.Address, 0, 1000)
	for i := 0; i < 1000; i++ {
		allUsers = append(allUsers, loader.generateDeterministicAddress(i))
	}
	graph.UserAddresses = allUsers
	graph.MultiDepositUsers = allUsers[:500]
	graph.SingleDepositUsers = allUsers[500:800]
	graph.InterUserOnlyUsers = allUsers[800:]

	// 4) 관계
	loader.createDetailedRelationships(graph)

	// 5) Tx 생성
	loader.generateDetailedTransactions(graph)

	graph.ExpectedCEXCount = len(graph.CEXAddresses)
	graph.ExpectedDepositCount = len(graph.DepositAddresses)
	graph.ExpectedUserCount = len(graph.UserAddresses)
	graph.TotalTransactions = len(graph.PredefinedTransactions)

	loader.graphStructure = graph
	return nil
}

func (loader *TxDefineLoader) createDetailedRelationships(graph *DeterministicGraphStructure) {
	// 1) MultiUser Deposits: 2~20명
	userIdx := 0
	for i, deposit := range graph.MultiUserDeposits {
		usersPerDeposit := 2 + (i % 19)
		users := make([]sharedDomain.Address, 0, usersPerDeposit)
		for j := 0; j < usersPerDeposit && userIdx < len(graph.UserAddresses); j++ {
			user := graph.UserAddresses[userIdx]
			users = append(users, user)
			graph.UserToDeposits[user] = append(graph.UserToDeposits[user], deposit)
			userIdx++
		}
		graph.DepositToUsers[deposit] = users
	}

	// 2) SingleUser Deposits: 1명
	for _, deposit := range graph.SingleUserDeposits {
		if userIdx >= len(graph.UserAddresses) {
			break
		}
		user := graph.UserAddresses[userIdx]
		graph.DepositToUsers[deposit] = []sharedDomain.Address{user}
		graph.UserToDeposits[user] = append(graph.UserToDeposits[user], deposit)
		userIdx++
	}

	// 3) MultiDeposit Users: 각 2~5개 보장
	for i, user := range graph.MultiDepositUsers {
		depositsPerUser := 2 + (i % 4) // 2~5
		existing := len(graph.UserToDeposits[user])
		need := depositsPerUser - existing
		for j := 0; j < need; j++ {
			depositIdx := (i*7 + j*11) % len(graph.DepositAddresses)
			deposit := graph.DepositAddresses[depositIdx]
			dup := false
			for _, d := range graph.UserToDeposits[user] {
				if d == deposit {
					dup = true
					break
				}
			}
			if !dup {
				graph.UserToDeposits[user] = append(graph.UserToDeposits[user], deposit)
				graph.DepositToUsers[deposit] = append(graph.DepositToUsers[deposit], user)
			}
		}
	}

	// 4) InterUserOnly: 쌍 구성
	for i := 0; i < len(graph.InterUserOnlyUsers)-1; i += 2 {
		u1 := graph.InterUserOnlyUsers[i]
		u2 := graph.InterUserOnlyUsers[i+1]
		graph.UserToUserPairs = append(graph.UserToUserPairs, [2]sharedDomain.Address{u1, u2})
	}
}

func (loader *TxDefineLoader) generateDetailedTransactions(graph *DeterministicGraphStructure) {
	startTime, _ := time.Parse("2006-01-02", "2025-01-01")
	currentTime := startTime

	txs := make([]sharedDomain.MarkedTransaction, 0, 4000)

	// 1) MultiDeposit Users → 각 deposit
	for _, user := range graph.MultiDepositUsers {
		for _, deposit := range graph.UserToDeposits[user] {
			tx := loader.createMarkedTransaction(user, deposit, len(txs), currentTime)
			txs = append(txs, tx)
			graph.TransactionsByCategory["user_to_deposit"]++
			currentTime = currentTime.Add(30 * time.Minute)
		}
	}

	// 2) SingleDeposit Users → 첫 deposit
	for _, user := range graph.SingleDepositUsers {
		deposits := graph.UserToDeposits[user]
		if len(deposits) > 0 {
			deposit := deposits[0]
			tx := loader.createMarkedTransaction(user, deposit, len(txs), currentTime)
			txs = append(txs, tx)
			graph.TransactionsByCategory["user_to_deposit"]++
			currentTime = currentTime.Add(30 * time.Minute)
		}
	}

	// 3) User ↔ User (양방향)
	for _, pair := range graph.UserToUserPairs {
		tx1 := loader.createMarkedTransaction(pair[0], pair[1], len(txs), currentTime)
		txs = append(txs, tx1)
		graph.TransactionsByCategory["user_to_user"]++
		currentTime = currentTime.Add(30 * time.Minute)

		tx2 := loader.createMarkedTransaction(pair[1], pair[0], len(txs), currentTime)
		txs = append(txs, tx2)
		graph.TransactionsByCategory["user_to_user"]++
		currentTime = currentTime.Add(30 * time.Minute)
	}

	// 4) Deposit → CEX (임의 매핑, 500개)
	for i := 0; i < 500; i++ {
		deposit := graph.DepositAddresses[i%len(graph.DepositAddresses)]
		cex := graph.CEXAddresses[i%len(graph.CEXAddresses)]
		tx := loader.createMarkedTransaction(deposit, cex, len(txs), currentTime)
		txs = append(txs, tx)
		graph.TransactionsByCategory["deposit_to_cex"]++
		currentTime = currentTime.Add(30 * time.Minute)
	}

	graph.PredefinedTransactions = txs
}

// ===== 주소/Tx 생성 =====

func (loader *TxDefineLoader) generateDeterministicAddress(seed int) sharedDomain.Address {
	var addr sharedDomain.Address
	for i := 0; i < 20; i++ {
		addr[i] = byte((seed*7 + i*13) % 256)
	}
	return addr
}

func (loader *TxDefineLoader) getDeterministicDepositAddress(index int) sharedDomain.Address {
	var addr sharedDomain.Address
	for i := 0; i < 20; i++ {
		addr[i] = byte((index*19 + i*29) % 256)
	}
	return addr
}

func (loader *TxDefineLoader) createMarkedTransaction(from, to sharedDomain.Address, seed int, blockTime time.Time) sharedDomain.MarkedTransaction {
	// TxID
	var txID sharedDomain.TxId
	for i := 0; i < 32; i++ {
		txID[i] = byte((seed*17 + i*23) % 256)
	}

	// Value (0.1 ~ 10 ETH)
	minWei := big.NewInt(100000000000000000) // 0.1 ETH
	maxWei := new(big.Int)
	maxWei.SetString("10000000000000000000", 10) // 10 ETH
	diff := new(big.Int).Sub(maxWei, minWei)

	value := new(big.Int).Mod(big.NewInt(int64(seed)), diff)
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

// ===== 출력/검증 =====

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

// ===== 실행 =====

func (loader *TxDefineLoader) Start(ctx context.Context) error {
	if err := kafka.EnsureKafkaConnection(loader.kafkaBrokers); err != nil {
		return fmt.Errorf("kafka connection failed: %w", err)
	}
	if err := kafka.CreateTopicIfNotExists(loader.kafkaBrokers, kafka.TestFedTxTopic, 1, 1); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	if loader.batchMode {
		go loader.sendTransactionsBatch(ctx)
	} else {
		go loader.sendTransactions(ctx)
	}
	return nil
}

func (loader *TxDefineLoader) sendTransactions(ctx context.Context) {
	defer close(loader.doneChannel)

	total := len(loader.graphStructure.PredefinedTransactions)
	fmt.Printf("📤 Starting deterministic transaction sending: %d transactions\n", total)

	interval := 50 * time.Microsecond
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
			if currentIdx >= int64(total) {
				fmt.Printf("✅ All %d transactions sent\n", total)
				return
			}
			tx := &loader.graphStructure.PredefinedTransactions[currentIdx]
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

func (loader *TxDefineLoader) sendTransactionsBatch(ctx context.Context) {
	defer close(loader.doneChannel)

	total := len(loader.graphStructure.PredefinedTransactions)
	fmt.Printf("🚀 Starting deterministic batch transaction sending: %d transactions (batch size: %d)\n",
		total, loader.batchSize)

	batchInterval := time.Duration(loader.batchSize) * time.Millisecond / 10
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
			totalTx := int64(total)
			if currentIdx >= totalTx {
				fmt.Printf("✅ All %d transactions sent in batch mode\n", total)
				return
			}

			remaining := totalTx - currentIdx
			batchSize := int64(loader.batchSize)
			if remaining < batchSize {
				batchSize = remaining
			}

			msgs := make([]kafka.Message[*sharedDomain.MarkedTransaction], 0, batchSize)
			for i := int64(0); i < batchSize; i++ {
				tx := &loader.graphStructure.PredefinedTransactions[currentIdx+i]
				msgs = append(msgs, kafka.Message[*sharedDomain.MarkedTransaction]{
					Key:   []byte("tx"),
					Value: tx,
				})
			}

			if err := loader.batchProducer.PublishMessagesBatch(ctx, msgs); err != nil {
				atomic.AddInt64(&loader.stats.Dropped, batchSize)
			} else {
				atomic.AddInt64(&loader.stats.Transmitted, batchSize)
			}

			atomic.AddInt64(&loader.stats.Generated, batchSize)
			atomic.AddInt64(&loader.currentIndex, batchSize)
		}
	}
}

func (loader *TxDefineLoader) Stop() {
	loader.stopOnce.Do(func() {
		close(loader.stopChannel)
	})
	<-loader.doneChannel
}

func (loader *TxDefineLoader) GetGraphStructure() *DeterministicGraphStructure {
	loader.mutex.RLock()
	defer loader.mutex.RUnlock()
	return loader.graphStructure
}

func (loader *TxDefineLoader) GetPipelineStats() PipelineStats {
	return PipelineStats{
		Generated:   atomic.LoadInt64(&loader.stats.Generated),
		Transmitted: atomic.LoadInt64(&loader.stats.Transmitted),
		Processed:   atomic.LoadInt64(&loader.stats.Processed),
		Dropped:     atomic.LoadInt64(&loader.stats.Dropped),
		StartTime:   loader.stats.StartTime,
	}
}

func (loader *TxDefineLoader) GetTPS() float64 {
	generated := atomic.LoadInt64(&loader.stats.Generated)
	elapsed := time.Since(loader.stats.StartTime).Seconds()
	if elapsed > 0 {
		return float64(generated) / elapsed
	}
	return 0
}

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

func (loader *TxDefineLoader) IsCompleted() bool {
	currentIdx := atomic.LoadInt64(&loader.currentIndex)
	return currentIdx >= int64(len(loader.graphStructure.PredefinedTransactions))
}

func (loader *TxDefineLoader) GetProgress() float64 {
	currentIdx := atomic.LoadInt64(&loader.currentIndex)
	total := int64(len(loader.graphStructure.PredefinedTransactions))
	if total == 0 {
		return 1.0
	}
	return float64(currentIdx) / float64(total)
}

func (loader *TxDefineLoader) ValidateResults() map[string]interface{} {
	graph := loader.graphStructure
	return map[string]interface{}{
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
}

// ===== 환경 준비(CEX/Deposit 파일) =====

func (loader *TxDefineLoader) setupEnvironment(config *TxFeederConfig) error {
	fmt.Println("\n2️⃣ Setting up TxDefineLoader environment...")

	if config.TargetIsolatedTestingDir != "" {
		_ = os.RemoveAll(config.TargetIsolatedTestingDir)
		if err := os.MkdirAll(config.TargetIsolatedTestingDir, 0755); err != nil {
			return fmt.Errorf("create isolated dir: %w", err)
		}
	}

	// CEX 복제/로딩
	if config.TargetIsolatedCEXFilePath != "" && config.ProjectRootDir != "" {
		sourceCEX := filepath.Join(config.ProjectRootDir, "shared", "txfeeder", "infra", "real_cex.txt")
		fmt.Printf("   🔍 Source CEX: %s\n", sourceCEX)
		fmt.Printf("   🔍 Target CEX: %s\n", config.TargetIsolatedCEXFilePath)

		if _, err := os.Stat(sourceCEX); os.IsNotExist(err) {
			return fmt.Errorf("source CEX missing: %s", sourceCEX)
		}
		if err := loader.copyFile(sourceCEX, config.TargetIsolatedCEXFilePath); err != nil {
			return fmt.Errorf("copy CEX: %w", err)
		}
		if err := loader.loadCEXSetFromFile(config.TargetIsolatedCEXFilePath); err != nil {
			return fmt.Errorf("load CEX: %w", err)
		}
	}

	// Mock Deposit 생성/로딩
	if config.TargetIsolatedMockDepositFilePath != "" && config.ProjectRootDir != "" {
		if err := loader.createMockDeposits(config.TargetIsolatedMockDepositFilePath, config.ProjectRootDir); err != nil {
			return fmt.Errorf("create mock deposits: %w", err)
		}
		if err := loader.mockDepositAddrs.LoadFromFile(config.TargetIsolatedMockDepositFilePath); err != nil {
			return fmt.Errorf("load mock deposits: %w", err)
		}
	}

	fmt.Printf("   ✅ TxDefineLoader environment prepared\n")
	return nil
}

func (loader *TxDefineLoader) loadCEXSetFromFile(cexFilePath string) error {
	fmt.Printf("   🔍 Loading CEX file: %s\n", cexFilePath)
	if _, err := os.Stat(cexFilePath); os.IsNotExist(err) {
		return fmt.Errorf("CEX file does not exist: %s", cexFilePath)
	}
	f, err := os.Open(cexFilePath)
	if err != nil {
		return fmt.Errorf("open CEX: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cexAddr := sharedDomain.NewCEXAddress(line)
		if cexAddr.IsValid() {
			loader.cexSet.Add(cexAddr.Address)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan CEX: %w", err)
	}

	fmt.Printf("   📦 CEX addresses loaded: %d\n", loader.cexSet.Size())
	if loader.cexSet.Size() == 0 {
		return fmt.Errorf("no CEX addresses loaded")
	}
	cexAddresses := loader.cexSet.GetAll()
	if len(cexAddresses) >= 3 {
		fmt.Printf("   🔍 CEX samples: %s, %s, %s\n",
			cexAddresses[0][:10]+"...",
			cexAddresses[1][:10]+"...",
			cexAddresses[2][:10]+"...")
	}
	return nil
}

func (loader *TxDefineLoader) createMockDeposits(filePath, projectRoot string) error {
	fmt.Printf("   🔍 Creating mock deposit addresses at %s\n", filePath)

	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer out.Close()

	src := filepath.Join(projectRoot, "shared", "txfeeder", "infra", "mocked_hidden_deposits.txt")
	fmt.Printf("   📄 Loading from %s\n", src)

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if _, err := out.WriteString("# Mock Deposit Addresses for TxDefineLoader\n\n"); err != nil {
		return err
	}

	sc := bufio.NewScanner(in)
	totalLength := 0
	lineCount := 0
	for sc.Scan() {
		line := sc.Text()
		if _, err := out.WriteString(line + "\n"); err != nil {
			return err
		}
		totalLength += len(line)
		lineCount++
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read deposit file: %w", err)
	}

	fmt.Printf("   ✅ Copied %d lines (total %d bytes of address strings)\n", lineCount, totalLength)
	return nil
}

func (loader *TxDefineLoader) copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
