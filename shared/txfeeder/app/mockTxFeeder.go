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
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/txfeeder/domain"
)

// EnvironmentConfig 환경 설정을 위한 구조체
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

// MockTxFeeder generates transactions for testing EE module with pipeline management
type MockTxFeeder struct {
	config           *domain.TxGeneratorConfig
	state            *domain.TxGeneratorState
	mockDepositAddrs *domain.MockDepositAddressSet
	cexSet           *sharedDomain.CEXSet

	// Channels for transaction output
	markedTxChannel chan sharedDomain.MarkedTransaction

	// Control channels
	stopChannel chan struct{}
	doneChannel chan struct{}

	// Synchronization
	mutex    sync.RWMutex
	stopOnce sync.Once // 중복 stop 방지

	// Environment management
	baseDir     string
	isolatedDir string

	// Pipeline management - 출력 채널 등록 방식
	requestedOutputChannels []chan<- *sharedDomain.MarkedTransaction // 등록된 출력 채널들
	
	// Pipeline 통계 및 상태 관리
	stats       PipelineStats
	debugStats  DebugStats
	wg          sync.WaitGroup
	channelOnce sync.Once // 트랜잭션 채널 중복 닫기 방지
}

// NewTxFeeder creates a new transaction generator with pipeline capabilities
func NewTxFeeder(config *domain.TxGeneratorConfig, cexSet *sharedDomain.CEXSet) *MockTxFeeder {
	return &MockTxFeeder{
		config:           config,
		state:            domain.NewTxGeneratorState(config.StartTime, config.TimeIncrementDuration, config.TransactionsPerTimeIncrement),
		mockDepositAddrs: domain.NewMockDepositAddressSet(),
		cexSet:           cexSet,
		markedTxChannel:  make(chan sharedDomain.MarkedTransaction, 100_000), // Buffer for 10k transactions
		stopChannel:      make(chan struct{}),
		doneChannel:      make(chan struct{}),
		requestedOutputChannels: make([]chan<- *sharedDomain.MarkedTransaction, 0),
		stats: PipelineStats{
			StartTime: time.Now(),
		},
		debugStats: DebugStats{},
	}
}

// LoadMockDepositAddresses loads mock deposit addresses from file
func (g *MockTxFeeder) LoadMockDepositAddresses(filePath string) error {
	return g.mockDepositAddrs.LoadFromFile(filePath)
}

// Start begins transaction generation
func (g *MockTxFeeder) Start(ctx context.Context) error {
	go g.generateTransactions(ctx)
	return nil
}

// Stop stops transaction generation (safe for multiple calls)
func (g *MockTxFeeder) Stop() {
	g.stopOnce.Do(func() {
		close(g.stopChannel)
	})
	<-g.doneChannel
}

// GetTxChannel returns the channel for receiving generated transactions
func (g *MockTxFeeder) GetTxChannel() <-chan sharedDomain.MarkedTransaction {
	return g.markedTxChannel
}

// GetGeneratedCount returns the current count of generated transactions
func (g *MockTxFeeder) GetGeneratedCount() int64 {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	return g.state.GeneratedCount
}

// RegisterOutputChannel registers a channel to receive generated transactions
func (g *MockTxFeeder) RegisterOutputChannel(outputCh chan<- *sharedDomain.MarkedTransaction) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.requestedOutputChannels = append(g.requestedOutputChannels, outputCh)
}

// GetPipelineStats returns current pipeline statistics
func (g *MockTxFeeder) GetPipelineStats() PipelineStats {
	return PipelineStats{
		Generated:   atomic.LoadInt64(&g.stats.Generated),
		Transmitted: atomic.LoadInt64(&g.stats.Transmitted),
		Processed:   atomic.LoadInt64(&g.stats.Processed),
		Dropped:     atomic.LoadInt64(&g.stats.Dropped),
		StartTime:   g.stats.StartTime,
	}
}

// generateTransactions is the main generation loop running in goroutine
func (g *MockTxFeeder) generateTransactions(ctx context.Context) {
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

			// Send to all registered output channels
			g.sendToOutputChannels(&tx, ctx)
		}
	}
}

// generateSingleTransaction generates a single MarkedTransaction
func (g *MockTxFeeder) generateSingleTransaction() sharedDomain.MarkedTransaction {
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
func (g *MockTxFeeder) determineTransactionType() TransactionType {
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
func (g *MockTxFeeder) generateDepositToCexTransaction() sharedDomain.MarkedTransaction {
	fromAddr := g.mockDepositAddrs.GetRandomAddress()
	toAddr := g.getRandomCexAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// generateRandomToDepositTransaction generates random -> mockedDepositAddress transaction
func (g *MockTxFeeder) generateRandomToDepositTransaction() sharedDomain.MarkedTransaction {
	fromAddr := domain.GenerateRandomAddress()
	toAddr := g.mockDepositAddrs.GetRandomAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// generateRandomTransaction generates a completely random transaction
func (g *MockTxFeeder) generateRandomTransaction() sharedDomain.MarkedTransaction {
	fromAddr := domain.GenerateRandomAddress()
	toAddr := domain.GenerateRandomAddress()

	return g.createMarkedTransaction(fromAddr, toAddr)
}

// createMarkedTransaction creates a MarkedTransaction with given from/to addresses
func (g *MockTxFeeder) createMarkedTransaction(from, to sharedDomain.Address) sharedDomain.MarkedTransaction {
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
		BlockTime:   g.state.CurrentTime,
		TxID:        txID,
		TxSyntax:    [2]sharedDomain.ContractBoolMark{sharedDomain.EOAMark, sharedDomain.EOAMark}, // Assume EOA-to-EOA for simplicity
		Nonce:       uint64(g.state.GeneratedCount),
		BlockNumber: sharedDomain.BlockNumber(g.state.GeneratedCount / 100), // Rough block number
		From:        from,
		To:          to,
		Value:       sharedDomain.BigInt{Int: randomValue},
		GasLimit:    sharedDomain.BigInt{Int: big.NewInt(21000)}, // Standard ETH transfer gas
		Input:       "",                                          // Empty for ETH transfers
	}
}

// getRandomCexAddress returns a random CEX address from the loaded set
func (g *MockTxFeeder) getRandomCexAddress() sharedDomain.Address {
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
func (g *MockTxFeeder) parseAddressString(hexStr string) (sharedDomain.Address, error) {
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

// sendToOutputChannels sends transaction to all registered output channels
func (g *MockTxFeeder) sendToOutputChannels(tx *sharedDomain.MarkedTransaction, ctx context.Context) {
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

// SetupEnvironment 격리된 테스트 환경을 설정 (feed_and_analyze.go에서 이동)
func (g *MockTxFeeder) SetupEnvironment(envConfig *EnvironmentConfig) error {
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
func (g *MockTxFeeder) LoadCEXSetFromFile(cexFilePath string) (*sharedDomain.CEXSet, error) {
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
func (g *MockTxFeeder) CleanupEnvironment() {
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
func (g *MockTxFeeder) Close() error {
	// Stop을 호출해서 트랜잭션 생성 중지
	g.Stop()
	return nil
}

// 내부 헬퍼 메서드들 (feed_and_analyze.go에서 이동)
func (g *MockTxFeeder) copyFile(src, dst string) error {
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

func (g *MockTxFeeder) createMockDeposits(filePath string) error {
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
func (g *MockTxFeeder) findProjectRoot() string {
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
