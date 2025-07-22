package domain

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

const (
	WindowSize      = 4 * 30 * 24 * time.Hour // 4개월 윈도우
	SlideInterval   = 7 * 24 * time.Hour      // 1주일 슬라이드
	TriggerInterval = 7 * 24 * time.Hour      // 1주일 트리거
	MaxTimeBuckets  = 21                      // 4개월 / 1주일 = 21개 버킷
)

// TimeBucket represents a time bucket in the sliding window
type TimeBucket struct {
	StartTime time.Time
	EndTime   time.Time
	ToUsers   map[string]time.Time // to_address -> first_active_time
}

// NewTimeBucket creates a new time bucket
func NewTimeBucket(startTime time.Time) *TimeBucket {
	return &TimeBucket{
		StartTime: startTime,
		EndTime:   startTime.Add(SlideInterval),
		ToUsers:   make(map[string]time.Time),
	}
}

// DualManager manages EOA relationships through sliding window analysis
type DualManager struct {
	groundKnowledge *GroundKnowledge
	graphRepo       GraphRepository

	// Sliding window management
	firstActiveTimeBuckets []*TimeBucket // 21개 타임버킷으로 4개월 윈도우 관리
	currentBucketIndex     int           // 현재 버킷 인덱스

	// Persistent KV storage for to->[]from mappings (대규모 데이터 처리용)
	pendingRelationsDB *badger.DB // to_address -> []from_address 영구 저장

	// Synchronization
	mutex sync.RWMutex
}

// NewDualManager creates a new dual manager instance
func NewDualManager(groundKnowledge *GroundKnowledge, graphRepo GraphRepository, pendingRelationsDBPath string) (*DualManager, error) {
	// Open BadgerDB for pending relations
	opts := badger.DefaultOptions(pendingRelationsDBPath)
	opts.Logger = nil
	pendingDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	dm := &DualManager{
		groundKnowledge:        groundKnowledge,
		graphRepo:              graphRepo,
		firstActiveTimeBuckets: make([]*TimeBucket, MaxTimeBuckets),
		currentBucketIndex:     0,
		pendingRelationsDB:     pendingDB,
	}

	// Initialize first time bucket
	now := time.Now()
	dm.firstActiveTimeBuckets[0] = NewTimeBucket(now)

	return dm, nil
}

// Close closes the dual manager and its resources
func (dm *DualManager) Close() error {
	return dm.pendingRelationsDB.Close()
}

// CheckTransaction is the main entry point for transaction analysis
func (dm *DualManager) CheckTransaction(tx *domain.MarkedTransaction) error {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	// Only process EOA-EOA transactions
	if tx.TxSyntax[0] != domain.EOAMark || tx.TxSyntax[1] != domain.EOAMark {
		return nil
	}

	return dm.handleAddress(tx)
}

// handleAddress processes transaction addresses according to their types
func (dm *DualManager) handleAddress(tx *domain.MarkedTransaction) error {
	fromAddr := tx.From
	toAddr := tx.To

	// 디버깅: 모든 트랜잭션의 케이스 분류 과정 로깅 (처음에는 항상 로깅)
	debugEnabled := true // 일단 모든 트랜잭션 디버깅
	if debugEnabled {
		fmt.Printf("🔀 DualManager: From=%s To=%s\n", 
			fromAddr.String()[:10]+"...", toAddr.String()[:10]+"...")
		fmt.Printf("   From_CEX=%t, To_CEX=%t, From_Deposit=%t, To_Deposit=%t\n",
			dm.groundKnowledge.IsCEXAddress(fromAddr),
			dm.groundKnowledge.IsCEXAddress(toAddr),
			dm.groundKnowledge.IsDepositAddress(fromAddr),
			dm.groundKnowledge.IsDepositAddress(toAddr))
	}

	// Case 1: from이 CEX인 경우 - to는 "txFromCexAddress"로 특수 처리
	if dm.groundKnowledge.IsCEXAddress(fromAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 1: CEX → Address (txFromCexAddress)\n")
		}
		return dm.handleExceptionalAddress(toAddr, "txFromCexAddress")
	}

	// Case 2: to가 CEX인 경우 - 새로운 입금주소 탐지
	if dm.groundKnowledge.IsCEXAddress(toAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 2: Deposit Detection (Address → CEX)\n")
		}
		return dm.handleDepositDetection(toAddr, fromAddr, tx)
	}

	// Case 3: from이 detectedDepositAddress인 경우 - to는 "txFromDepositAddress"로 특수 처리
	if dm.groundKnowledge.IsDepositAddress(fromAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 3: Detected Deposit → Address (txFromDepositAddress)\n")
		}
		return dm.handleExceptionalAddress(toAddr, "txFromDepositAddress")
	}

	// Case 4: to가 detectedDepositAddress인 경우 - from,to 듀얼을 그래프DB에 저장
	if dm.groundKnowledge.IsDepositAddress(toAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 4: Address → Detected Deposit (saveToGraphDB)\n")
		}
		return dm.saveToGraphDB(fromAddr, toAddr, tx)
	}

	// Case 5: 일반 트랜잭션 - DualManager 윈도우 버퍼에 추가
	if debugEnabled {
		fmt.Printf("   → Case 5: Regular Transaction (addToWindowBuffer)\n")
	}
	return dm.addToWindowBuffer(tx)
}

// handleExceptionalAddress processes exceptional addresses (to be implemented)
func (dm *DualManager) handleExceptionalAddress(address domain.Address, addressType string) error {
	//TODO: 추후 구현할 특수 주소 처리 로직
	return nil
}

// handleDepositDetection handles detection of new deposit addresses
func (dm *DualManager) handleDepositDetection(cexAddr, depositAddr domain.Address, tx *domain.MarkedTransaction) error {
	fmt.Printf("💰 handleDepositDetection: %s → CEX %s\n", 
		depositAddr.String()[:10]+"...", cexAddr.String()[:10]+"...")
		
	// 1. 새로운 입금주소를 detectedDepositAddress에 추가
	if err := dm.groundKnowledge.DetectNewDepositAddress(depositAddr, cexAddr); err != nil {
		fmt.Printf("   ❌ DetectNewDepositAddress failed: %v\n", err)
		return err
	}
	fmt.Printf("   ✅ DetectNewDepositAddress succeeded\n")

	// 2. DualManager의 pendingRelationsDB에서 depositAddr을 to로 하는 []from 값들 조회
	depositAddrStr := depositAddr.String()
	fromAddresses, err := dm.getPendingRelations(depositAddrStr)
	if err == nil && len(fromAddresses) > 0 {
		// 3. [](to,from) 쌍들을 그래프DB에 저장
		for _, fromAddrStr := range fromAddresses {
			fromAddr, err := parseAddressFromString(fromAddrStr)
			if err != nil {
				continue
			}

			if err := dm.saveConnectionToGraphDB(fromAddr, depositAddr, tx.TxID); err != nil {
				return err
			}
		}

		// 처리된 관계 제거
		if err := dm.deletePendingRelations(depositAddrStr); err != nil {
			return err
		}
	}

	return nil
}

// saveToGraphDB saves from-to dual relationship to graph database
func (dm *DualManager) saveToGraphDB(fromAddr, toAddr domain.Address, tx *domain.MarkedTransaction) error {
	return dm.saveConnectionToGraphDB(fromAddr, toAddr, tx.TxID)
}

// saveConnectionToGraphDB saves a connection between two EOAs to the graph database
func (dm *DualManager) saveConnectionToGraphDB(fromAddr, toAddr domain.Address, txID domain.TxId) error {
	// Create or update nodes
	nodeFrom := NewEOANode(fromAddr)
	nodeTo := NewEOANode(toAddr)

	if err := dm.graphRepo.SaveNode(nodeFrom); err != nil {
		return err
	}
	if err := dm.graphRepo.SaveNode(nodeTo); err != nil {
		return err
	}

	// Create or update edge
	_, err := dm.graphRepo.GetEdge(fromAddr, toAddr)
	if err != nil {
		// Create new edge
		edge := NewEOAEdge(fromAddr, toAddr, toAddr, txID, SameDepositUsage) // toAddr is depositAddr
		return dm.graphRepo.SaveEdge(edge)
	}

	// Update existing edge with new evidence
	return dm.graphRepo.UpdateEdgeEvidence(fromAddr, toAddr, txID, SameDepositUsage)
}

// addToWindowBuffer adds transaction to the sliding window buffer
func (dm *DualManager) addToWindowBuffer(tx *domain.MarkedTransaction) error {
	txTime := tx.BlockTime
	toAddrStr := tx.To.String()
	fromAddrStr := tx.From.String()

	// 1. Update firstActiveTimeBuckets (핵심 도메인 로직)
	if err := dm.updateFirstActiveTimeBuckets(toAddrStr, txTime); err != nil {
		return err
	}

	// 2. Add to pending relations in BadgerDB
	if err := dm.addToPendingRelations(toAddrStr, fromAddrStr); err != nil {
		return err
	}

	return nil
}

// updateFirstActiveTimeBuckets updates the sliding window buckets
// ! 중요한 도메인 로직: 윈도우 에이징 알고리즘
// ! - 한 번 윈도우에 들어온 to user의 값은 갱신하지 않음
// ! - 4개월 간 선택받지 못하면 자동으로 떨어져 나감
// ! - 에이징의 대상은 "to user"(입금 주소 탐지를 위한 핵심 로직)
func (dm *DualManager) updateFirstActiveTimeBuckets(toAddr string, txTime time.Time) error {
	// 1. 적절한 타임버킷 찾기 또는 생성
	bucketIndex := dm.findOrCreateTimeBucket(txTime)
	currentBucket := dm.firstActiveTimeBuckets[bucketIndex]

	// 2. 윈도우 전체에서 해당 to user가 이미 존재하는지 확인
	if dm.isToUserInWindow(toAddr) {
		return nil // 이미 윈도우에 있으면 갱신하지 않음 (핵심 도메인 로직)
	}

	// 3. 새로운 to user를 현재 버킷에 추가
	currentBucket.ToUsers[toAddr] = txTime

	// 4. 버킷 개수 관리 (22개가 되면 정리)
	if dm.countActiveBuckets() >= MaxTimeBuckets+1 {
		return dm.cleanupOldestBucket()
	}

	return nil
}

// findOrCreateTimeBucket finds appropriate bucket or creates new one
func (dm *DualManager) findOrCreateTimeBucket(txTime time.Time) int {
	// 현재 활성 버킷들 중에서 txTime이 속할 버킷 찾기
	for i, bucket := range dm.firstActiveTimeBuckets {
		if bucket == nil {
			continue
		}
		if txTime.After(bucket.StartTime) && txTime.Before(bucket.EndTime) {
			return i
		}
	}

	// 새로운 버킷 생성 필요
	return dm.createNewTimeBucket(txTime)
}

// createNewTimeBucket creates a new time bucket
func (dm *DualManager) createNewTimeBucket(txTime time.Time) int {
	// 빈 슬롯 찾기
	for i, bucket := range dm.firstActiveTimeBuckets {
		if bucket == nil {
			// 1주일 경계로 정렬된 시작 시간 계산
			weekStart := dm.calculateWeekStart(txTime)
			dm.firstActiveTimeBuckets[i] = NewTimeBucket(weekStart)
			return i
		}
	}

	// 모든 슬롯이 차있으면 순환적으로 사용
	dm.currentBucketIndex = (dm.currentBucketIndex + 1) % MaxTimeBuckets
	weekStart := dm.calculateWeekStart(txTime)
	dm.firstActiveTimeBuckets[dm.currentBucketIndex] = NewTimeBucket(weekStart)
	return dm.currentBucketIndex
}

// calculateWeekStart calculates the start of week for given time
func (dm *DualManager) calculateWeekStart(t time.Time) time.Time {
	// 주의 시작점을 일요일 00:00:00으로 계산
	year, month, day := t.Date()
	weekday := t.Weekday()
	daysToSubtract := int(weekday)
	weekStart := time.Date(year, month, day-daysToSubtract, 0, 0, 0, 0, t.Location())
	return weekStart
}

// isToUserInWindow checks if to user already exists in the entire window
func (dm *DualManager) isToUserInWindow(toAddr string) bool {
	for _, bucket := range dm.firstActiveTimeBuckets {
		if bucket == nil {
			continue
		}
		if _, exists := bucket.ToUsers[toAddr]; exists {
			return true
		}
	}
	return false
}

// countActiveBuckets counts non-nil buckets
func (dm *DualManager) countActiveBuckets() int {
	count := 0
	for _, bucket := range dm.firstActiveTimeBuckets {
		if bucket != nil {
			count++
		}
	}
	return count
}

// cleanupOldestBucket removes oldest bucket and its associated kvDB entries
func (dm *DualManager) cleanupOldestBucket() error {
	// 가장 오래된 버킷 찾기
	var oldestBucket *TimeBucket
	var oldestIndex int
	var oldestTime time.Time = time.Now()

	for i, bucket := range dm.firstActiveTimeBuckets {
		if bucket != nil && bucket.StartTime.Before(oldestTime) {
			oldestTime = bucket.StartTime
			oldestBucket = bucket
			oldestIndex = i
		}
	}

	if oldestBucket == nil {
		return nil
	}

	// 해당 버킷의 to users들을 키로 하는 pendingRelationsDB 항목들 제거
	for toAddr := range oldestBucket.ToUsers {
		if err := dm.deletePendingRelations(toAddr); err != nil {
			// Log error but continue cleanup
			continue
		}
	}

	// 버킷 제거
	dm.firstActiveTimeBuckets[oldestIndex] = nil

	return nil
}

// BadgerDB helper methods for pending relations management

// getPendingRelations retrieves the list of from addresses for a given to address
func (dm *DualManager) getPendingRelations(toAddr string) ([]string, error) {
	var fromAddresses []string
	
	err := dm.pendingRelationsDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(toAddr))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &fromAddresses)
		})
	})

	if err == badger.ErrKeyNotFound {
		return []string{}, nil // Return empty slice if not found
	}
	
	return fromAddresses, err
}

// addToPendingRelations adds a from address to the list of a to address
func (dm *DualManager) addToPendingRelations(toAddr, fromAddr string) error {
	return dm.pendingRelationsDB.Update(func(txn *badger.Txn) error {
		// Get existing relations
		var fromAddresses []string
		item, err := txn.Get([]byte(toAddr))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		
		if err == nil {
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &fromAddresses)
			})
			if err != nil {
				return err
			}
		}

		// Check if from address already exists
		for _, existing := range fromAddresses {
			if existing == fromAddr {
				return nil // Already exists
			}
		}

		// Add new from address
		fromAddresses = append(fromAddresses, fromAddr)

		// Save updated list
		data, err := json.Marshal(fromAddresses)
		if err != nil {
			return err
		}

		return txn.Set([]byte(toAddr), data)
	})
}

// deletePendingRelations removes all pending relations for a to address
func (dm *DualManager) deletePendingRelations(toAddr string) error {
	return dm.pendingRelationsDB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(toAddr))
	})
}

// countPendingRelations counts the total number of pending relations
func (dm *DualManager) countPendingRelations() int {
	count := 0
	dm.pendingRelationsDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count
}

// GetWindowStats returns statistics about the sliding window
func (dm *DualManager) GetWindowStats() map[string]interface{} {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	activeBuckets := dm.countActiveBuckets()
	totalToUsers := 0
	totalPendingRelations := dm.countPendingRelations()

	for _, bucket := range dm.firstActiveTimeBuckets {
		if bucket != nil {
			totalToUsers += len(bucket.ToUsers)
		}
	}

	return map[string]interface{}{
		"active_buckets":       activeBuckets,
		"total_to_users":       totalToUsers,
		"pending_relations":    totalPendingRelations,
		"window_size_hours":    WindowSize.Hours(),
		"slide_interval_hours": SlideInterval.Hours(),
		"max_buckets":          MaxTimeBuckets,
	}
}
