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

	// Sliding window management (순환 큐 구조)
	firstActiveTimeBuckets []*TimeBucket // 21개 타임버킷으로 4개월 윈도우 관리
	frontIndex             int           // 가장 오래된 버킷 인덱스 (제거 대상)
	rearIndex              int           // 가장 최신 버킷 인덱스 (추가 위치)
	bucketCount            int           // 현재 버킷 개수 (0~21)

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
		frontIndex:             0,  // 첫 번째 버킷이 들어갈 위치
		rearIndex:              0,  // 첫 번째 버킷이 들어갈 위치
		bucketCount:            0,  // 초기 버킷 개수
		pendingRelationsDB:     pendingDB,
	}

	//TODO 첫 번째 트랜잭션의 시간을 기준으로 동적으로 첫 버킷을 생성하도록 변경
	//TODO 이렇게 하면 txGenerator의 시작 시간과 무관하게 첫 트랜잭션부터 1주일씩 버킷 생성됨
	// Initialize first time bucket - 첫 트랜잭션이 올 때까지 대기
	// now := time.Now()
	// dm.firstActiveTimeBuckets[0] = NewTimeBucket(now)

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
	//todo 근데 이거 중복 체킹이긴 함. 프로덕션 후 문제 없으면 충분히 제거 가능
	//todo ProcessSingle에서 미리 검사함. 애초에 카프카 큐에서 분리 시 신뢰도 가능하고
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
	debugEnabled := false //성능 이슈로 디버깅 취소//true // 일단 모든 트랜잭션 디버깅
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
// ! 성능 관련 로직이 (케스케이딩 버킷) 수정이 필요한 함수
// TODO 성능 관련 로직 수정 필요!!
func (dm *DualManager) handleDepositDetection(cexAddr, depositAddr domain.Address, tx *domain.MarkedTransaction) error {
	//fmt.Printf("💰 handleDepositDetection: %s → CEX %s\n",	depositAddr.String()[:10]+"...", cexAddr.String()[:10]+"...")

	// 1. 새로운 입금주소를 detectedDepositAddress에 추가
	if err := dm.groundKnowledge.DetectNewDepositAddress(depositAddr, cexAddr); err != nil {
		fmt.Printf("   ❌ DetectNewDepositAddress failed: %v\n", err)
		return err
	}
	//fmt.Printf("   ✅ DetectNewDepositAddress succeeded\n")

	// 2. DualManager의 pendingRelationsDB에서 depositAddr을 to로 하는 []from 값들 조회
	// TODO pendingRelations에서 관리하는 타입을 to-> []fromInfo로 변경 요구
	// TODO fromInfo는 [txTD,address]로 저장하기
	depositAddrStr := depositAddr.String()
	fromAddresses, err := dm.getPendingRelations(depositAddrStr)
	if err == nil && len(fromAddresses) > 0 {
		// 3. [](to,from) 쌍들을 그래프DB에 저장
		for _, fromAddrStr := range fromAddresses {
			fromAddr, err := parseAddressFromString(fromAddrStr)
			if err != nil {
				continue
			}

			//TODO 이 구문 수정 필요. 여기의 txID는 cex,deposit의 관계지, deposit->eoa의 txId가 아님
			//TODO 추후 pendingRelationsDB에서 fromInfo를 [txTD, address]로 저장하게 한 후, 그거스이 txID쓰기
			if err := dm.saveConnectionToGraphDB(fromAddr, depositAddr, tx.TxID); err != nil {
				return err
			}
		}

		// 처리된 관계 제거
		//TODO pendingRelations와 windowBucket은 항상 "holind한 toUser가 동일"해야 하므로, 펜딩에서 제거 시 윈도우에서도 제거 필요
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

	// 디버깅: 매 50 트랜잭션마다 시간 로깅 (10분×50=8.3시간마다)
	static_counter++
	if static_counter%50 == 0 || static_counter <= 20 {
		fmt.Printf("⏰ TX #%d time: %s (1주=1008분=약17tx, 21개 버킷=357tx에서 순환)\n",
			static_counter, txTime.Format("2006-01-02 15:04:05"))
	}

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

// 디버깅용 전역 카운터
var static_counter int64

// updateFirstActiveTimeBuckets updates the sliding window buckets with circular queue logic
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

	return nil
}

// findOrCreateTimeBucket finds appropriate bucket or creates new one with circular queue logic
func (dm *DualManager) findOrCreateTimeBucket(txTime time.Time) int {
	// 첫 번째 트랜잭션인 경우 - 첫 트랜잭션 시간을 기준으로 첫 버킷 생성
	if dm.bucketCount == 0 {
		weekStart := dm.calculateWeekStart(txTime)
		dm.firstActiveTimeBuckets[0] = NewTimeBucket(weekStart)
		
		// 순환 큐 초기화
		dm.frontIndex = 0    // 첫 번째 버킷이 가장 오래된 버킷
		dm.rearIndex = 0     // 첫 번째 버킷이 가장 최신 버킷
		dm.bucketCount = 1   // 버킷 개수 증가
		
		fmt.Printf("🪣 First bucket created at index 0: %s - %s (front:%d, rear:%d, count:%d)\n",
			weekStart.Format("2006-01-02 15:04:05"),
			weekStart.Add(SlideInterval).Format("2006-01-02 15:04:05"),
			dm.frontIndex, dm.rearIndex, dm.bucketCount)
		return 0
	}

	// 현재 활성 버킷들 중에서 txTime이 속할 버킷 찾기
	for i := 0; i < dm.bucketCount; i++ {
		bucketIndex := (dm.frontIndex + i) % MaxTimeBuckets
		bucket := dm.firstActiveTimeBuckets[bucketIndex]
		
		// 반닫힌 구간 [StartTime, EndTime): StartTime <= txTime < EndTime
		if !txTime.Before(bucket.StartTime) && txTime.Before(bucket.EndTime) {
			return bucketIndex
		}
	}

	// 디버깅: 새 버킷이 필요한 경우 현재 상황 로그
	if dm.bucketCount < 5 { // 처음 몇 개만 로깅
		fmt.Printf("🔍 No matching bucket found for txTime: %s (active buckets: %d)\n",
			txTime.Format("2006-01-02 15:04:05"), dm.bucketCount)
		for i := 0; i < dm.bucketCount; i++ {
			bucketIndex := (dm.frontIndex + i) % MaxTimeBuckets
			bucket := dm.firstActiveTimeBuckets[bucketIndex]
			fmt.Printf("   Bucket[%d]: %s - %s\n", bucketIndex,
				bucket.StartTime.Format("2006-01-02 15:04:05"),
				bucket.EndTime.Format("2006-01-02 15:04:05"))
		}
	}

	// 새로운 버킷 생성 필요
	return dm.addNewTimeBucket(txTime)
}

// addNewTimeBucket adds a new time bucket using proper circular queue logic
func (dm *DualManager) addNewTimeBucket(txTime time.Time) int {
	weekStart := dm.calculateWeekStart(txTime)
	
	if dm.bucketCount < MaxTimeBuckets {
		// 공간이 남아있는 경우: rear 다음 위치에 새 버킷 추가
		newRearIndex := (dm.rearIndex + 1) % MaxTimeBuckets
		dm.firstActiveTimeBuckets[newRearIndex] = NewTimeBucket(weekStart)
		dm.rearIndex = newRearIndex
		dm.bucketCount++
		
		fmt.Printf("🪣 New bucket added at index %d: %s - %s (front:%d, rear:%d, count:%d)\n",
			newRearIndex,
			weekStart.Format("2006-01-02 15:04:05"),
			weekStart.Add(SlideInterval).Format("2006-01-02 15:04:05"),
			dm.frontIndex, dm.rearIndex, dm.bucketCount)
		
		return newRearIndex
	} else {
		// 공간이 꽉 찬 경우 (21개): front 버킷을 제거하고 그 자리에 새 버킷 추가
		oldBucket := dm.firstActiveTimeBuckets[dm.frontIndex]
		
		// 기존 버킷의 pendingRelations 정리
		pendingBefore := dm.countPendingRelations()
		toUsersCount := len(oldBucket.ToUsers)
		deletedRelations := 0
		
		for toAddr := range oldBucket.ToUsers {
			if err := dm.deletePendingRelations(toAddr); err != nil {
				fmt.Printf("   ⚠️ Failed to delete pending relations for %s: %v\n", toAddr[:10]+"...", err)
				continue
			}
			deletedRelations++
		}
		
		// front 위치에 새 버킷 생성 (덮어쓰기)
		newBucketIndex := dm.frontIndex
		dm.firstActiveTimeBuckets[newBucketIndex] = NewTimeBucket(weekStart)
		
		// front를 다음 위치로 이동, rear는 새로 생성된 버킷으로 설정
		dm.frontIndex = (dm.frontIndex + 1) % MaxTimeBuckets
		dm.rearIndex = newBucketIndex
		// bucketCount는 21 고정
		
		pendingAfter := dm.countPendingRelations()
		
		fmt.Printf("🪣 BUCKET ROTATION[%d]: %s-%s → %s-%s (front:%d, rear:%d, count:%d)\n",
			newBucketIndex,
			oldBucket.StartTime.Format("2006-01-02 15:04"),
			oldBucket.EndTime.Format("2006-01-02 15:04"),
			weekStart.Format("2006-01-02 15:04"),
			weekStart.Add(SlideInterval).Format("2006-01-02 15:04"),
			dm.frontIndex, dm.rearIndex, dm.bucketCount)
		fmt.Printf("   🗑️  PendingRelations cleanup: %d→%d (deleted %d/%d toUsers)\n",
			pendingBefore, pendingAfter, deletedRelations, toUsersCount)
		
		return newBucketIndex
	}
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
// ! 성능 최적화: 최신 버킷(rearIndex)부터 역순으로 검색 - 캐시 효과 극대화
// ! 도메인 로직: 최근에 등장한 유저가 다시 등장할 확률이 높음
func (dm *DualManager) isToUserInWindow(toAddr string) bool {
	if dm.bucketCount == 0 {
		return false
	}

	// 최신 버킷(rearIndex)부터 역순으로 검색
	for i := 0; i < dm.bucketCount; i++ {
		// 순환 큐에서 최신부터 역순 인덱스 계산
		bucketIndex := (dm.rearIndex - i + MaxTimeBuckets) % MaxTimeBuckets
		bucket := dm.firstActiveTimeBuckets[bucketIndex]
		
		if bucket != nil {
			if _, exists := bucket.ToUsers[toAddr]; exists {
				// 성능 로깅 (첫 10개만)
				if static_counter <= 10 {
					fmt.Printf("   🔍 Cache hit: User found in bucket[%d] (search depth: %d)\n", bucketIndex, i+1)
				}
				return true
			}
		}
	}
	return false
}

// countActiveBuckets returns cached active bucket count (O(1) 성능)
func (dm *DualManager) countActiveBuckets() int {
	return dm.bucketCount
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
// TODO 현재는 순회를 통해서 카운트함. 성능 개선 필요
// ! 주요 성능 개선 필요 구간임!
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
