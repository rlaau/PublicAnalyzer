package app

import (
	"fmt"
	"sync"

	localdomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/infra"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

const (
	WindowSize      = 4 * 30 * 24 * chaintimer.Hour // 4개월 윈도우
	SlideInterval   = 7 * 24 * chaintimer.Hour      // 1주일 슬라이드
	TriggerInterval = 7 * 24 * chaintimer.Hour      // 1주일 트리거
	MaxTimeBuckets  = 21                            // 4개월 / 1주일 = 21개 버킷
)

// DualManager manages EOA relationships through sliding window analysis
type DualManager struct {
	infra infra.DualManagerInfra

	// Sliding window management (순환 큐 구조)
	firstActiveTimeBuckets []*TimeBucket // 21개 타임버킷으로 4개월 윈도우 관리
	frontIndex             int           // 가장 오래된 버킷 인덱스 (제거 대상)
	rearIndex              int           // 가장 최신 버킷 인덱스 (추가 위치)
	bucketCount            int           // 현재 버킷 개수 (0~21)

	// Synchronization (최적화된 뮤텍스)
	mutex        sync.RWMutex // 전체 구조체 보호용 (구조 변경 등)
	bucketsMutex sync.RWMutex // TimeBucket 관련 작업 전용 (BadgerDB는 자체 동시성 보장)
}

// TimeBucket represents a time bucket in the sliding window
type TimeBucket struct {
	StartTime chaintimer.ChainTime
	EndTime   chaintimer.ChainTime
	ToUsers   map[domain.Address]chaintimer.ChainTime // to_address -> first_active_time
}

// NewTimeBucket creates a new time bucket
func NewTimeBucket(startTime chaintimer.ChainTime) *TimeBucket {
	return &TimeBucket{
		StartTime: startTime,
		EndTime:   startTime.Add(SlideInterval),
		ToUsers:   make(map[domain.Address]chaintimer.ChainTime),
	}
}

// NewDualManager creates a new dual manager instance
func NewDualManager(managerInfra infra.DualManagerInfra) (*DualManager, error) {

	dm := &DualManager{
		infra:                  managerInfra,
		firstActiveTimeBuckets: make([]*TimeBucket, MaxTimeBuckets),
		frontIndex:             0, // 첫 번째 버킷이 들어갈 위치
		rearIndex:              0, // 첫 번째 버킷이 들어갈 위치
		bucketCount:            0, // 초기 버킷 개수
	}

	return dm, nil
}

// Close closes the dual manager and its resources
func (dm *DualManager) Close() error {
	return dm.infra.PendingRelationRepo.Close()
}

// CheckTransaction is the main entry point for transaction analysis
func (dm *DualManager) CheckTransaction(tx *domain.MarkedTransaction) (*domain.MarkedTransaction, error) {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	// Only process EOA-EOA transactions
	//todo 근데 이거 중복 체킹이긴 함. 프로덕션 후 문제 없으면 충분히 제거 가능
	//todo ProcessSingle에서 미리 검사함. 애초에 카프카 큐에서 분리 시 신뢰도 가능하고
	if tx.TxSyntax[0] != domain.EOAMark || tx.TxSyntax[1] != domain.EOAMark {
		return nil, fp.ErrorSkipStep
	}
	return tx, nil
}

// HandleAddress processes transaction addresses according to their types
func (dm *DualManager) HandleAddress(tx *domain.MarkedTransaction) (*domain.MarkedTransaction, error) {
	fromAddr := tx.From
	toAddr := tx.To

	// 디버깅: 모든 트랜잭션의 케이스 분류 과정 로깅 (처음에는 항상 로깅)
	debugEnabled := false //성능 이슈로 디버깅 취소//true // 일단 모든 트랜잭션 디버깅
	if debugEnabled {
		fmt.Printf("🔀 DualManager: From=%s To=%s\n",
			fromAddr.String()[:10]+"...", toAddr.String()[:10]+"...")
		fmt.Printf("   From_CEX=%t, To_CEX=%t, From_Deposit=%t, To_Deposit=%t\n",
			dm.infra.GroundKnowledge.IsCEXAddress(fromAddr),
			dm.infra.GroundKnowledge.IsCEXAddress(toAddr),
			dm.infra.GroundKnowledge.IsDepositAddress(fromAddr),
			dm.infra.GroundKnowledge.IsDepositAddress(toAddr))
	}

	// Case 1: from이 CEX인 경우 - to는 "txFromCexAddress"로 특수 처리
	if dm.infra.GroundKnowledge.IsCEXAddress(fromAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 1: CEX → Address (txFromCexAddress)\n")
		}
		err := dm.handleExceptionalAddress(toAddr, "txFromCexAddress")
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 2: to가 CEX인 경우 - 새로운 입금주소 탐지
	if dm.infra.GroundKnowledge.IsCEXAddress(toAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 2: Deposit Detection (Address → CEX)\n")
		}
		err := dm.handleDepositDetection(toAddr, fromAddr, tx)
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 3: from이 detectedDepositAddress인 경우 - to는 "txFromDepositAddress"로 특수 처리
	if dm.infra.GroundKnowledge.IsDepositAddress(fromAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 3: Detected Deposit → Address (txFromDepositAddress)\n")
		}
		err := dm.handleExceptionalAddress(toAddr, "txFromDepositAddress")
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 4: to가 detectedDepositAddress인 경우 - from,to 듀얼을 그래프DB에 저장
	if dm.infra.GroundKnowledge.IsDepositAddress(toAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 4: Address → Detected Deposit (saveToGraphDB)\n")
		}
		err := dm.saveToGraphDB(fromAddr, toAddr, tx)
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 5: 일반 트랜잭션 - DualManager 윈도우 버퍼에 추가
	if debugEnabled {
		fmt.Printf("   → Case 5: Regular Transaction (addToWindowBuffer)\n")
	}
	return tx, nil
}

// handleExceptionalAddress processes exceptional addresses (to be implemented)
func (dm *DualManager) handleExceptionalAddress(_ domain.Address, _ string) error {
	//TODO: 추후 구현할 특수 주소 처리 로직
	return nil
}

// handleDepositDetection handles detection of new deposit addresses
func (dm *DualManager) handleDepositDetection(cexAddr, depositAddr domain.Address, tx *domain.MarkedTransaction) error {
	//fmt.Printf("💰 handleDepositDetection: %s → CEX %s\n",	depositAddr.String()[:10]+"...", cexAddr.String()[:10]+"...")

	// 1. 새로운 입금주소를 detectedDepositAddress에 추가
	if err := dm.infra.GroundKnowledge.DetectNewDepositAddress(depositAddr, cexAddr); err != nil {
		fmt.Printf("   ❌ DetectNewDepositAddress failed: %v\n", err)
		return err
	}
	//fmt.Printf("   ✅ DetectNewDepositAddress succeeded\n")

	// 2. DualManager의 pendingRelationsDB에서 depositAddr을 to로 하는 []from 값들 조회
	// TODO pendingRelations에서 관리하는 타입을 to-> []fromInfo로 변경 요구
	// TODO fromInfo는 [txTD,address]로 저장하기
	fromAddresses, err := dm.infra.PendingRelationRepo.GetPendingRelations(depositAddr)
	if err == nil && len(fromAddresses) > 0 {
		// 3. [](to,from) 쌍들을 그래프DB에 저장
		for _, fromAddr := range fromAddresses {

			//TODO 이 구문 수정 필요. 여기의 txID는 cex,deposit의 관계지, deposit->eoa의 txId가 아님
			//TODO그러니까, 여기서 "가장 중요한 값인" "depsot,eoa"의 관계 자체는 잘 저장이 됨. 근데 얘내를 증명하느 "TxID"가 deposit-cex의 것임.
			//TODO 그러니까, "애초부타 pendingDB가 txID를 함꼐 저장하게 해서" fromInfo 방식 타입으로 불러온 후 제대로된 txID저장 필요
			//TODO 추후 pendingRelationsDB에서 fromInfo를 [txTD, address]로 저장하게 한 후, 그거스이 txID쓰기
			if err := dm.saveConnectionToGraphDB(fromAddr, depositAddr, tx.TxID); err != nil {
				return err
			}
		}

		// 처리된 관계 제거
		//TODO pendingRelations와 windowBucket은 항상 "holding한 toUser가 동일"해야 하므로, 펜딩에서 제거 시 윈도우에서도 제거 필요
		if err := dm.infra.PendingRelationRepo.DeletePendingRelations(depositAddr); err != nil {
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
	nodeFrom := localdomain.NewEOANode(fromAddr)
	nodeTo := localdomain.NewEOANode(toAddr)

	if err := dm.infra.GraphRepo.SaveNode(nodeFrom); err != nil {
		return err
	}
	if err := dm.infra.GraphRepo.SaveNode(nodeTo); err != nil {
		return err
	}

	// Create or update edge
	_, err := dm.infra.GraphRepo.GetEdge(fromAddr, toAddr)
	if err != nil {
		// Create new edge
		edge := localdomain.NewEOAEdge(fromAddr, toAddr, toAddr, txID, localdomain.SameDepositUsage) // toAddr is depositAddr
		return dm.infra.GraphRepo.SaveEdge(edge)
	}

	// Update existing edge with new evidence
	return dm.infra.GraphRepo.UpdateEdgeEvidence(fromAddr, toAddr, txID, localdomain.SameDepositUsage)
}

// AddToWindowBuffer adds transaction to the sliding window buffer
func (dm *DualManager) AddToWindowBuffer(tx *domain.MarkedTransaction) (*domain.MarkedTransaction, error) {
	txTime := tx.BlockTime
	toAddr := tx.To
	fromAddr := tx.From

	// 디버깅: 매 50 트랜잭션마다 시간 로깅 (10분×50=8.3시간마다)
	static_counter++
	if static_counter%50 == 0 || static_counter <= 20 {
		fmt.Printf("⏰ TX #%d time: %s (1주=1008분=약17tx, 21개 버킷=357tx에서 순환)\n",
			static_counter, txTime.Format("2006-01-02 15:04:05"))
	}

	// 1. Update firstActiveTimeBuckets (핵심 도메인 로직)
	if err := dm.updateFirstActiveTimeBuckets(toAddr, txTime); err != nil {
		return nil, err
	}

	// 2. Add to pending relations in BadgerDB
	if err := dm.infra.PendingRelationRepo.AddToPendingRelations(toAddr, fromAddr); err != nil {
		return nil, err
	}

	return nil, fp.ErrorSkipStep
}

// 디버깅용 전역 카운터
var static_counter int64

// updateFirstActiveTimeBuckets updates the sliding window buckets with circular queue logic
// ! 중요한 도메인 로직: 윈도우 에이징 알고리즘
// ! - 한 번 윈도우에 들어온 to user의 값은 갱신하지 않음
// ! - 4개월 간 선택받지 못하면 자동으로 떨어져 나감
// ! - 에이징의 대상은 "to user"(입금 주소 탐지를 위한 핵심 로직)
func (dm *DualManager) updateFirstActiveTimeBuckets(toAddr domain.Address, txTime chaintimer.ChainTime) error {
	// 1. 적절한 타임버킷 찾기 또는 생성 (쓰기 락 필요)
	dm.bucketsMutex.Lock()
	bucketIndex := dm.findOrCreateTimeBucket(txTime)
	currentBucket := dm.firstActiveTimeBuckets[bucketIndex]
	dm.bucketsMutex.Unlock()

	// 2. 윈도우 전체에서 해당 to user가 이미 존재하는지 확인 (읽기 락)
	dm.bucketsMutex.RLock()
	exists := dm.isToUserInWindow(toAddr)
	dm.bucketsMutex.RUnlock()

	if exists {
		return nil // 이미 윈도우에 있으면 갱신하지 않음 (핵심 도메인 로직)
	}

	// 3. 새로운 to user를 현재 버킷에 추가 (쓰기 락)
	dm.bucketsMutex.Lock()
	currentBucket.ToUsers[toAddr] = txTime
	dm.bucketsMutex.Unlock()

	return nil
}

// findOrCreateTimeBucket finds appropriate bucket or creates new one with circular queue logic
func (dm *DualManager) findOrCreateTimeBucket(txTime chaintimer.ChainTime) int {
	// 첫 번째 트랜잭션인 경우 - 첫 트랜잭션 시간을 기준으로 첫 버킷 생성
	if dm.bucketCount == 0 {
		weekStart := dm.calculateWeekStart(txTime)
		dm.firstActiveTimeBuckets[0] = NewTimeBucket(weekStart)

		// 순환 큐 초기화
		dm.frontIndex = 0  // 첫 번째 버킷이 가장 오래된 버킷
		dm.rearIndex = 0   // 첫 번째 버킷이 가장 최신 버킷
		dm.bucketCount = 1 // 버킷 개수 증가

		fmt.Printf("🪣 First bucket created at index 0: %s - %s (front:%d, rear:%d, count:%d)\n",
			weekStart.Format("2006-01-02 15:04:05"),
			weekStart.Add(SlideInterval).Format("2006-01-02 15:04:05"),
			dm.frontIndex, dm.rearIndex, dm.bucketCount)
		return 0
	}

	// 현재 활성 버킷들 중에서 txTime이 속할 버킷 찾기
	//TODO 이 로직이 좀. 바보같은데? 왜 first버킷부터 그런 식으로 찾는거지??
	//TODO 미리 인덱스-버킷을 만드는건 좋은데,퍼스트부터 할 필요는 전혀 없음. 최신부터 하면 1회만에 찾는데.
	//TODO 거꾸로 하면 20회나 더해야함. 진짜 굳이 싶은 로직임.
	for i := 0; i < dm.bucketCount; i++ {
		bucketIndex := (dm.rearIndex - i + MaxTimeBuckets) % MaxTimeBuckets
		bucket := dm.firstActiveTimeBuckets[bucketIndex]

		// 반닫힌 구간 [StartTime, EndTime): StartTime <= txTime < EndTime
		if !txTime.Before(bucket.StartTime) && txTime.Before(bucket.EndTime) {
			return bucketIndex
		}
	}

	// 디버깅: 새 버킷이 필요한 경우 현재 상황 로그
	//이게 5인건 전혀 문제가 없음. 차피 맨 밑에서 add하므로, 여긴 걍 로그임
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
func (dm *DualManager) addNewTimeBucket(txTime chaintimer.ChainTime) int {
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

		// *기존 버킷의 pendingRelations 정리
		// *타임 버킷과 팬딩 DB가 홀딩하는 유저는 항상 동기화됨.
		pendingBefore := dm.infra.PendingRelationRepo.CountPendingRelations()
		toUsersCount := len(oldBucket.ToUsers)
		deletedRelations := 0
		for toAddr := range oldBucket.ToUsers {
			if err := dm.infra.PendingRelationRepo.DeletePendingRelations(toAddr); err != nil {
				fmt.Printf("   ⚠️ Failed to delete pending relations for %s: %v\n", toAddr.String()[:10]+"...", err)
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

		pendingAfter := dm.infra.PendingRelationRepo.CountPendingRelations()

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
func (dm *DualManager) calculateWeekStart(t chaintimer.ChainTime) chaintimer.ChainTime {
	// 주의 시작점을 일요일 00:00:00으로 계산
	year, month, day := t.Date()
	weekday := t.Weekday()
	daysToSubtract := int(weekday)
	weekStart := chaintimer.ChainDate(year, month, day-daysToSubtract, 0, 0, 0, 0, t.Location())
	return weekStart
}

// isToUserInWindow checks if to user already exists in the entire window
// ! 성능 최적화: 최신 버킷(rearIndex)부터 역순으로 검색 - 캐시 효과 극대화
// ! 도메인 로직: 최근에 등장한 유저가 다시 등장할 확률이 높음
func (dm *DualManager) isToUserInWindow(toAddr domain.Address) bool {
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

// GetWindowStats returns statistics about the sliding window
func (dm *DualManager) GetWindowStats() map[string]any {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	activeBuckets := dm.countActiveBuckets()
	totalToUsers := 0
	totalPendingRelations := dm.infra.PendingRelationRepo.CountPendingRelations()

	for _, bucket := range dm.firstActiveTimeBuckets {
		if bucket != nil {
			totalToUsers += len(bucket.ToUsers)
		}
	}

	return map[string]any{
		"active_buckets":       activeBuckets,
		"total_to_users":       totalToUsers,
		"pending_relations":    totalPendingRelations,
		"window_size_hours":    WindowSize.Hours(),
		"slide_interval_hours": SlideInterval.Hours(),
		"max_buckets":          MaxTimeBuckets,
	}
}
