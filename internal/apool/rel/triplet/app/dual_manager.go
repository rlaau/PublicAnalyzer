package app

import (
	"fmt"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/roperepo"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

// DualManager manages EOA relationships through sliding window analysis
type DualManager struct {
	infra *infra.DualManagerInfra

	relPool iface.RelPort

	// Synchronization (최적화된 뮤텍스)
	mutex sync.RWMutex // 전체 구조체 보호용 (구조 변경 등)
}

// NewDualManager creates a new dual manager instance
func NewDualManager(managerInfra *infra.DualManagerInfra, pool iface.RelPort) (*DualManager, error) {
	dm := &DualManager{
		infra:   managerInfra,
		relPool: pool,
	}
	//* 타임 버킷이 버려질 땐 펜딩 릴레이션도 버려지게 설정
	// 타임버킷 로테이트 시, 해당 버킷의 to-user 펜딩 릴레이션 정리
	managerInfra.TimeBucketManager.OnRotate = func(oldBucket *infra.TimeBucket) {
		// 1) 키 스냅샷을 먼저 뜬다 (동시 수정/삭제로부터 보호)
		keys := make([]domain.Address, 0, len(oldBucket.ToUsers))
		for to := range oldBucket.ToUsers {
			keys = append(keys, to)
		}

		deleted, err := dm.infra.PendingRelationRepo.DeletePendingRelationsBatch(keys)
		if err != nil {
			fmt.Printf("   ⚠️ Failed to delete pending relations for %s: %v\n",
				keys[0].String()[:10]+"...", err)
		}

		// 3) 출력은 스냅샷 길이 기준 (맵이 비워졌더라도 정확한 개수 출력)
		fmt.Printf("%d개의 펜딩 to user를 두 저장소에서 제거함\n", deleted)
	}

	return dm, nil
}

// Close closes the dual manager and its resources
func (dm *DualManager) Close() error {
	if err := dm.infra.PendingRelationRepo.Close(); err != nil {
		return err
	}
	return dm.infra.TimeBucketManager.Close()
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

func (dm *DualManager) IsCEXAddress(addr domain.Address) bool {
	return dm.relPool.GetApooPort().GetNodPort().GetEoPort().IsCex(addr)
}

func (dm *DualManager) IsDepositAddress(addr domain.Address) bool {
	isDeposit, _ := dm.relPool.GetApooPort().GetNodPort().GetEoPort().IsDepositAddress(addr)
	return isDeposit
}

// HandleAddress processes transaction addresses according to their types
func (dm *DualManager) HandleAddress(tx *domain.MarkedTransaction) (*domain.MarkedTransaction, error) {
	fromAddr := tx.From
	toAddr := tx.To

	// 디버깅: 모든 트랜잭션의 케이스 분류 과정 로깅 (처음에는 항상 로깅)
	debugEnabled := false //성능 이슈로 디버깅 취소//true // 일단 모든 트랜잭션 디버깅
	allDbg := false
	if allDbg {
		fmt.Printf("🔀 DualManager: From=%s To=%s\n",
			fromAddr.String()[:10]+"...", toAddr.String()[:10]+"...")
		fmt.Printf("   From_CEX=%t, To_CEX=%t, From_Deposit=%t, To_Deposit=%t\n",
			dm.IsCEXAddress(fromAddr),
			dm.IsCEXAddress(toAddr),
			dm.IsDepositAddress(fromAddr),
			dm.IsDepositAddress(toAddr))
	}

	// Case 1: from이 CEX인 경우 - to는 "txFromCexAddress"로 특수 처리
	if dm.IsCEXAddress(fromAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 1: CEX → Address (txFromCexAddress)\n")
		}
		err := dm.handleTxFromCex(fromAddr, tx.To, tx)
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 2: to가 CEX인 경우 - 새로운 입금주소 탐지
	if dm.IsCEXAddress(toAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 2: Deposit Detection (Address → CEX)\n")
		}
		err := dm.handleDepositDetection(toAddr, fromAddr, tx, tx.BlockTime)
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 3: from이 detectedDepositAddress인 경우 - to는 "txFromDepositAddress"로 특수 처리
	if dm.IsDepositAddress(fromAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 3: Detected Deposit → Address (txFromDepositAddress)\n")
		}
		err := dm.handleTxFromDepositWithNotCex(fromAddr, tx.To, tx)
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 4: to가 detectedDepositAddress인 경우 - from,to 듀얼을 그래프DB에 저장
	if dm.IsDepositAddress(toAddr) {
		if debugEnabled {
			fmt.Printf("   → Case 4: Address → Detected Deposit (saveToGraphDB)\n")
		}
		if dm.IsCEXAddress(fromAddr) {
			dm.relPool.GetApooPort().GetNodPort().GetEoPort().UpdateDepositTxCount(toAddr, 1)
		}
		fromScala := infra.FromScala{
			FromAddress: fromAddr,
			LastTxId:    tx.TxID,
			LastTime:    tx.BlockTime,
		}
		err := dm.saveDualToGraphDB(fromScala, toAddr)
		return nil, fp.HandleErrOrNilToSkipStep(err)
	}

	// Case 5: 일반 트랜잭션 - DualManager 윈도우 버퍼에 추가
	if debugEnabled {
		fmt.Printf("   → Case 5: Regular Transaction (addToWindowBuffer)\n")
	}
	return tx, nil
}

func (dm *DualManager) handleTxFromCex(cex domain.Address, other domain.Address, tx *domain.MarkedTransaction) error {
	author := roperepo.AuthorSystem
	trait := roperepo.TraitCexToOther

	cexAndRule := ropedomain.AddressAndRule{
		Address: cex,
		Rule:    roperepo.RuleCex,
	}
	otherAndRule := ropedomain.AddressAndRule{
		Address: other,
		Rule:    roperepo.RuleRecievingFromCex,
	}
	txScala := ropedomain.TxScala{
		TxId:     tx.TxID,
		Time:     tx.BlockTime,
		ScoreInc: 1,
	}
	traitEvent := ropedomain.NewTraitEvent(
		author,
		trait,
		cexAndRule,
		otherAndRule,
		txScala,
	)
	return dm.relPool.RopeDB().PushTraitEvent(traitEvent)
}

func (dm *DualManager) handleTxFromDepositWithNotCex(deposit domain.Address, other domain.Address, tx *domain.MarkedTransaction) error {
	author := roperepo.AuthorSystem
	trait := roperepo.TraitDepositToOther
	depositAndRule := ropedomain.AddressAndRule{
		Address: deposit,
		Rule:    roperepo.RuleDeposit,
	}
	otherAndRule := ropedomain.AddressAndRule{
		Address: other,
		Rule:    roperepo.RuleRecievingFromDeposit,
	}
	txScala := ropedomain.TxScala{
		TxId:     tx.TxID,
		Time:     tx.BlockTime,
		ScoreInc: 1,
	}
	te := ropedomain.NewTraitEvent(
		author,
		trait,
		depositAndRule,
		otherAndRule,
		txScala,
	)
	return dm.relPool.RopeDB().PushTraitEvent(te)
}

// 새 디포짓 주소 발견한 경우
func (dm *DualManager) DetectNewDepositAddress(dd *domain.DetectedDeposit) error {
	return dm.relPool.GetApooPort().GetNodPort().GetEoPort().SaveDetectedDeposit(dd)
}

// handleDepositDetection handles detection of new deposit addresses
func (dm *DualManager) handleDepositDetection(cexAddr, depositAddr domain.Address, tx *domain.MarkedTransaction, time chaintimer.ChainTime) error {
	//fmt.Printf("💰 handleDepositDetection: %s → CEX %s\n", depositAddr.String()[:10]+"...", cexAddr.String()[:10]+"...")
	debugEnabled := false
	// 1. 새로운 입금주소를 detectedDepositAddress에 추가
	dd := &domain.DetectedDeposit{
		CEXAddress: cexAddr,
		Address:    depositAddr,
		TxCount:    1,
		DetectedAt: time,
	}
	if err := dm.DetectNewDepositAddress(dd); err != nil {
		fmt.Printf("   ❌ DetectNewDepositAddress failed: %v\n", err)
		return err
	}
	if debugEnabled {

		fmt.Printf("   ✅ DetectNewDepositAddress succeeded\n")
	}
	// CEX와 Deposit의 연결을 그래프DB에 추가
	if err := dm.saveCexAndDepositToGraphDB(cexAddr, depositAddr, tx.TxID, time); err != nil {
		fmt.Printf("Cex, Deposit연결을 그래프DB저장하려던 중 에러남")
	}
	// 3. DualManager의 pendingRelationsDB에서 depositAddr을 to로 하는 []fromScala 값들 조회
	fromScalas, err := dm.infra.PendingRelationRepo.GetPendingRelations(depositAddr)
	if err == nil && len(fromScalas) > 0 {
		// 3. [](to,from) 쌍들을 그래프DB에 저장
		for _, fromScala := range fromScalas {
			if err := dm.saveDualRelationToGraphDB(fromScala, depositAddr); err != nil {
				return err
			}
		}

		// 처리된 관계 제거
		if err := dm.infra.PendingRelationRepo.DeletePendingRelations(depositAddr); err != nil {
			return err
		}
		if isRemoved := dm.DeleteUserFromTimeBucket(depositAddr); !isRemoved {
			fmt.Printf("dm.handleDepositDetection: 팬딩 릴레이션에선 depositAddr제거했는데, 타임버킷에선 depositAddr을 찾지 못해서 제거하지 못함")
			return nil
		}
	}

	return nil
}

// saveDualToGraphDB saves from-to dual relationship to graph database
func (dm *DualManager) saveDualToGraphDB(fromScala infra.FromScala, toAddr domain.Address) error {
	return dm.saveDualRelationToGraphDB(fromScala, toAddr)
}

func (dm *DualManager) saveCexAndDepositToGraphDB(cex, deposit domain.Address, txId domain.TxId, lastTime chaintimer.ChainTime) error {
	trait := roperepo.TraitCexAndDeposit
	addrAndRule1 := ropedomain.AddressAndRule{
		Address: cex,
		Rule:    roperepo.RuleCex,
	}
	addrAndRule2 := ropedomain.AddressAndRule{
		Address: deposit,
		Rule:    roperepo.RuleDeposit,
	}
	txScala := ropedomain.TxScala{
		TxId:     txId,
		Time:     lastTime,
		ScoreInc: 1,
	}
	traitEvent := ropedomain.NewTraitEvent(
		roperepo.AuthorSystem,
		trait,
		addrAndRule1,
		addrAndRule2,
		txScala,
	)
	return dm.relPool.RopeDB().PushTraitEvent(traitEvent)
}

// saveDualRelationToGraphDB saves a connection between two EOAs to the graph database
func (dm *DualManager) saveDualRelationToGraphDB(fromScala infra.FromScala, toAddr domain.Address) error {
	trait := roperepo.TraitDepositAndUser
	addressAndRule1 := ropedomain.AddressAndRule{
		Address: fromScala.FromAddress,
		Rule:    roperepo.RuleUser,
	}
	addressAndRule2 := ropedomain.AddressAndRule{
		Address: toAddr,
		Rule:    roperepo.RuleDeposit,
	}
	txScala := ropedomain.TxScala{
		TxId: fromScala.LastTxId,
		Time: fromScala.LastTime,
		//1만큼 증가
		ScoreInc: fromScala.Volume,
	}

	traitEvent := ropedomain.NewTraitEvent(
		roperepo.AuthorSystem,
		trait,
		addressAndRule1,
		addressAndRule2,
		txScala,
	)
	return dm.relPool.RopeDB().PushTraitEvent(traitEvent)

}

// 일반 트랜잭션 → 윈도 버퍼/팬딩 적재
func (dm *DualManager) AddToWindowBuffer(tx *domain.MarkedTransaction) (*domain.MarkedTransaction, error) {
	txTime := tx.BlockTime
	toAddr := tx.To
	fromAddr := tx.From

	static_counter++

	// 윈도 내 미존재시에만 현재 주 버킷에 추가
	_, _ = dm.infra.TimeBucketManager.AddIfAbsent(toAddr, txTime)
	fromScala := infra.FromScala{
		FromAddress: fromAddr,
		LastTxId:    tx.TxID,
		LastTime:    tx.BlockTime,
	}
	if err := dm.infra.PendingRelationRepo.AddToPendingRelations(toAddr, fromScala); err != nil {
		return nil, fmt.Errorf("AddToWindowBuffer: failed to AddToPendingRelations: %w", err)
	}
	return nil, fp.ErrorSkipStep
}

// 디버깅용 전역 카운터
var static_counter int64

// DeleteUserFromTimeBucket removes the given to user from the sliding window buckets.
// It scans from the latest bucket (rearIndex) backward and deletes on first match.
// Returns true if the user was found and removed; otherwise returns false.
func (dm *DualManager) DeleteUserFromTimeBucket(toAddr domain.Address) bool {
	return dm.infra.TimeBucketManager.DeleteToUser(toAddr)
}

func (dm *DualManager) GetWindowStats() map[string]any {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	return map[string]any{
		"active_buckets":       dm.infra.TimeBucketManager.ActiveCount(),
		"total_to_users":       dm.infra.TimeBucketManager.TotalToUsers(),
		"pending_relations":    dm.infra.PendingRelationRepo.CountPendingRelations(),
		"window_size_hours":    infra.WindowSize.Hours(),
		"slide_interval_hours": infra.SlideInterval.Hours(),
		"max_buckets":          infra.MaxTimeBuckets,
	}
}
