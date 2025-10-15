package app

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/roperepo"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/pface"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/meter/rsv"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

// DualManager manages EOA relationships through sliding window analysis
type DualManager struct {
	infra *infra.DualManagerInfra

	relPool pface.RelPort

	// Synchronization (최적화된 뮤텍스)
	mutex sync.RWMutex // 전체 구조체 보호용 (구조 변경 등)

	// Performance monitoring
	perfMeter *rsv.ReservoirMeter
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewDualManager creates a new dual manager instance
func NewDualManager(managerInfra *infra.DualManagerInfra, pool pface.RelPort) (*DualManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	dm := &DualManager{
		infra:     managerInfra,
		relPool:   pool,
		perfMeter: rsv.NewFunctionReservoirMeter(10_000),
		ctx:       ctx,
		cancel:    cancel,
	}

	// 성능 모니터링 고루틴 시작 (20초마다 통계 출력)
	go dm.startPerformanceMonitoring()

	return dm, nil
}

// Close closes the dual manager and its resources
func (dm *DualManager) Close() error {
	dm.cancel() // 성능 모니터링 고루틴 종료

	if dm.infra.HeavyTimeBucketManager != nil {
		return dm.infra.HeavyTimeBucketManager.Close()
	}
	return nil
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
// TODO 함수명 변경 & 함수 끊기. 1) HandleXXX는 "XXX"라는 "이벤트"를 처리해야 함. 2) 그거 바탕으로 함수 끊기
// TODO 그러므로 HandleAddress는 잘못된 작명임
// TODO 수정 시 "선언적으로" 바꿔서 처리하기
// TODO 그러니까, 내가 원하는 것은 (case1->case1 Callback)같은 거잖음.
// TODO 그럼, 이렇게 처리하삼
// TODO 1->  TransfromToRelation따라 케이스 분류
// TODO 2-> PublishRelationEvent이벤트발행
// TODO 3-> HandleRelationEvent 이벤트 처리
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
	// author := roperepo.AuthorSystem
	// trait := roperepo.TraitCexToOther

	// cexAndRule := ropedomain.AddressAndRule{
	// 	Address: cex,
	// 	Rule:    roperepo.RuleCex,
	// }
	// otherAndRule := ropedomain.AddressAndRule{
	// 	Address: other,
	// 	Rule:    roperepo.RuleRecievingFromCex,
	// }
	// txScala := ropedomain.TxScala{
	// 	TxId:        tx.TxID,
	// 	Time:        tx.BlockTime,
	// 	ScoreInc:    1,
	// 	ValueOrNull: tx.ValueOrNull,
	// }
	// traitEvent := ropedomain.NewTraitEvent(
	// 	author,
	// 	trait,
	// 	cexAndRule,
	// 	otherAndRule,
	// 	txScala,
	// )
	return nil
}

func (dm *DualManager) handleTxFromDepositWithNotCex(deposit domain.Address, other domain.Address, tx *domain.MarkedTransaction) error {
	// author := roperepo.AuthorSystem
	// trait := roperepo.TraitDepositToOther
	// depositAndRule := ropedomain.AddressAndRule{
	// 	Address: deposit,
	// 	Rule:    roperepo.RuleDeposit,
	// }
	// otherAndRule := ropedomain.AddressAndRule{
	// 	Address: other,
	// 	Rule:    roperepo.RuleRecievingFromDeposit,
	// }
	// txScala := ropedomain.TxScala{
	// 	TxId:        tx.TxID,
	// 	Time:        tx.BlockTime,
	// 	ScoreInc:    1,
	// 	ValueOrNull: tx.ValueOrNull,
	// }
	// te := ropedomain.NewTraitEvent(
	// 	author,
	// 	trait,
	// 	depositAndRule,
	// 	otherAndRule,
	// 	txScala,
	// )
	return nil
}

// 새 디포짓 주소 발견한 경우
func (dm *DualManager) DetectNewDepositAddress(dd *domain.DetectedDeposit) error {
	defer rsv.MeasureTime(dm.perfMeter, "DetectNewDepositAddress")()
	return dm.relPool.GetApooPort().GetNodPort().GetEoPort().SaveDetectedDeposit(dd)
}

// handleDepositDetection handles detection of new deposit addresses
// TODO 이런걸 "이벤트"로 처리하기
// TODO "디포짓 발견"을 처리하는 함수인데도, cex, depositAddr을 받는건 로직이 맞지 않음.
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
	if err := dm.saveCexAndDepositToGraphDB(cexAddr, depositAddr, tx, time); err != nil {
		fmt.Printf("Cex, Deposit연결을 그래프DB저장하려던 중 에러남")
	}
	// 3. DualManager의 pendingRelationsDB에서 depositAddr을 to로 하는 []fromScala 값들 조회
	fromScalas, err := dm.getPendingRelations(depositAddr)
	if err == nil && len(fromScalas) > 0 {
		// 3. [](to,from) 쌍들을 그래프DB에 저장
		for _, fromScala := range fromScalas {
			if err := dm.saveDualRelationToGraphDB(fromScala, depositAddr); err != nil {
				return err
			}
		}

	}

	return nil
}
func (dm *DualManager) getPendingRelations(deposit domain.Address) ([]infra.FromScala, error) {
	defer rsv.MeasureTime(dm.perfMeter, "getPendingRelations")()
	return dm.infra.HeavyTimeBucketManager.GetFromScalas(deposit), nil
}

// saveDualToGraphDB saves from-to dual relationship to graph database
func (dm *DualManager) saveDualToGraphDB(fromScala infra.FromScala, toAddr domain.Address) error {
	return dm.saveDualRelationToGraphDB(fromScala, toAddr)
}

func (dm *DualManager) saveCexAndDepositToGraphDB(cex, deposit domain.Address, tx *domain.MarkedTransaction, lastTime chaintimer.ChainTime) error {
	defer rsv.MeasureTime(dm.perfMeter, "saveCexAndDepositToGraphDB")()

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
		TxId:        tx.TxID,
		Time:        lastTime,
		ScoreInc:    1,
		ValueOrNull: tx.ValueOrNull,
	}
	traitEvent := ropedomain.NewTraitEvent(
		roperepo.AuthorSystem,
		trait,
		addrAndRule1,
		addrAndRule2,
		txScala,
	)
	return dm.relPool.DispatchTraitEvent(traitEvent)
}

// saveDualRelationToGraphDB saves a connection between two EOAs to the graph database
func (dm *DualManager) saveDualRelationToGraphDB(fromScala infra.FromScala, toAddr domain.Address) error {
	defer rsv.MeasureTime(dm.perfMeter, "saveDualRelationToGraphDB")()

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
		ScoreInc:    fromScala.Volume,
		ValueOrNull: fromScala.ValueOrNull,
	}

	traitEvent := ropedomain.NewTraitEvent(
		roperepo.AuthorSystem,
		trait,
		addressAndRule1,
		addressAndRule2,
		txScala,
	)
	return dm.relPool.DispatchTraitEvent(traitEvent)

}

// 일반 트랜잭션 → 윈도 버퍼/팬딩 적재
func (dm *DualManager) AddToWindowBuffer(tx *domain.MarkedTransaction) (*domain.MarkedTransaction, error) {
	defer rsv.MeasureTime(dm.perfMeter, "AddToWindowBuffer")()

	toAddr := tx.To
	fromAddr := tx.From

	static_counter++

	// 윈도 내 미존재시에만 현재 주 버킷에 추가
	fromScala := infra.FromScala{
		FromAddress: fromAddr,
		LastTxId:    tx.TxID,
		LastTime:    tx.BlockTime,
	}
	func() {
		defer rsv.MeasureTime(dm.perfMeter, "AddToWindowBuffer.AddIfAbsent")()
		_, _ = dm.infra.HeavyTimeBucketManager.AddIfAbsent(toAddr, fromScala)
	}()

	return nil, fp.ErrorSkipStep
}

// 디버깅용 전역 카운터
var static_counter int64

func (dm *DualManager) GetWindowStats() map[string]any {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	return map[string]any{
		"active_buckets":       dm.infra.HeavyTimeBucketManager.ActiveCount(),
		"total_to_users":       dm.infra.HeavyTimeBucketManager.TotalToUsers(),
		"pending_relations":    dm.infra.HeavyTimeBucketManager.CountPendingRelations(),
		"window_size_hours":    infra.WindowSize.Hours(),
		"slide_interval_hours": infra.SlideInterval.Hours(),
		"max_buckets":          infra.MaxTimeBuckets,
	}
}

// startPerformanceMonitoring은 20초마다 성능 통계를 출력하는 고루틴을 시작합니다.
func (dm *DualManager) startPerformanceMonitoring() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	fmt.Printf("[DualManager Performance Monitor] Started - will report every 20 seconds\n")

	for {
		select {
		case <-dm.ctx.Done():
			fmt.Printf("[DualManager Performance Monitor] Shutting down\n")
			return
		case <-ticker.C:
			dm.printPerformanceStats()
		}
	}
}

// printPerformanceStats는 현재 성능 통계를 출력합니다.
func (dm *DualManager) printPerformanceStats() {
	stats := dm.perfMeter.GetSortedStats()

	if len(stats) == 0 {
		fmt.Printf("=== [DualManager Performance] No function calls recorded ===\n")
		return
	}

	fmt.Printf("\n=== [DualManager Performance Statistics] ===\n")
	fmt.Printf("%-35s %8s %12s %12s %12s %12s\n",
		"Function", "Count", "Mean", "P50", "P95", "P99")
	fmt.Println("---------------------------------------------------------------------------------")

	for _, stat := range stats {
		funcName := stat.FunctionName
		if len(funcName) > 35 {
			funcName = funcName[:32] + "..."
		}

		fmt.Printf("%-35s %8d %12s %12s %12s %12s\n",
			funcName,
			stat.Count,
			formatDuration(stat.Mean),
			formatDuration(stat.P50),
			formatDuration(stat.P95),
			formatDuration(stat.P99),
		)
	}
	fmt.Printf("=== [DualManager Performance End] ===\n\n")
}

// formatDuration은 듀레이션을 읽기 쉬운 형태로 포맷합니다.
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.0fns", float64(d))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.1fμs", float64(d)/float64(time.Microsecond))
	} else if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	} else {
		return fmt.Sprintf("%.3fs", float64(d)/float64(time.Second))
	}
}
