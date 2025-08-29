package nod

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// TODO rel에도 동일한 디스트리뷰터 있음
// TODO 추후 일반화하기
// 새로운 단일 Kafka 배치 소비자 + eventbus 팬아웃
type TxDistributor struct {
	consumer <-chan domain.MarkedTransaction

	// 각 모듈별 eventbus
	busEo  *eventbus.EventBus[domain.MarkedTransaction]
	busCo *eventbus.EventBus[domain.MarkedTransaction]

	stop chan struct{}
	wg   sync.WaitGroup
}

// 새로운 TxFanoutManager 생성자 - 내부적으로 transaction 버스들 생성
func NewTxDistributor(isTest mode.ProcessingMode, capLimit int, consumer <-chan domain.MarkedTransaction) (*TxDistributor, error) {
	// 테스트/프로덕션 루트 분기
	var root func() string
	if isTest.IsTest() {
		root = computation.FindTestingStorageRootPath
	} else {
		root = computation.FindProductionStorageRootPath
	}

	// Transaction 분배용 버스들 생성 (tx_fanout 폴더에 저장)
	rel := func(name string) string {
		return filepath.Join("relation_pool", "tx_fanout", fmt.Sprintf("%s.jsonl", name))
	}

	busTriplet, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_triplet"), capLimit)

	busCreation, err := eventbus.NewWithRoot[domain.MarkedTransaction](root, rel("tx_creation"), capLimit)
	if err != nil {
		busTriplet.Close()
		return nil, fmt.Errorf("failed to create CCE tx bus: %w", err)
	}

	f := &TxDistributor{
		consumer:    consumer,
		busCo: busTriplet,
		busEo:  busCreation,
		stop:        make(chan struct{}),
	}
	f.wg.Add(1)
	go f.run()
	return f, nil
}

func (f *TxDistributor) run() {
	defer f.wg.Done()
	//TODO 극한의 튜닝 필요 시엔 여기를 배칭으로 바꿀 수 있음
	//TODO 하지만 그 경우의 성능 향상이 과연 클까? 인메모리 버스인데??
	//TODO 옵션 정도로만 생각하기
	for {
		select {
		case <-f.stop:
			return
		case tx, ok := <-f.consumer:
			if !ok { // 소비 채널 닫힘 → 종료
				return
			}
			f.fanoutTransaction(tx)
		}
	}
}

// 트랜잭션 분배 로직
func (f *TxDistributor) fanoutTransaction(tx domain.MarkedTransaction) {
	//TODO: 목표는 relation pool이 각 모듈에게 적절한 tx분배하는 것. 현재는 오직 Triplet모듈에만 tx주고 있음. 추후 MarkTransaction기반으로 올바르게 전달할 것.

	// !!!!!!현재는 모든 트랜잭션을 Triplet 모듈에만 전달
	if err := f.busCo.Publish(tx); err != nil {
		fmt.Printf("[TxFanoutManager] failed to publish to EE: %v\n", err)
	}

	// !!향후 구현될 분배 로직:
	//TODO 내 경우, 포트 기반 분배
	// switch {
	// case tx.TxSyntax[0] == domain.EOAMark && tx.TxSyntax[1] == domain.EOAMark:
	//     // EOA -> EOA: EE 모듈
	//     f.busEE.Publish(tx)
	// case tx.TxSyntax[0] == domain.ContractMark && tx.TxSyntax[1] == domain.ContractMark:
	//     // Contract -> Contract: CC 모듈
	//     f.busCC.Publish(tx)
	// case tx.TxSyntax[0] == domain.ContractMark && tx.TxSyntax[1] == domain.EOAMark:
	//     // Contract -> EOA: CCE 모듈
	//     f.busCCE.Publish(tx)
	// case tx.TxSyntax[0] == domain.EOAMark && tx.TxSyntax[1] == domain.ContractMark:
	//     // EOA -> Contract: EEC 모듈
	//     f.busEEC.Publish(tx)
	// }
}

// TxFanoutManager Close 메서드
func (f *TxDistributor) Close() error {
	close(f.stop) // run 루프 깨우기
	f.wg.Wait()   // goroutine 합류

	// 자체 관리하는 transaction 버스들 정리
	f.busCo.Close()
	f.busEo.Close()
	// consumer는 이걸 선언한 상위에서 닫음
	return nil
}
