package app

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/eo/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/eo/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/sync/errgroup"
)

type EoDesign interface {
	//노출용 인터페이스
	cface.EoPort
	//추가는 자유
	//추가 시엔 EoModel로, 앱 핵심 추가하는걸 추천
}

// ! 디자인 강제
var _ EoDesign = (*Eo)(nil)

type Eo struct {
	mode      mode.ProcessingMode
	nodPort   pface.NodPort
	closed    atomic.Bool
	count     atomic.Int64
	cexDB     *infra.FileCEXRepository
	depositDB *infra.DepositRepository
}

func NewEo(mode mode.ProcessingMode) *Eo {

	cexDB, err := infra.NewFileCEXRepository(mode)
	if err != nil {
		panic("cexDB열기 중 에러 발생")
	}
	depDb, err := infra.NewBadgerDepositRepository(mode, 128)
	if err != nil {
		panic("deposit열기 중 에러 발생")
	}
	return &Eo{
		cexDB:     cexDB,
		depositDB: depDb,
		mode:      mode,
		count:     atomic.Int64{},
	}

}

func (eo *Eo) Name() gface.ModuleName { return gface.ParseModuleName("Eo") }
func (eo *Eo) Start(parent context.Context) error {

	if eo.nodPort == nil {
		return fmt.Errorf("EO not have nodport")
	}

	g, ctx := errgroup.WithContext(parent)

	g.Go(func() error {
		upstream := eo.nodPort.ConsumeEoTxByFanout()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case _, ok := <-upstream:
				if !ok {
					return nil
				}
				eo.count.Add(1)
				if eo.count.Load()%100_000 == 0 {
					fmt.Printf("현재 eo가 %d개의 메시지 받고 버렸음\n", eo.count.Load())
				}
			}
		}
	})
	err := g.Wait()
	eo.Close()
	return err
}

func (eo *Eo) Close() error {
	if eo.closed.Swap(true) {
		return nil
	}
	if err := eo.cexDB.Close(); err != nil {
		return err
	}

	if err := eo.depositDB.Close(); err != nil {
		return err
	}
	return nil
}
func (eo *Eo) Closed() bool { return eo.closed.Load() }
func (eo *Eo) RegisterParent(nod gface.Module) error {
	np, ok := nod.(pface.NodPort)
	if !ok {
		fmt.Printf("RegisterParent: Eo가 받은 NodPort가 런타임 타입체크 실패함")
		return fmt.Errorf("parent is not NodPort")
	}
	eo.nodPort = np
	return nil
}

func (eo *Eo) SaveDetectedDeposit(deposit domain.DepositDetection) error {
	return eo.depositDB.UpdateDetectedDeposit(deposit)
}

func (eo *Eo) IsDepositAddress(addr domain.Address) (bool, error) {
	return eo.depositDB.IsDepositAddress(addr)
}

func (eo *Eo) CEXAddresses() map[string]struct{} {
	return eo.cexDB.GetCexSet()
}
func (eo *Eo) IsCex(addr domain.Address) bool {
	return eo.cexDB.IsContain(addr.String())
}
func (eo *Eo) UpdateDepositTxCount(addr domain.Address, count int64) error {
	//TODO 이것도 이벤트 어그리게이터 기반으로 바꾸기
	//TODO 주소를 이벤트화 해서 디포짓 DB에 이벤트 보내는 걸로. 동기 처리해도 되고 아녀도 되고. 블로킹이라 해도 GMP가 잘 해주니
	return eo.depositDB.UpdateTxCount(addr, count)
}

func (eo *Eo) GetDepositInfo(addr domain.Address) (*domain.DepositDetection, error) {
	return eo.depositDB.GetDepositInfo(addr)
}
