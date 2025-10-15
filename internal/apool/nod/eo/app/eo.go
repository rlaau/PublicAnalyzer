package app

import (
	"context"
	"path/filepath"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/eo/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// TODO 어그리게이터에 맞게, 이벤트 기반으로 잘 꾸며보기.
// TODO 밑의 TODO들 참고하삼.
type EoDesign interface {
	gface.Module
	SaveDetectedDeposit(deposit *domain.DetectedDeposit) error
	IsDepositAddress(addr domain.Address) (bool, error)
	IsCex(addr domain.Address) bool
	CEXAddresses() map[string]struct{}
	UpdateDepositTxCount(addr domain.Address, count int64) error
	GetDepositInfo(addr domain.Address) (*domain.DetectedDeposit, error)
}

// TODO 추후 EO도 스트리밍 가능하게 바꾸기!! 그럴 시엔 아래 인터페이스를 매꿀 것!!!
func (eo *Eo) Name() gface.ModuleName          { return gface.ParseModuleName("Eo") }
func (eo *Eo) Start(ctx context.Context) error { panic("not implemented") }
func (eo *Eo) Close() error                    { panic("not implemented") }

type Eo struct {
	mode      mode.ProcessingMode
	cexDB     *infra.FileCEXRepository
	depositDB *infra.DepositRepository
}

func NewEo(mode mode.ProcessingMode, nodPool iface.NodPort) *Eo {
	var rootDir string
	if mode.IsTest() {
		rootDir = computation.ComputeThisTestingStorage("nod", "eo")

	} else {
		rootDir = computation.ComputeThisProductionStorage("nod", "eo")
	}
	cexDB, err := infra.NewFileCEXRepository(mode, filepath.Join(rootDir, "cex.txt"))
	if err != nil {
		panic("cexDB열기 중 에러 발생")
	}
	depDb, err := infra.NewBadgerDepositRepository(mode, filepath.Join(rootDir, "deposit"), 128)
	if err != nil {
		panic("deposit열기 중 에러 발생")
	}
	return &Eo{cexDB: cexDB,
		depositDB: depDb,
		mode:      mode}

}

func (eo *Eo) SaveDetectedDeposit(deposit *domain.DetectedDeposit) error {
	return eo.depositDB.SaveDetectedDeposit(deposit)
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

func (eo *Eo) GetDepositInfo(addr domain.Address) (*domain.DetectedDeposit, error) {
	return eo.depositDB.GetDepositInfo(addr)
}

// !!! 이 밑은 테스트용으로 DB바꾸는 코드임. 주의!!
// ! 프로덕션에선 쓰지 말 것!

func (eo *Eo) ChangeDBPath(target string, path string) {
	var err error
	switch target {
	case "CEX":
		eo.cexDB, err = infra.NewFileCEXRepository(eo.mode, path)
		if err != nil {
			panic("Co DB 바꾸는 중 에러 발생")
		}
	case "DEP":
		eo.depositDB, err = infra.NewBadgerDepositRepository(eo.mode, path, 128)
		if err != nil {
			panic("Co DB 바꾸는 중 에러 발생")
		}
	default:
		panic("DB코드 아님")
	}
}
