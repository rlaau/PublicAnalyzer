package app

import (
	"path/filepath"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/eo/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

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
