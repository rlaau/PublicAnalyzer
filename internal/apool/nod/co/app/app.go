package app

import (
	"bytes"
	"errors"
	"math/big"
	"path/filepath"

	"github.com/ethereum/go-ethereum/rlp"
	codomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/infra"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/crypto/sha3"
)

type Co struct {
	contDB    *infra.ContractDB
	cexDB     *infra.FileCEXRepository
	depositDB *infra.DepositRepository

	mode mode.ProcessingMode
}

type CoCfg struct {
	Mode mode.ProcessingMode
}

func NewCo(coCfg CoCfg, nodPool iface.NodPort) *Co {
	var rootDir string
	if coCfg.Mode.IsTest() {
		rootDir = computation.ComputeThisTestingStorage("nod/co")
	} else {
		//프로덕션 환경에선 고유 루트 디렉터리 사용
		rootDir = computation.ComputeThisProductionStorage("nod/co")
	}
	contDbDir := filepath.Join(rootDir, "cont")
	contDB, err := infra.NewContractDB(coCfg.Mode, contDbDir)
	if err != nil {
		panic("contDB오픈 중에 에러 발생")
	}
	cexDB, err := infra.NewFileCEXRepository(coCfg.Mode, filepath.Join(rootDir, "cex.txt"))
	if err != nil {
		panic("cexDB열기 중 에러 발생")
	}
	depDb, err := infra.NewBadgerDepositRepository(coCfg.Mode, filepath.Join(rootDir, "deposit"), 128)
	if err != nil {
		panic("deposit열기 중 에러 발생")
	}
	return &Co{
		contDB:    contDB,
		cexDB:     cexDB,
		depositDB: depDb,
		mode:      coCfg.Mode,
	}
}

func (co *Co) RegisterContract(creator domain.Address, nonce domain.Nonce, blockTime chaintimer.ChainTime, txId domain.TxId) (domain.Address, error) {
	//TODO 이건 좀 로직임. 생성 시 잡는거라서
	inferedCreated, isOk := tryPredictCreatedContractAddress(creator, nonce)
	if !isOk {
		return domain.Address{}, errors.New("컨트렉트 주소 추론 실패")
	}
	//이미 포착한 경우 추가 x
	if co.contDB.IsContain(inferedCreated) {
		return inferedCreated, nil
	}
	//! 여기 부분에 로직 추가하기!!
	//TODO 현재는 2025-01-01까지의 컨트렉트 모두 있다고 판단해서 따로 추가하진 않음
	//TODO 추후엔 인퍼링 강화, 추가시 바이트 코드 분석 통한 토큰이나 프로토콜 심볼 파악 등이 추가될 것!!!
	//TODO 보통은 디멘디드 분석이 맞을듯. 기본적으로 싹다 false인데, 좀 의심되는 놈이다 싶음 추가 분석으로
	return inferedCreated, nil
}

func (co *Co) SaveDetectedDeposit(deposit *codomain.DetectedDepositWithEvidence) error {
	return co.depositDB.SaveDetectedDeposit(deposit)
}

func (co *Co) IsDepositAddress(addr domain.Address) (bool, error) {
	return co.depositDB.IsDepositAddress(addr)
}
func (co *Co) IsCex(addr domain.Address) bool {
	return co.cexDB.IsContain(addr.String())
}
func (co *Co) UpdateDepositTxCount(addr domain.Address, count int64) error {
	return co.depositDB.UpdateTxCount(addr, count)
}

func (co *Co) GetDepositInfo(addr domain.Address) (*codomain.DetectedDepositWithEvidence, error) {
	return co.depositDB.GetDepositInfo(addr)
}

func (co *Co) CheckIsContract(address domain.Address) bool {
	return co.contDB.IsContain(address)

}

// tryPredictCreatedContractAddress(from, nonce) with RLP + Legacy Keccak-256
func tryPredictCreatedContractAddress(from domain.Address, nonce domain.Nonce) (domain.Address, bool) {
	nonceUint, nonceValid := nonce.Uint64()
	if !nonceValid {
		return domain.Address{}, false
	}
	var buf bytes.Buffer
	if nonceUint == 0 {
		_ = rlp.Encode(&buf, []interface{}{from[:], ""})
	} else {
		_ = rlp.Encode(&buf, []interface{}{from[:], new(big.Int).SetUint64(nonceUint)})
	}
	h := sha3.NewLegacyKeccak256()
	h.Write(buf.Bytes())
	sum := h.Sum(nil)

	var out [20]byte
	copy(out[:], sum[12:])
	return out, true
}

// !!! 이 밑은 테스트용으로 DB바꾸는 코드임. 주의!!
// ! 프로덕션에선 쓰지 말 것!

func (co *Co) ChangeDBPath(target string, path string) {
	var err error
	switch target {
	case "CEX":
		co.cexDB, err = infra.NewFileCEXRepository(co.mode, path)
		if err != nil {
			panic("Co DB 바꾸는 중 에러 발생")
		}
	case "DEP":
		co.depositDB, err = infra.NewBadgerDepositRepository(co.mode, path, 128)
		if err != nil {
			panic("Co DB 바꾸는 중 에러 발생")
		}
	case "CON":
		co.contDB, err = infra.NewContractDB(co.mode, path)
		if err != nil {
			panic("Co DB 바꾸는 중 에러 발생")
		}
	default:
		panic("DB코드 아님")
	}
}
