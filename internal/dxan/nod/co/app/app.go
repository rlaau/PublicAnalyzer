package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/co/infra"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/co/pface"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
	"golang.org/x/crypto/sha3"
	"golang.org/x/sync/errgroup"
)

type CoDesign interface {
	gface.Module
	gface.Child
	RegisterContract(creator domain.Address, nonce domain.Nonce, blockTime chaintimer.ChainTime, txId domain.TxId) (domain.Address, error)
	CheckIsContract(address domain.Address) bool
}
type Co struct {
	nodPort pface.NodPort
	closed  atomic.Bool
	count   atomic.Int64
	contDB  *infra.ContractDB
	mode    mode.ProcessingMode
}

type CoCfg struct {
	Mode mode.ProcessingMode
}

func NewCo(coCfg CoCfg) *Co {

	contDB, err := infra.NewContractDB(coCfg.Mode)
	if err != nil {
		fmt.Printf("NewCo: 에러 발생: %s", err.Error())

		panic("contDB오픈 중에 에러 발생")
	}

	return &Co{
		contDB: contDB,
		mode:   coCfg.Mode,
	}
}
func (co *Co) Name() gface.ModuleName {
	return gface.ParseModuleName("Co")
}

func (co *Co) Start(parent context.Context) error {
	if co.nodPort == nil {
		return fmt.Errorf("Co not have notport")
	}
	g, ctx := errgroup.WithContext(parent)
	g.Go(func() error {
		upstream := co.nodPort.ConsumeCoTxByFanout()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case _, ok := <-upstream:
				if !ok {
					return nil
				}
				co.count.Add(1)
				if co.count.Load()%100_000 == 0 {
					fmt.Printf("현재 co가 %d개의 메시지 받고 버렸음\n", co.count.Load())
				}
			}
		}
	})
	err := g.Wait()
	co.Close()
	return err
}

func (co *Co) Close() error {
	if co.closed.Swap(true) {
		return nil
	}
	if err := co.contDB.Close(); err != nil {
		return err
	}
	return nil
}

func (co *Co) Closed() bool {
	return co.closed.Load()
}

func (co *Co) RegisterParent(nod gface.Module) error {
	np, ok := nod.(pface.NodPort)
	if !ok {
		fmt.Printf("RegisterParent:Co가 받은 NodPort가 런타임 타입체크 실패함.")
		return fmt.Errorf("parent is not NodPort")
	}
	co.nodPort = np
	return nil
}
func (co *Co) RegisterContract(creator domain.Address, nonce domain.Nonce, blockTime chaintimer.ChainTime, txId domain.TxId) (domain.Address, error) {

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
