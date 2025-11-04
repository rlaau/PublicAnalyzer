package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

// d_model.go: 도메인이 지니는 핵심 규칙을 모델링 함.
// 도메인의 엔티티가 어떻게 구성되는지, 업데이트 되는 규칙은 무엇인지 모델링함.
// 가능한 한 primitive타입들을 래핑해서 파싱으로 생성되도록 유도함.
// 도메인의 부가적인 기능 (ex: 마셜링) 등은 여기서 정의하지 않음.
// 오직 도메인에서 바라본 핵심 기능들만을 정의함. 이것 만으로도 설계를 표현 가능.

// CexAddress는 CexAddress주소를 뜻함
type CexAddress domain.Address

// DepositAddress는 DepositAddress주소를 뜻함. DepositAddress은 CEX에서 User마다 부여하는 개인 예치 지갑을 말함. 일종의 프록시임.
type DepositAddress domain.Address

// UserAddress는 UserAddress주소를 뜻함. 자신의 DEX자산을 CEX거래소에 입금하고자 하는 사람의 지갑 주소임.
type UserAddress domain.Address

// UndefinedAddress는 아직 정체가 정해지지 않는 지갑의 주소임
type UndefinedAddress domain.Address

// FromScala는 From 주소 입장에서 본 To주소와의 거래 강도임.
type FromScala struct {
	FromAddress domain.Address
	LastTxId    domain.TxId
	LastTime    chaintimer.ChainTime
	Volume      int32 //Volume==거래회수
	ValueOrNull fp.Option[domain.Value]
}

// Update updates the FromScala with new transaction data
func (fs *FromScala) Update(txId domain.TxId, txTime chaintimer.ChainTime, value fp.Option[domain.Value]) {
	fs.LastTxId = txId
	fs.LastTime = txTime
	if fs.Volume < 2147483647 { // math.MaxInt32
		fs.Volume++
	}
	if !value.V.IsZero() {
		fs.ValueOrNull = domain.ParseValueOption(fs.ValueOrNull.V.Add(value.V))
	}
}

// Matches checks if this FromScala is for the given address
func (fs *FromScala) Matches(addr domain.Address) bool {
	return fs.FromAddress == addr
}

type FromScalas []FromScala

// FindByAddress finds the FromScala for the given address
func (fss FromScalas) FindByAddress(addr domain.Address) (int, bool) {
	for i := range fss {
		if fss[i].Matches(addr) {
			return i, true
		}
	}
	return -1, false
}

// AddOrUpdate adds a new FromScala or updates an existing one
func (fss FromScalas) AddOrUpdate(from domain.Address, txId domain.TxId, txTime chaintimer.ChainTime, value fp.Option[domain.Value]) FromScalas {
	idx, found := fss.FindByAddress(from)
	if found {
		fss[idx].Update(txId, txTime, value)
		return fss
	}
	return append(fss, NewFromScala(from, txId, txTime, value))
}

// Copy creates a deep copy of the FromScalas
func (fss FromScalas) Copy() FromScalas {
	if len(fss) == 0 {
		return FromScalas{}
	}
	result := make(FromScalas, len(fss))
	copy(result, fss)
	return result
}
