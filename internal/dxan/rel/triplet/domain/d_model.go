package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type CEX domain.Address
type Deposit domain.Address
type User domain.Address

// From 주소 입장에서 본 To주소와의 거래 강도
type FromScala struct {
	FromAddress domain.Address
	LastTxId    domain.TxId
	LastTime    chaintimer.ChainTime
	Volume      int32 //Volume==거래회수
	ValueOrNull ropedomain.ValueOrNull
}

// Update updates the FromScala with new transaction data
func (fs *FromScala) Update(txId domain.TxId, txTime chaintimer.ChainTime, value ropedomain.ValueOrNull) {
	fs.LastTxId = txId
	fs.LastTime = txTime
	if fs.Volume < 2147483647 { // math.MaxInt32
		fs.Volume++
	}
	if !value.V.IsZero() {
		fs.ValueOrNull = ropedomain.ParseValue(fs.ValueOrNull.V.Add(value.V))
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
func (fss FromScalas) AddOrUpdate(from domain.Address, txId domain.TxId, txTime chaintimer.ChainTime, value ropedomain.ValueOrNull) FromScalas {
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
