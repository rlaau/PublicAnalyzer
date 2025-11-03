package domain

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/shopspring/decimal"
)

// From 주소 입장에서 본 To주소와의 거래 강도
type FromScala struct {
	FromAddress domain.Address
	LastTxId    domain.TxId
	LastTime    chaintimer.ChainTime
	Volume      int32
	ValueOrNull ropedomain.Value
}

// === Marshaling/Unmarshaling ===

// Marshal serializes a single FromScala to binary format
func (fs *FromScala) Marshal(w io.Writer) error {
	// FromAddress (20B)
	if _, err := w.Write(fs.FromAddress[:]); err != nil {
		return err
	}
	// LastTxId (32B)
	if _, err := w.Write(fs.LastTxId[:]); err != nil {
		return err
	}
	// LastTime (int64 unix)
	unix := time.Time(fs.LastTime).UTC().Unix()
	if err := binary.Write(w, binary.LittleEndian, unix); err != nil {
		return err
	}
	// Volume (int32)
	if err := binary.Write(w, binary.LittleEndian, fs.Volume); err != nil {
		return err
	}
	// ValueOrNull (var-bytes, ascii decimal)
	return fs.ValueOrNull.Marshal(w)
}

// Unmarshal deserializes a single FromScala from binary format
func (fs *FromScala) Unmarshal(r io.Reader) error {
	// FromAddress (20B)
	if _, err := io.ReadFull(r, fs.FromAddress[:]); err != nil {
		return err
	}
	// LastTxId (32B)
	if _, err := io.ReadFull(r, fs.LastTxId[:]); err != nil {
		return err
	}
	// LastTime
	var unix int64
	if err := binary.Read(r, binary.LittleEndian, &unix); err != nil {
		return err
	}
	fs.LastTime = chaintimer.ChainTime(time.Unix(unix, 0).UTC())
	// Volume
	if err := binary.Read(r, binary.LittleEndian, &fs.Volume); err != nil {
		return err
	}
	// ValueOrNull
	d, err := readDecimal(r)
	if err != nil {
		return err
	}
	fs.ValueOrNull = ropedomain.ParseValue(d)
	return nil
}

// NewFromScala creates a FromScala with initial values
func NewFromScala(from domain.Address, txId domain.TxId, txTime chaintimer.ChainTime, value ropedomain.Value) FromScala {
	volume := int32(1)
	return FromScala{
		FromAddress: from,
		LastTxId:    txId,
		LastTime:    txTime,
		Volume:      volume,
		ValueOrNull: value,
	}
}

// Update updates the FromScala with new transaction data
func (fs *FromScala) Update(txId domain.TxId, txTime chaintimer.ChainTime, value ropedomain.Value) {
	fs.LastTxId = txId
	fs.LastTime = txTime
	if fs.Volume < 2147483647 { // math.MaxInt32
		fs.Volume++
	}
	if !value.IsZero() {
		fs.ValueOrNull = fs.ValueOrNull.Add(value)
	}
}

// Matches checks if this FromScala is for the given address
func (fs *FromScala) Matches(addr domain.Address) bool {
	return fs.FromAddress == addr
}

type FromScalas []FromScala

// Marshal serializes FromScalas slice to binary format
func (fss FromScalas) Marshal(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(fss))); err != nil {
		return err
	}
	for i := range fss {
		if err := fss[i].Marshal(w); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalFromScalas deserializes FromScalas slice from binary format
func UnmarshalFromScalas(r io.Reader) (FromScalas, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	out := make(FromScalas, int(n))
	for i := 0; i < int(n); i++ {
		if err := out[i].Unmarshal(r); err != nil {
			return nil, err
		}
	}
	return out, nil
}

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
func (fss FromScalas) AddOrUpdate(from domain.Address, txId domain.TxId, txTime chaintimer.ChainTime, value ropedomain.Value) FromScalas {
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

func readVarBytes(r io.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
func readDecimal(r io.Reader) (decimal.Decimal, error) {
	b, err := readVarBytes(r)
	if err != nil {
		return decimal.Zero, err
	}
	if len(b) == 0 {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(string(b))
}
