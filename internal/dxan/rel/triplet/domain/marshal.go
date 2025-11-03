package domain

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
)

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
	v, _ := fs.ValueOrNull.Marshal()
	return computation.WriteVarBytes(w, v)
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
	b, err := computation.ReadVarBytes(r)
	var v ropedomain.ValueOrNull
	if err != nil {
		return err
	}

	v, _ = v.Unmarshal(b)

	fs.ValueOrNull = v
	return nil
}

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
