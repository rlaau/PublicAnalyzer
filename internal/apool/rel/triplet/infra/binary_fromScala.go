package infra

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	ldomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/shopspring/decimal"
)

// === binary helpers for FromScala ===

func writeVarBytes(w *bytes.Buffer, b []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
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

// TxId를 [32]byte로 가정. (다르면 여기만 바꾸면 됨)
const txIDLen = 32

func writeTxID(w *bytes.Buffer, id domain.TxId) error {
	// id가 [32]byte라면 아래와 같이 바로 쓰면 됨.
	_, err := w.Write(id[:])
	return err
}

func readTxID(r io.Reader, id *domain.TxId) error {
	_, err := io.ReadFull(r, id[:])
	return err
}

// decimal.Decimal은 텍스트(ASCII)로 저장 (정밀도 보존 + 구현 간단)
func writeDecimal(w *bytes.Buffer, d decimal.Decimal) error {
	txt := d.String() // "0", "123.45", ...
	return writeVarBytes(w, []byte(txt))
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

// FromScala 슬라이스 쓰기
func writeFromScalas(w *bytes.Buffer, xs []ldomain.FromScala) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(xs))); err != nil {
		return err
	}
	for i := range xs {
		x := &xs[i]
		// FromAddress (20B)
		if _, err := w.Write(x.FromAddress[:]); err != nil {
			return err
		}
		// LastTxId (32B 가정)
		if err := writeTxID(w, x.LastTxId); err != nil {
			return err
		}
		// LastTime (int64 unix)
		unix := time.Time(x.LastTime).UTC().Unix()
		if err := binary.Write(w, binary.LittleEndian, unix); err != nil {
			return err
		}
		// Volume (int32)
		if err := binary.Write(w, binary.LittleEndian, x.Volume); err != nil {
			return err
		}
		// ValueOrNull (var-bytes, ascii decimal)
		if err := writeDecimal(w, x.ValueOrNull); err != nil {
			return err
		}
	}
	return nil
}

// FromScala 슬라이스 읽기
func readFromScalas(r io.Reader) ([]ldomain.FromScala, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	out := make([]ldomain.FromScala, int(n))
	for i := 0; i < int(n); i++ {
		x := &out[i]
		// FromAddress (20B)
		if _, err := io.ReadFull(r, x.FromAddress[:]); err != nil {
			return nil, err
		}
		// LastTxId (32B 가정)
		if err := readTxID(r, &x.LastTxId); err != nil {
			return nil, err
		}
		// LastTime
		var unix int64
		if err := binary.Read(r, binary.LittleEndian, &unix); err != nil {
			return nil, err
		}
		x.LastTime = chaintimer.ChainTime(time.Unix(unix, 0).UTC())
		// Volume
		if err := binary.Read(r, binary.LittleEndian, &x.Volume); err != nil {
			return nil, err
		}
		// ValueOrNull
		d, err := readDecimal(r)
		if err != nil {
			return nil, err
		}
		x.ValueOrNull = d
	}
	return out, nil
}
