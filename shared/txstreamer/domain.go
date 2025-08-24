package txstreamer

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	. "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/shopspring/decimal"
)

/*
mmap 고정 레코드 레이아웃 (수정 금지)

	0  .. 31   tx_hash [32]
	32 .. 39   nonce u64 [8]
	40 .. 59   from [20]
	60 .. 79   to [20] (null -> zero)
	80 .. 99   receipt_contract_address [20] (null -> zero)

100 .. 131  value_wei u256 [32]
132 .. 139  block_timestamp_us i64 [8]
140 .. 147  block_number u64 [8]        (현재 0)
148 .. 151  transaction_index u32 [4]   (현재 0)
152 .. 159  pad [8]
*/
const (
	RecSize = 160

	OffHash  = 0
	OffNonce = 32
	OffFrom  = 40
	OffTo    = 60
	OffRC    = 80
	OffVal   = 100
	OffTS    = 132
	OffBN    = 140
	OffTXI   = 148
	OffPad   = 152
)

/* ==============================
   유틸 (hex, u256<->decimal, 정규화)
   ============================== */

func normalizeHexLowerNo0x(s string) (string, error) {
	ss := strings.TrimSpace(strings.ToLower(s))
	ss = strings.TrimPrefix(ss, "0x")
	if len(ss)%2 == 1 {
		// nibble 홀수면 앞에 0 패딩
		ss = "0" + ss
	}
	_, err := hex.DecodeString(ss)
	return ss, err
}

func writeHexFixed(dst []byte, hexStr string, want int) error {
	ss, err := normalizeHexLowerNo0x(hexStr)
	if err != nil {
		return err
	}
	b, _ := hex.DecodeString(ss)
	if len(b) > want {
		// 과길이 방어(정상적이면 오지 않음)
		b = b[len(b)-want:]
	}
	// left-pad zeros
	pad := want - len(b)
	for i := 0; i < pad; i++ {
		dst[i] = 0x00
	}
	copy(dst[pad:], b)
	return nil
}

func readHexFixed(src []byte) string {
	return "0x" + strings.ToLower(hex.EncodeToString(src))
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func isZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// u256(32B BE) → decimal (정수 wei)
func u256ToDecimal(b32 [32]byte) decimal.Decimal {
	n := new(big.Int).SetBytes(b32[:])
	return decimal.NewFromBigInt(n, 0)
}

// decimal(정수 wei) → u256(32B BE)
func decimalToU256(d decimal.Decimal) ([32]byte, error) {
	var out [32]byte
	if !d.IsInteger() {
		return out, fmt.Errorf("wei must be integer, got %s", d.String())
	}
	// decimal → big.Int via text
	n := new(big.Int)
	n, ok := n.SetString(d.String(), 10)
	if !ok || n.Sign() < 0 {
		return out, fmt.Errorf("invalid wei %q", d.String())
	}
	b := n.Bytes()
	if len(b) > 32 {
		return out, errors.New("overflow u256")
	}
	copy(out[32-len(b):], b)
	return out, nil
}

/* ===========================================================
   A) mmap 레코드 → RawTransaction
   =========================================================== */

func DecodeRecordToRaw(rec []byte, timeLayout string) (RawTransaction, error) {
	if len(rec) != RecSize {
		return RawTransaction{}, fmt.Errorf("invalid record size: %d", len(rec))
	}
	// 필드 파싱
	txid := strings.ToLower(readHexFixed(rec[OffHash : OffHash+32]))
	nonceU := binary.BigEndian.Uint64(rec[OffNonce : OffNonce+8])
	from := strings.ToLower(readHexFixed(rec[OffFrom : OffFrom+20]))

	to := ""
	if !isZero(rec[OffTo : OffTo+20]) {
		to = strings.ToLower(readHexFixed(rec[OffTo : OffTo+20]))
	}
	rc := ""
	if !isZero(rec[OffRC : OffRC+20]) {
		rc = strings.ToLower(readHexFixed(rec[OffRC : OffRC+20]))
	}

	var v32 [32]byte
	copy(v32[:], rec[OffVal:OffVal+32])
	val := u256ToDecimal(v32)

	tsUs := int64(binary.BigEndian.Uint64(rec[OffTS : OffTS+8]))
	if timeLayout == "" {
		timeLayout = time.RFC3339
	}
	ts := time.Unix(0, tsUs*1000).UTC().Format(timeLayout)

	return RawTransaction{
		TxId:        txid,
		Nonce:       NewNonce(nonceU),
		From:        from,
		To:          to,
		RCAddr:      rc,
		ValueOrNull: val,
		BlockTime:   ts,
	}, nil
}

/* ===========================================================
   B) RawTransaction  → mmap 레코드
   =========================================================== */

func EncodeRecordFromRaw(x RawTransaction) ([]byte, error) {
	rec := make([]byte, RecSize)

	// TxId (hash 32B)
	if x.TxId != "" {
		if err := writeHexFixed(rec[OffHash:OffHash+32], x.TxId, 32); err != nil {
			return nil, fmt.Errorf("txid: %w", err)
		}
	} else {
		zero(rec[OffHash : OffHash+32])
	}

	// nonce
	if x.Nonce.Valid {
		binary.BigEndian.PutUint64(rec[OffNonce:OffNonce+8], x.Nonce.Value)
	} else {
		zero(rec[OffNonce : OffNonce+8])
	}

	// from / to / rcaddr
	if x.From != "" {
		if err := writeHexFixed(rec[OffFrom:OffFrom+20], x.From, 20); err != nil {
			return nil, fmt.Errorf("from: %w", err)
		}
	} else {
		zero(rec[OffFrom : OffFrom+20])
	}
	if x.To != "" {
		if err := writeHexFixed(rec[OffTo:OffTo+20], x.To, 20); err != nil {
			return nil, fmt.Errorf("to: %w", err)
		}
	} else {
		zero(rec[OffTo : OffTo+20])
	}
	if x.RCAddr != "" {
		if err := writeHexFixed(rec[OffRC:OffRC+20], x.RCAddr, 20); err != nil {
			return nil, fmt.Errorf("rcaddr: %w", err)
		}
	} else {
		zero(rec[OffRC : OffRC+20])
	}

	// value (decimal → u256)
	u, err := decimalToU256(x.ValueOrNull)
	if err != nil {
		return nil, fmt.Errorf("value: %w", err)
	}
	copy(rec[OffVal:OffVal+32], u[:])

	// BlockTime → micros
	if x.BlockTime != "" {
		t, err := time.Parse(time.RFC3339, x.BlockTime)
		if err != nil {
			return nil, fmt.Errorf("blockTime parse (RFC3339): %w", err)
		}
		binary.BigEndian.PutUint64(rec[OffTS:OffTS+8], uint64(t.UTC().UnixMicro()))
	} else {
		zero(rec[OffTS : OffTS+8])
	}

	// block_number / tx_index / pad
	zero(rec[OffBN : OffBN+8])
	zero(rec[OffTXI : OffTXI+4])
	zero(rec[OffPad : OffPad+8])
	return rec, nil
}

/* ===========================================================
   C) “BQ에서 읽은 원시 값” → RawTransaction
   - 외부 패키지 타입(NullString 등) 의존 피하기 위해
     포인터 문자열로 NULL 표현
   =========================================================== */

type BQParts struct {
	Hash        string    // "0x..." 32바이트 해시
	Nonce       *int64    // nil 가능 (무효)
	From        string    // "0x..." 20바이트 주소
	To          *string   // nil => NULL
	ValueStrWei *string   // nil => NULL, 문자열 정수(wei)
	TS          time.Time // block_timestamp (UTC)
	RCAddr      *string   // nil => NULL
}

func RawFromBQParts(p BQParts, timeLayout string) (RawTransaction, error) {
	// nonce
	n := Nonce{}.Null()
	if p.Nonce != nil && *p.Nonce >= 0 {
		n = NewNonce(uint64(*p.Nonce))
	}

	// to / rc 처리
	to := ""
	if p.To != nil && *p.To != "" {
		to = strings.ToLower(*p.To)
	}
	rc := ""
	if p.RCAddr != nil && *p.RCAddr != "" {
		rc = strings.ToLower(*p.RCAddr)
	}

	// value
	val := decimal.Zero
	if p.ValueStrWei != nil && *p.ValueStrWei != "" {
		d, err := decimal.NewFromString(*p.ValueStrWei)
		if err != nil {
			return RawTransaction{}, fmt.Errorf("value parse: %w", err)
		}
		val = d
	}

	if timeLayout == "" {
		timeLayout = time.RFC3339
	}
	return RawTransaction{
		TxId:        strings.ToLower(p.Hash),
		Nonce:       n,
		From:        strings.ToLower(p.From),
		To:          to,
		RCAddr:      rc,
		ValueOrNull: val,
		BlockTime:   p.TS.UTC().Format(timeLayout),
	}, nil
}

/* ===========================================================
   D) “BQ에서 읽은 원시 값” → mmap 레코드
   =========================================================== */

func EncodeRecordFromBQParts(p BQParts) ([]byte, error) {
	rec := make([]byte, RecSize)

	// hash
	if err := writeHexFixed(rec[OffHash:OffHash+32], p.Hash, 32); err != nil {
		return nil, fmt.Errorf("hash: %w", err)
	}

	// nonce
	if p.Nonce != nil && *p.Nonce >= 0 {
		binary.BigEndian.PutUint64(rec[OffNonce:OffNonce+8], uint64(*p.Nonce))
	} else {
		zero(rec[OffNonce : OffNonce+8])
	}

	// from
	if err := writeHexFixed(rec[OffFrom:OffFrom+20], p.From, 20); err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}

	// to
	if p.To != nil && *p.To != "" {
		if err := writeHexFixed(rec[OffTo:OffTo+20], *p.To, 20); err != nil {
			return nil, fmt.Errorf("to: %w", err)
		}
	} else {
		zero(rec[OffTo : OffTo+20])
	}

	// rcaddr
	if p.RCAddr != nil && *p.RCAddr != "" {
		if err := writeHexFixed(rec[OffRC:OffRC+20], *p.RCAddr, 20); err != nil {
			return nil, fmt.Errorf("rcaddr: %w", err)
		}
	} else {
		zero(rec[OffRC : OffRC+20])
	}

	// value
	if p.ValueStrWei != nil && *p.ValueStrWei != "" {
		d, err := decimal.NewFromString(*p.ValueStrWei)
		if err != nil {
			return nil, fmt.Errorf("value parse: %w", err)
		}
		u, err := decimalToU256(d)
		if err != nil {
			return nil, fmt.Errorf("value to u256: %w", err)
		}
		copy(rec[OffVal:OffVal+32], u[:])
	} else {
		zero(rec[OffVal : OffVal+32])
	}

	// timestamp (micros)
	binary.BigEndian.PutUint64(rec[OffTS:OffTS+8], uint64(p.TS.UTC().UnixMicro()))

	// 나머지 0
	zero(rec[OffBN : OffBN+8])
	zero(rec[OffTXI : OffTXI+4])
	zero(rec[OffPad : OffPad+8])

	return rec, nil
}
