package domain

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ZeroOption fp.Option[ropedomain.Value] = ropedomain.ParseValueOption(decimal.Zero)

// === Test Helpers ===

func createTestAddress(b byte) domain.Address {
	var addr domain.Address
	for i := range addr {
		addr[i] = b
	}
	return addr
}

func createTestTxId(b byte) domain.TxId {
	var txId domain.TxId
	for i := range txId {
		txId[i] = b
	}
	return txId
}

func createTestTime() chaintimer.ChainTime {
	return chaintimer.ChainTime(time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC))
}

func TestFromScala_JSON_Marshal_Unmarshal(t *testing.T) {
	t.Run("round trip preserves data (Some non-zero decimal)", func(t *testing.T) {
		original := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			createTestTime(),
			ropedomain.ParseValueOption(decimal.NewFromFloat(123.456)),
		)
		original.Volume = 42

		var buf bytes.Buffer
		require.NoError(t, json.NewEncoder(&buf).Encode(original))

		var decoded FromScala
		require.NoError(t, json.NewDecoder(&buf).Decode(&decoded))

		assert.Equal(t, original.FromAddress, decoded.FromAddress)
		assert.Equal(t, original.LastTxId, decoded.LastTxId)
		assert.Equal(t, original.LastTime.Unix(), decoded.LastTime.Unix())
		assert.Equal(t, original.Volume, decoded.Volume)
		assert.True(t, original.ValueOrNull.V.Equal(decoded.ValueOrNull.V))
	})

	t.Run("Some(0) vs None(null) are different (ZeroOption emits \"0\")", func(t *testing.T) {
		// Some(0) 케이스
		withZero := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			createTestTime(),
			ZeroOption, // == Some(decimal.Zero)
		)

		b1, err := json.Marshal(withZero)
		require.NoError(t, err)
		assert.Contains(t, string(b1), `"valueOrNull":"0"`, "ZeroOption은 Some(0)이므로 null이 아니라 \"0\" 문자열로 직렬화되어야 함")

		var round1 FromScala
		require.NoError(t, json.Unmarshal(b1, &round1))
		assert.True(t, round1.ValueOrNull.IsSome())
		assert.True(t, round1.ValueOrNull.V.Equal(decimal.Zero))

		// None(null) 케이스
		withNone := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			createTestTime(),
			fp.None[ropedomain.Value](), // 진짜 None
		)

		b2, err := json.Marshal(withNone)
		require.NoError(t, err)
		assert.Contains(t, string(b2), `"valueOrNull":null`, "None은 반드시 null로 직렬화되어야 함")

		var round2 FromScala
		require.NoError(t, json.Unmarshal(b2, &round2))
		_, ok := round2.ValueOrNull.Value()
		assert.False(t, ok, "역직렬화 후에도 Ok=false(=None) 이어야 함")
	})

	t.Run("handles large decimal value", func(t *testing.T) {
		large := decimal.RequireFromString("999999999999999999.123456789012345678")
		original := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			createTestTime(),
			ropedomain.ParseValueOption(large),
		)

		var buf bytes.Buffer
		require.NoError(t, json.NewEncoder(&buf).Encode(original))

		var decoded FromScala
		require.NoError(t, json.NewDecoder(&buf).Decode(&decoded))

		assert.True(t, original.ValueOrNull.V.Equal(decoded.ValueOrNull.V))
	})
}

func TestFromScalas_JSON_Marshal_Unmarshal(t *testing.T) {
	t.Run("round trip preserves multiple elements", func(t *testing.T) {
		original := FromScalas{
			NewFromScala(createTestAddress(0xAA), createTestTxId(0x11), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(1.1))),
			NewFromScala(createTestAddress(0xBB), createTestTxId(0x22), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(2.2))),
			NewFromScala(createTestAddress(0xCC), createTestTxId(0x33), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(3.3))),
		}

		var buf bytes.Buffer
		require.NoError(t, json.NewEncoder(&buf).Encode(original))

		var decoded FromScalas
		require.NoError(t, json.NewDecoder(&buf).Decode(&decoded))

		require.Len(t, decoded, len(original))
		for i := range original {
			assert.Equal(t, original[i].FromAddress, decoded[i].FromAddress)
			assert.Equal(t, original[i].LastTxId, decoded[i].LastTxId)
			assert.Equal(t, original[i].Volume, decoded[i].Volume)
			assert.True(t, original[i].ValueOrNull.V.Equal(decoded[i].ValueOrNull.V))
		}
	})

	t.Run("handles empty slice", func(t *testing.T) {
		original := FromScalas{}

		var buf bytes.Buffer
		require.NoError(t, json.NewEncoder(&buf).Encode(original))

		var decoded FromScalas
		require.NoError(t, json.NewDecoder(&buf).Decode(&decoded))

		assert.Len(t, decoded, 0)
	})

	t.Run("handles large slice", func(t *testing.T) {
		original := make(FromScalas, 100)
		for i := range original {
			original[i] = NewFromScala(
				createTestAddress(byte(i)),
				createTestTxId(byte(i)),
				createTestTime(),
				ropedomain.ParseValueOption(decimal.NewFromInt(int64(i))),
			)
		}

		var buf bytes.Buffer
		require.NoError(t, json.NewEncoder(&buf).Encode(original))

		var decoded FromScalas
		require.NoError(t, json.NewDecoder(&buf).Decode(&decoded))

		require.Len(t, decoded, 100)
		for i := range original {
			assert.Equal(t, original[i].FromAddress, decoded[i].FromAddress)
			assert.True(t, original[i].ValueOrNull.V.Equal(decoded[i].ValueOrNull.V))
		}
	})
}

// (선택) JSON 모양을 약하게 고정해두는 스키마 안정성 테스트
func TestJSONShape_WeakContracts(t *testing.T) {
	fs := NewFromScala(
		createTestAddress(0xAA),
		createTestTxId(0xBB),
		chaintimer.ChainTime(time.Unix(1700000000, 0).UTC()),
		ropedomain.ParseValueOption(decimal.NewFromFloat(100.5)),
	)

	b, err := json.Marshal(fs)
	require.NoError(t, err)
	js := string(b)

	// 존재해야 하는 키
	assert.Contains(t, js, `"fromAddress"`)
	assert.Contains(t, js, `"lastTxId"`)
	assert.Contains(t, js, `"lastTime"`)
	assert.Contains(t, js, `"volume"`)
	assert.Contains(t, js, `"valueOrNull"`)

	// Some일 때 문자열 또는 숫자(정책에 따라)로 직렬화되지만 shopspring/decimal은 문자열이 기본
	assert.Contains(t, js, `"valueOrNull":"`)
}

// === NewFromScala Tests ===

func TestNewFromScala(t *testing.T) {
	t.Run("creates FromScala with valid values", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := ropedomain.ParseValueOption(decimal.NewFromFloat(1.5))

		fs := NewFromScala(from, txId, txTime, value)

		assert.Equal(t, from, fs.FromAddress)
		assert.Equal(t, txId, fs.LastTxId)
		assert.Equal(t, txTime, fs.LastTime)
		assert.Equal(t, int32(1), fs.Volume)
		assert.True(t, value.V.Equal(fs.ValueOrNull.V))
	})

	t.Run("creates FromScala with zero value", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := decimal.Zero

		fs := NewFromScala(from, txId, txTime, ropedomain.ParseValueOption(value))

		assert.Equal(t, int32(1), fs.Volume)
		assert.True(t, fs.ValueOrNull.V.IsZero())
	})
}

// === FromScala.Update Tests ===

func TestFromScala_Update(t *testing.T) {
	t.Run("updates with new transaction data", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId1 := createTestTxId(0xBB)
		txTime1 := createTestTime()
		value1 := ropedomain.ParseValueOption(decimal.NewFromFloat(1.0))

		fs := NewFromScala(from, txId1, txTime1, value1)

		// Update with new transaction
		txId2 := createTestTxId(0xCC)
		txTime2 := txTime1.Add(chaintimer.Hour)
		value2 := ropedomain.ParseValueOption(decimal.NewFromFloat(2.0))

		fs.Update(txId2, txTime2, value2)

		assert.Equal(t, txId2, fs.LastTxId)
		assert.Equal(t, txTime2, fs.LastTime)
		assert.Equal(t, int32(2), fs.Volume)
		assert.True(t, ropedomain.ParseValueOption(decimal.NewFromFloat(3.0)).V.Equal(fs.ValueOrNull.V))
	})

	t.Run("updates volume without changing value when zero", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := ropedomain.ParseValueOption(decimal.NewFromFloat(5.0))

		fs := NewFromScala(from, txId, txTime, value)

		// Update with zero value
		newTxId := createTestTxId(0xCC)
		newTime := txTime.Add(chaintimer.Hour)

		fs.Update(newTxId, newTime, ropedomain.ParseValueOption(decimal.Zero))

		assert.Equal(t, int32(2), fs.Volume)
		assert.True(t, value.V.Equal(fs.ValueOrNull.V)) // Value unchanged
	})

	t.Run("does not overflow volume", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()

		fs := NewFromScala(from, txId, txTime, ropedomain.ParseValueOption(decimal.Zero))
		fs.Volume = 2147483647 // MaxInt32

		fs.Update(txId, txTime, ropedomain.ParseValueOption(decimal.Zero))

		assert.Equal(t, int32(2147483647), fs.Volume) // Should not overflow
	})
}

// === FromScala.Matches Tests ===

func TestFromScala_Matches(t *testing.T) {
	t.Run("returns true for matching address", func(t *testing.T) {
		addr := createTestAddress(0xAA)
		fs := NewFromScala(addr, createTestTxId(0xBB), createTestTime(), ropedomain.ParseValueOption(decimal.Zero))

		assert.True(t, fs.Matches(addr))
	})

	t.Run("returns false for different address", func(t *testing.T) {
		addr1 := createTestAddress(0xAA)
		addr2 := createTestAddress(0xBB)
		fs := NewFromScala(addr1, createTestTxId(0xCC), createTestTime(), ropedomain.ParseValueOption(decimal.Zero))

		assert.False(t, fs.Matches(addr2))
	})
}

// === FromScalas.FindByAddress Tests ===

func TestFromScalas_FindByAddress(t *testing.T) {
	t.Run("finds existing address", func(t *testing.T) {
		addr1 := createTestAddress(0xAA)
		addr2 := createTestAddress(0xBB)
		addr3 := createTestAddress(0xCC)
		fss := FromScalas{
			NewFromScala(addr1, createTestTxId(0x11), createTestTime(), ZeroOption),
			NewFromScala(addr2, createTestTxId(0x22), createTestTime(), ZeroOption),
			NewFromScala(addr3, createTestTxId(0x33), createTestTime(), ZeroOption),
		}

		idx, found := fss.FindByAddress(addr2)

		assert.True(t, found)
		assert.Equal(t, 1, idx)
		assert.Equal(t, addr2, fss[idx].FromAddress)
	})

	t.Run("returns not found for missing address", func(t *testing.T) {
		addr1 := createTestAddress(0xAA)
		addr2 := createTestAddress(0xBB)

		fss := FromScalas{
			NewFromScala(addr1, createTestTxId(0x11), createTestTime(), ZeroOption),
		}

		idx, found := fss.FindByAddress(addr2)

		assert.False(t, found)
		assert.Equal(t, -1, idx)
	})

	t.Run("handles empty slice", func(t *testing.T) {
		fss := FromScalas{}
		addr := createTestAddress(0xAA)

		idx, found := fss.FindByAddress(addr)

		assert.False(t, found)
		assert.Equal(t, -1, idx)
	})
}

// === FromScalas.AddOrUpdate Tests ===

func TestFromScalas_AddOrUpdate(t *testing.T) {
	t.Run("adds new FromScala when not exists", func(t *testing.T) {
		fss := FromScalas{}
		addr := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := ropedomain.ParseValueOption(decimal.NewFromFloat(1.5))

		result := fss.AddOrUpdate(addr, txId, txTime, value)

		require.Len(t, result, 1)
		assert.Equal(t, addr, result[0].FromAddress)
		assert.Equal(t, txId, result[0].LastTxId)
		assert.Equal(t, int32(1), result[0].Volume)
		assert.True(t, value.V.Equal(result[0].ValueOrNull.V))
	})

	t.Run("updates existing FromScala", func(t *testing.T) {
		addr := createTestAddress(0xAA)
		txId1 := createTestTxId(0xBB)
		txTime1 := createTestTime()
		value1 := ropedomain.ParseValueOption(decimal.NewFromFloat(1.0))

		fss := FromScalas{
			NewFromScala(addr, txId1, txTime1, value1),
		}

		txId2 := createTestTxId(0xCC)
		txTime2 := txTime1.Add(chaintimer.Hour)
		value2 := ropedomain.ParseValueOption(decimal.NewFromFloat(2.0))

		result := fss.AddOrUpdate(addr, txId2, txTime2, value2)

		require.Len(t, result, 1)
		assert.Equal(t, txId2, result[0].LastTxId)
		assert.Equal(t, txTime2, result[0].LastTime)
		assert.Equal(t, int32(2), result[0].Volume)
		assert.True(t, ropedomain.ParseValueOption(decimal.NewFromFloat(3.0)).V.Equal(result[0].ValueOrNull.V))
	})

	t.Run("adds to existing slice", func(t *testing.T) {
		addr1 := createTestAddress(0xAA)
		addr2 := createTestAddress(0xBB)

		fss := FromScalas{
			NewFromScala(addr1, createTestTxId(0x11), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(1.0))),
		}

		result := fss.AddOrUpdate(addr2, createTestTxId(0x22), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(2.0)))

		require.Len(t, result, 2)
		assert.Equal(t, addr1, result[0].FromAddress)
		assert.Equal(t, addr2, result[1].FromAddress)
	})
}

// === FromScalas.Copy Tests ===

func TestFromScalas_Copy(t *testing.T) {
	t.Run("creates independent copy", func(t *testing.T) {
		addr := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := ropedomain.ParseValueOption(decimal.NewFromFloat(1.0))
		original := FromScalas{
			NewFromScala(addr, txId, txTime, value),
		}

		copied := original.Copy()

		// Modify copy
		copied[0].Volume = 999

		// Original should be unchanged
		assert.Equal(t, int32(1), original[0].Volume)
		assert.Equal(t, int32(999), copied[0].Volume)
	})

	t.Run("handles empty slice", func(t *testing.T) {
		original := FromScalas{}
		copied := original.Copy()

		assert.NotNil(t, copied)
		assert.Len(t, copied, 0)
	})

	t.Run("copies multiple elements", func(t *testing.T) {
		fss := FromScalas{
			NewFromScala(createTestAddress(0xAA), createTestTxId(0x11), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(1.0))),
			NewFromScala(createTestAddress(0xBB), createTestTxId(0x22), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(2.0))),
			NewFromScala(createTestAddress(0xCC), createTestTxId(0x33), createTestTime(), ropedomain.ParseValueOption(decimal.NewFromFloat(3.0))),
		}

		copied := fss.Copy()

		require.Len(t, copied, 3)
		for i := range fss {
			assert.Equal(t, fss[i].FromAddress, copied[i].FromAddress)
			assert.Equal(t, fss[i].LastTxId, copied[i].LastTxId)
			assert.Equal(t, fss[i].Volume, copied[i].Volume)
		}
	})
}
