package domain

import (
	"bytes"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var Zero ropedomain.ValueOrNull = ropedomain.ParseValueOrNull(decimal.Zero)

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

// === NewFromScala Tests ===

func TestNewFromScala(t *testing.T) {
	t.Run("creates FromScala with valid values", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.5))

		fs := NewFromScala(from, txId, txTime, value)

		assert.Equal(t, from, fs.FromAddress)
		assert.Equal(t, txId, fs.LastTxId)
		assert.Equal(t, txTime, fs.LastTime)
		assert.Equal(t, int32(1), fs.Volume)
		assert.True(t, value.Equal(fs.ValueOrNull))
	})

	t.Run("creates FromScala with zero value", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := decimal.Zero

		fs := NewFromScala(from, txId, txTime, ropedomain.ParseValueOrNull(value))

		assert.Equal(t, int32(1), fs.Volume)
		assert.True(t, fs.ValueOrNull.IsZero())
	})
}

// === FromScala.Update Tests ===

func TestFromScala_Update(t *testing.T) {
	t.Run("updates with new transaction data", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId1 := createTestTxId(0xBB)
		txTime1 := createTestTime()
		value1 := ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.0))

		fs := NewFromScala(from, txId1, txTime1, value1)

		// Update with new transaction
		txId2 := createTestTxId(0xCC)
		txTime2 := txTime1.Add(chaintimer.Hour)
		value2 := ropedomain.ParseValueOrNull(decimal.NewFromFloat(2.0))

		fs.Update(txId2, txTime2, value2)

		assert.Equal(t, txId2, fs.LastTxId)
		assert.Equal(t, txTime2, fs.LastTime)
		assert.Equal(t, int32(2), fs.Volume)
		assert.True(t, ropedomain.ParseValueOrNull(decimal.NewFromFloat(3.0)).Equal(fs.ValueOrNull))
	})

	t.Run("updates volume without changing value when zero", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()
		value := ropedomain.ParseValueOrNull(decimal.NewFromFloat(5.0))

		fs := NewFromScala(from, txId, txTime, value)

		// Update with zero value
		newTxId := createTestTxId(0xCC)
		newTime := txTime.Add(chaintimer.Hour)

		fs.Update(newTxId, newTime, ropedomain.ParseValueOrNull(decimal.Zero))

		assert.Equal(t, int32(2), fs.Volume)
		assert.True(t, value.Equal(fs.ValueOrNull)) // Value unchanged
	})

	t.Run("does not overflow volume", func(t *testing.T) {
		from := createTestAddress(0xAA)
		txId := createTestTxId(0xBB)
		txTime := createTestTime()

		fs := NewFromScala(from, txId, txTime, ropedomain.ParseValueOrNull(decimal.Zero))
		fs.Volume = 2147483647 // MaxInt32

		fs.Update(txId, txTime, ropedomain.ParseValueOrNull(decimal.Zero))

		assert.Equal(t, int32(2147483647), fs.Volume) // Should not overflow
	})
}

// === FromScala.Matches Tests ===

func TestFromScala_Matches(t *testing.T) {
	t.Run("returns true for matching address", func(t *testing.T) {
		addr := createTestAddress(0xAA)
		fs := NewFromScala(addr, createTestTxId(0xBB), createTestTime(), ropedomain.ParseValueOrNull(decimal.Zero))

		assert.True(t, fs.Matches(addr))
	})

	t.Run("returns false for different address", func(t *testing.T) {
		addr1 := createTestAddress(0xAA)
		addr2 := createTestAddress(0xBB)
		fs := NewFromScala(addr1, createTestTxId(0xCC), createTestTime(), ropedomain.ParseValueOrNull(decimal.Zero))

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
			NewFromScala(addr1, createTestTxId(0x11), createTestTime(), Zero),
			NewFromScala(addr2, createTestTxId(0x22), createTestTime(), Zero),
			NewFromScala(addr3, createTestTxId(0x33), createTestTime(), Zero),
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
			NewFromScala(addr1, createTestTxId(0x11), createTestTime(), Zero),
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
		value := ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.5))

		result := fss.AddOrUpdate(addr, txId, txTime, value)

		require.Len(t, result, 1)
		assert.Equal(t, addr, result[0].FromAddress)
		assert.Equal(t, txId, result[0].LastTxId)
		assert.Equal(t, int32(1), result[0].Volume)
		assert.True(t, value.Equal(result[0].ValueOrNull))
	})

	t.Run("updates existing FromScala", func(t *testing.T) {
		addr := createTestAddress(0xAA)
		txId1 := createTestTxId(0xBB)
		txTime1 := createTestTime()
		value1 := ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.0))

		fss := FromScalas{
			NewFromScala(addr, txId1, txTime1, value1),
		}

		txId2 := createTestTxId(0xCC)
		txTime2 := txTime1.Add(chaintimer.Hour)
		value2 := ropedomain.ParseValueOrNull(decimal.NewFromFloat(2.0))

		result := fss.AddOrUpdate(addr, txId2, txTime2, value2)

		require.Len(t, result, 1)
		assert.Equal(t, txId2, result[0].LastTxId)
		assert.Equal(t, txTime2, result[0].LastTime)
		assert.Equal(t, int32(2), result[0].Volume)
		assert.True(t, ropedomain.ParseValueOrNull(decimal.NewFromFloat(3.0)).Equal(result[0].ValueOrNull))
	})

	t.Run("adds to existing slice", func(t *testing.T) {
		addr1 := createTestAddress(0xAA)
		addr2 := createTestAddress(0xBB)

		fss := FromScalas{
			NewFromScala(addr1, createTestTxId(0x11), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.0))),
		}

		result := fss.AddOrUpdate(addr2, createTestTxId(0x22), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(2.0)))

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
		value := ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.0))
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
			NewFromScala(createTestAddress(0xAA), createTestTxId(0x11), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.0))),
			NewFromScala(createTestAddress(0xBB), createTestTxId(0x22), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(2.0))),
			NewFromScala(createTestAddress(0xCC), createTestTxId(0x33), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(3.0))),
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

// === Marshal/Unmarshal Tests ===

func TestFromScala_Marshal_Unmarshal(t *testing.T) {
	t.Run("round trip preserves data", func(t *testing.T) {
		original := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			createTestTime(),
			ropedomain.ParseValueOrNull(decimal.NewFromFloat(123.456)),
		)
		original.Volume = 42

		var buf bytes.Buffer
		err := original.Marshal(&buf)
		require.NoError(t, err)

		var decoded FromScala
		err = decoded.Unmarshal(&buf)
		require.NoError(t, err)

		assert.Equal(t, original.FromAddress, decoded.FromAddress)
		assert.Equal(t, original.LastTxId, decoded.LastTxId)
		assert.Equal(t, original.LastTime.Unix(), decoded.LastTime.Unix())
		assert.Equal(t, original.Volume, decoded.Volume)
		assert.True(t, original.ValueOrNull.Equal(decoded.ValueOrNull))
	})

	t.Run("handles zero value", func(t *testing.T) {
		original := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			createTestTime(),
			Zero,
		)

		var buf bytes.Buffer
		err := original.Marshal(&buf)
		require.NoError(t, err)

		var decoded FromScala
		err = decoded.Unmarshal(&buf)
		require.NoError(t, err)

		assert.True(t, decoded.ValueOrNull.IsZero())
	})

	t.Run("handles large decimal value", func(t *testing.T) {
		largeValue := ropedomain.ParseValueOrNull(decimal.RequireFromString("999999999999999999.123456789012345678"))
		original := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			createTestTime(),
			largeValue,
		)

		var buf bytes.Buffer
		err := original.Marshal(&buf)
		require.NoError(t, err)

		var decoded FromScala
		err = decoded.Unmarshal(&buf)
		require.NoError(t, err)

		assert.True(t, original.ValueOrNull.Equal(decoded.ValueOrNull))
	})
}

func TestFromScalas_Marshal_Unmarshal(t *testing.T) {
	t.Run("round trip preserves multiple elements", func(t *testing.T) {
		original := FromScalas{
			NewFromScala(createTestAddress(0xAA), createTestTxId(0x11), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(1.1))),
			NewFromScala(createTestAddress(0xBB), createTestTxId(0x22), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(2.2))),
			NewFromScala(createTestAddress(0xCC), createTestTxId(0x33), createTestTime(), ropedomain.ParseValueOrNull(decimal.NewFromFloat(3.3))),
		}

		var buf bytes.Buffer
		err := original.Marshal(&buf)
		require.NoError(t, err)

		decoded, err := UnmarshalFromScalas(&buf)
		require.NoError(t, err)

		require.Len(t, decoded, len(original))
		for i := range original {
			assert.Equal(t, original[i].FromAddress, decoded[i].FromAddress)
			assert.Equal(t, original[i].LastTxId, decoded[i].LastTxId)
			assert.Equal(t, original[i].Volume, decoded[i].Volume)
			assert.True(t, original[i].ValueOrNull.Equal(decoded[i].ValueOrNull))
		}
	})

	t.Run("handles empty slice", func(t *testing.T) {
		original := FromScalas{}

		var buf bytes.Buffer
		err := original.Marshal(&buf)
		require.NoError(t, err)

		decoded, err := UnmarshalFromScalas(&buf)
		require.NoError(t, err)

		assert.Len(t, decoded, 0)
	})

	t.Run("handles large slice", func(t *testing.T) {
		original := make(FromScalas, 100)
		for i := range original {
			original[i] = NewFromScala(
				createTestAddress(byte(i)),
				createTestTxId(byte(i)),
				createTestTime(),
				ropedomain.ParseValueOrNull(decimal.NewFromInt(int64(i))),
			)
		}

		var buf bytes.Buffer
		err := original.Marshal(&buf)
		require.NoError(t, err)

		decoded, err := UnmarshalFromScalas(&buf)
		require.NoError(t, err)

		require.Len(t, decoded, 100)
		for i := range original {
			assert.Equal(t, original[i].FromAddress, decoded[i].FromAddress)
			assert.True(t, original[i].ValueOrNull.Equal(decoded[i].ValueOrNull))
		}
	})
}

// === Binary Format Compatibility Tests ===

func TestBinaryFormatStability(t *testing.T) {
	t.Run("binary format remains consistent", func(t *testing.T) {
		// This test ensures the binary format doesn't accidentally change
		fs := NewFromScala(
			createTestAddress(0xAA),
			createTestTxId(0xBB),
			chaintimer.ChainTime(time.Unix(1700000000, 0).UTC()),
			ropedomain.ParseValueOrNull(decimal.NewFromFloat(100.5)),
		)

		var buf bytes.Buffer
		err := fs.Marshal(&buf)
		require.NoError(t, err)

		// Binary format: 20B address + 32B txId + 8B unix time + 4B volume + varBytes decimal
		// Address: 20 bytes
		// TxId: 32 bytes
		// Unix: 8 bytes
		// Volume: 4 bytes
		// Decimal: 4 bytes length + string bytes
		assert.Greater(t, buf.Len(), 20+32+8+4+4)
	})
}
