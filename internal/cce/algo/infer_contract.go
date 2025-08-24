package algo

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"golang.org/x/crypto/sha3"
)

// TryPredictCreatedContractAddress(from, nonce) with RLP + Legacy Keccak-256
func TryPredictCreatedContractAddress(from domain.Address, nonce domain.Nonce) (domain.Address, bool) {
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
