package domain

import (
	"encoding/hex"
	"math/big"
	"time"
)

type RawTransaction struct {
	BlockTime time.Time
	TxId      string
	From      string
	To        string
	Value     string
	Gas       string
	Input     string // ABI encoded data (함수 시그니처 및 파라미터)
	Nonce     string // 트랜잭션 nonce (선택적, 필요시 사용)
}

// 트랜잭션 구조체
type MarkedTransaction struct {
	BlockTime   time.Time
	TxID        TxId
	TxSyntax    [2]ContractBoolMark //해당 tx가 C2C, C2C, C2D, D2C, D2D 중 어떤 형태인지 표기
	Nonce       uint64
	BlockNumber BlockNumber
	From        Address
	To          Address
	Value       BigInt
	GasLimit    BigInt
	Input       string
}

type TxId [32]byte

type ContractBoolMark bool

const (
	ContractMark ContractBoolMark = true  // 컨트랙트
	EOAMark      ContractBoolMark = false // EOA (외부 소유 계정)
)

// ✅ 문자열 변환 (0x + hex encoding)
func (t TxId) String() string {
	return "0x" + hex.EncodeToString(t[:])
}

// ✅ BigInt 변환 (MongoDB & BigQuery 호환)
type BigInt struct {
	Int *big.Int
}

// ✅ BigInt 생성자 함수
func NewBigInt(value string) BigInt {
	b := new(big.Int)
	b.SetString(value, 10)
	return BigInt{Int: b}
}

// ✅ 문자열 변환 (MongoDB & BigQuery에서 사용)
func (b BigInt) String() string {
	if b.Int == nil {
		return "0"
	}
	return b.Int.String()
}
func (b *BigInt) SetString(value string, base int) {
	if b.Int == nil {
		b.Int = new(big.Int)
	}
	b.Int.SetString(value, base)
}

// ✅ Cmp 메서드 추가 (비교 연산 지원)
// -1(other가 더 큼), 0, 1(b가 더 큼) 중하나로 부등연산 결과 리턴.
func (b BigInt) Cmp(other BigInt) int {
	if b.Int == nil || other.Int == nil {
		return -1 // 비교할 값이 없으면 작은 값으로 간주
	}
	return b.Int.Cmp(other.Int)
}

// TX 원시 데이터
// {
// 	"hash": "0x...",       // 트랜잭션 해시
// 	"from": "0xSender",    // 발신 주소
// 	"to": "0xRecipient",   // 수신 주소 (EOA or CA)
// 	"value": "0",          // 전송 ETH 양
// 	"gas": 21000,          // 가스 제한
// 	"gasPrice": "50 Gwei", // 가스 가격
// 	"input": "0x..."       // 호출 데이터 (ABI encoded data)(함수 시그니처 및 파라미터)
// Eth전송만 할 시엔 input값이 없음. (물론, 스테이킹 시도 이더 전송 취급이라, 이걸 바탕으로 신택스 확정은 불가)
//   }
