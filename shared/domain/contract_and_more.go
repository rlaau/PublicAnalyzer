package domain

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"

//TODO 추후 이 컨트렉트는 CID기반으로 전환될 것

// Contract는 "모든 컨트렉트가 지니는" "가장 일반적인 정보들"을 표현
type Contract struct {
	//키
	Address Address
	//생성 정보
	CreatorOrZero    Address
	CreationTxOrZero TxId
	CreatedAt        chaintimer.ChainTime
	//소유 정보
	OwnerOrZero    Address
	OwnerLogOrZero []Address
	//메타 정보
	IsERC20  bool
	IsERC721 bool
}

type Token struct {
	//키
	Address Address
	//토큰 메타
	Symbol         string
	Name           string
	DecimalsOrZero int64
}

type TokenScore struct {
	//키
	Address Address
	//점수
	AnyScore1 int64
}
type TokenRunnable struct{}

type TokenResult struct {
	Remarks string
}
type NFT struct{}

type ContractKind int

const (
	UnsortedContract ContractKind = iota
	ProtocolContract
	ProxyContract
)

type OtherContract struct{}
