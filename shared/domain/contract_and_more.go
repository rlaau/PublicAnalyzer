package domain

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"

// 컨트렉트의 게이트웨이
type RawContract struct {
	Address string `json:"address"`

	// 생성 정보
	Creator    *string               `json:"creator,omitempty"`
	CreationTx *string               `json:"creation_tx,omitempty"`
	CreatedAt  *chaintimer.ChainTime `json:"created_at,omitempty"`
	//CreatedBlock int64                `json:"created_block,omitempty"`

	// 코드/인터페이스
	//BytecodeSHA256    string   `json:"bytecode_sha256,omitempty"`
	IsERC20  bool `json:"is_erc20,omitempty"`
	IsERC721 bool `json:"is_erc721,omitempty"`
	//FunctionSighashes []string `json:"function_sighashes,omitempty"`
	// Ownable 등에서 추정된 Owner(있을 때만)
	Owner    *string   `json:"owner,omitempty"`
	OwnerLog *[]string `json:"ownerlog,omitempty"`

	// 관리
	LastSeenAt chaintimer.ChainTime `json:"last_seen_at,omitempty"`
}

// 컨트렉트 중 토큰인 것
type RawToken struct {
	Address string `json:"address"`

	// 토큰 메타
	Symbol   string `json:"symbol,omitempty"`
	Name     string `json:"name,omitempty"`
	Decimals *int64 `json:"decimals,omitempty"`
}

// 컨트렉트 중 프로토콜 인 것
type RawProtocol struct {
	Address string `json:"address"`
}

type MarkedContract struct {
	//TODO 여기선 ID 매퍼 이용
}

// KV 키 규칙
func KeyFor(addr string) []byte { return []byte("contract:" + addr) }
