package domain

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"

type Contract struct {
	Address string `json:"address"`

	// 생성 정보
	Creator      string               `json:"creator,omitempty"`
	CreationTx   string               `json:"creation_tx,omitempty"`
	CreatedAt    chaintimer.ChainTime `json:"created_at,omitempty"`
	CreatedBlock int64                `json:"created_block,omitempty"`

	// 코드/인터페이스
	BytecodeSHA256    string   `json:"bytecode_sha256,omitempty"`
	IsERC20           bool     `json:"is_erc20,omitempty"`
	IsERC721          bool     `json:"is_erc721,omitempty"`
	FunctionSighashes []string `json:"function_sighashes,omitempty"`

	// 토큰 메타
	Symbol   string `json:"symbol,omitempty"`
	Name     string `json:"name,omitempty"`
	Decimals *int64 `json:"decimals,omitempty"`

	// Ownable 등에서 추정된 Owner(있을 때만)
	Owner *string `json:"owner,omitempty"`

	// 관리
	LastSeenAt chaintimer.ChainTime `json:"last_seen_at,omitempty"`
}

// KV 키 규칙
func KeyFor(addr string) []byte { return []byte("contract:" + addr) }
