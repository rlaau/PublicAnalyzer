package domain
// ===== 도메인 메시지 =====
type RawTransaction struct {
	BlockTime string
	TxId      string
	From      string
	To        string
	Nonce     string
}