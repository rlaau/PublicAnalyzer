package domain

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"

type Contract struct {
	address   Address
	isErc20   bool
	isErc721  bool
	createdAt chaintimer.ChainTime
	creatTx   TxId
}
