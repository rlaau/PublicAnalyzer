package domain

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/ct"

type Contract struct {
	address   Address
	isErc20   bool
	isErc721  bool
	createdAt ct.ChainTime
	creatTx   TxId
}
