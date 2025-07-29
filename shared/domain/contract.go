package domain

import "time"

type Contract struct {
	address   Address
	isErc20   bool
	isErc721  bool
	createdAt time.Time
	creatTx   TxId
}
