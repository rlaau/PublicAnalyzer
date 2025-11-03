package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/protocol"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// NewFromScala creates a FromScala with initial values
func NewFromScala(from domain.Address, txId domain.TxId, txTime chaintimer.ChainTime, value ropedomain.ValueOrNull) FromScala {
	volume := int32(1)
	return FromScala{
		FromAddress: from,
		LastTxId:    txId,
		LastTime:    txTime,
		Volume:      volume,
		ValueOrNull: value,
	}
}
func BuildDepositDetection(cex CEX, deposit Deposit, t chaintimer.ChainTime) domain.DetectedDeposit {
	return domain.DetectedDeposit{
		CEXAddress:     domain.Address(cex),
		DepositAddress: domain.Address(deposit),
		TxCount:        1,
		DetectedAt:     t,
	}
}
func BuildCexAndDepositEvent(cex CEX, deposit Deposit, tx domain.MarkedTransaction) ropedomain.TraitEvent {
	trait := protocol.TraitCexAndDeposit
	cexAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(cex),
		Rule:    protocol.RuleCex,
	}
	depositAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    protocol.RuleDeposit,
	}
	txScala := ropedomain.TxScala{
		TxId:        tx.TxID,
		Time:        tx.BlockTime,
		ScoreInc:    1,
		ValueOrNull: ropedomain.ParseValue(tx.ValueOrNull),
	}
	return ropedomain.NewTraitEvent(
		protocol.AuthorSystem,
		trait,
		cexAndRule,
		depositAndRule,
		txScala,
	)
}

func BuildDualEventWithFromScala(deposit Deposit, user User, fromScala FromScala) ropedomain.TraitEvent {
	trait := protocol.TraitDepositAndUser
	depositAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    protocol.RuleDeposit,
	}
	userAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(user),
		Rule:    protocol.RuleUser,
	}
	txScala := ropedomain.TxScala{
		TxId:        fromScala.LastTxId,
		Time:        fromScala.LastTime,
		ScoreInc:    fromScala.Volume,
		ValueOrNull: fromScala.ValueOrNull,
	}
	return ropedomain.NewTraitEvent(
		protocol.AuthorSystem,
		trait,
		depositAndRule,
		userAndRule,
		txScala,
	)
}

func BuildDualEventWithTx(deposit Deposit, user User, tx domain.MarkedTransaction) ropedomain.TraitEvent {
	trait := protocol.TraitDepositAndUser
	depositRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    protocol.RuleDeposit,
	}
	userAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(user),
		Rule:    protocol.RuleUser,
	}
	txScala := ropedomain.TxScala{
		TxId:        tx.TxID,
		Time:        tx.BlockTime,
		ScoreInc:    1,
		ValueOrNull: ropedomain.ParseValue(tx.ValueOrNull),
	}
	return ropedomain.NewTraitEvent(
		protocol.AuthorSystem,
		trait,
		depositRule,
		userAndRule,
		txScala,
	)
}
