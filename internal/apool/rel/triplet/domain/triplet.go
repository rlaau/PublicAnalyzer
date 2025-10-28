package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/dto"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type CEX domain.Address
type Deposit domain.Address
type User domain.Address

func BuildDepositDetection(cex CEX, deposit Deposit, t chaintimer.ChainTime) domain.DetectedDeposit {
	return domain.DetectedDeposit{
		CEXAddress:     domain.Address(cex),
		DepositAddress: domain.Address(deposit),
		TxCount:        1,
		DetectedAt:     t,
	}
}
func BuildCexAndDepositEvent(cex CEX, deposit Deposit, tx domain.MarkedTransaction) ropedomain.TraitEvent {
	trait := dto.TraitCexAndDeposit
	cexAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(cex),
		Rule:    dto.RuleCex,
	}
	depositAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    dto.RuleDeposit,
	}
	txScala := ropedomain.TxScala{
		TxId:        tx.TxID,
		Time:        tx.BlockTime,
		ScoreInc:    1,
		ValueOrNull: tx.ValueOrNull,
	}
	return ropedomain.NewTraitEvent(
		dto.AuthorSystem,
		trait,
		cexAndRule,
		depositAndRule,
		txScala,
	)
}

func BuildDualEventWithFromScala(deposit Deposit, user User, fromScala FromScala) ropedomain.TraitEvent {
	trait := dto.TraitDepositAndUser
	depositAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    dto.RuleDeposit,
	}
	userAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(user),
		Rule:    dto.RuleUser,
	}
	txScala := ropedomain.TxScala{
		TxId:        fromScala.LastTxId,
		Time:        fromScala.LastTime,
		ScoreInc:    fromScala.Volume,
		ValueOrNull: fromScala.ValueOrNull,
	}
	return ropedomain.NewTraitEvent(
		dto.AuthorSystem,
		trait,
		depositAndRule,
		userAndRule,
		txScala,
	)
}

func BuildDualEventWithTx(deposit Deposit, user User, tx domain.MarkedTransaction) ropedomain.TraitEvent {
	trait := dto.TraitDepositAndUser
	depositRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    dto.RuleDeposit,
	}
	userAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(user),
		Rule:    dto.RuleUser,
	}
	txScala := ropedomain.TxScala{
		TxId:        tx.TxID,
		Time:        tx.BlockTime,
		ScoreInc:    1,
		ValueOrNull: tx.ValueOrNull,
	}
	return ropedomain.NewTraitEvent(
		dto.AuthorSystem,
		trait,
		depositRule,
		userAndRule,
		txScala,
	)
}
