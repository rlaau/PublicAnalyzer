package infra

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// ! DB 복호화에 쓰이는 상수임. 절대 건들지 말 것
// ! Append는 가능
const (
	TraitDepositAndUser ropedomain.TraitCode = 1
	TraitCexAndDeposit  ropedomain.TraitCode = 2
)

var TraitLegend = map[ropedomain.TraitCode]string{
	TraitDepositAndUser: "TraitDepositAndUser",
	TraitCexAndDeposit:  "TraitCexAndDeposit",
}

const (
	RuleUser    ropedomain.RuleCode = 1
	RuleDeposit ropedomain.RuleCode = 2
	RuleCex     ropedomain.RuleCode = 3
)

var RuleLegend = map[ropedomain.RuleCode]string{
	RuleUser:    "RuleUser",
	RuleDeposit: "RuleDeposit",
	RuleCex:     "RuleCex",
}

var TraitName = map[ropedomain.TraitCode]string{
	TraitDepositAndUser: "DepositAndUser",
	TraitCexAndDeposit:  "CexAndDeposit",
}

// ///////////////////////////////////////////////////////////////////////////
type RawBadgerProvider interface {
	RawBadgerDB() *badger.DB
}

func NewBagerEeGraphDB(mode mode.ProcessingMode) (ropeapp.RopeDB, error) {
	db, err := ropeapp.NewRopeDB(mode, "EeGraphDB", TraitLegend, RuleLegend)
	if err != nil {
		return nil, fmt.Errorf("failed to open rope db")
	}
	return db, nil
}
