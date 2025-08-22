package infra

import (
	"fmt"

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
const (
	RuleUser    ropedomain.RuleCode = 1
	RuleDeposit ropedomain.RuleCode = 2
	RuleCex     ropedomain.RuleCode = 3
)

/////////////////////////////////////////////////////////////////////////////

func NewBagerEeGraphDB(mode mode.ProcessingMode) (ropeapp.RopeDB, error) {
	db, err := ropeapp.NewRopeDB(mode, "EeGraphDB")
	if err != nil {
		return nil, fmt.Errorf("failed to open rope db")
	}
	return db, nil
}
