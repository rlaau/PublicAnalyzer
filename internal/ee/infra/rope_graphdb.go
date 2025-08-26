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
	// * EOA - EOA의 트레이트
	TraitDepositAndUser ropedomain.TraitCode = 1
	TraitCexAndDeposit  ropedomain.TraitCode = 2
	//*EOA-CONT의 트레이트
	TraitCreatorAndCreatedContract ropedomain.TraitCode = 3
)

var TraitLegend = map[ropedomain.TraitCode]string{
	TraitDepositAndUser:            "TraitDepositAndUser",
	TraitCexAndDeposit:             "TraitCexAndDeposit",
	TraitCreatorAndCreatedContract: "TraitCreatorAndCreatedContract",
}
var (
	PolyTraitSameCluster ropedomain.PolyTraitCode = 1
)
var PolyTraitLegend = map[ropedomain.PolyTraitCode]ropedomain.PolyNameAndTraits{
	PolyTraitSameCluster: {
		Name: "PolyTraitSameCluster",
		//* CEX_Deposit의 관계나 Creator_Created의 관계는 둘다 "same cluser"의 다형 트레이트에 속함
		Traits: []ropedomain.TraitCode{TraitDepositAndUser, TraitCreatorAndCreatedContract},
	},
}

const (
	// * EOA-EOA의 룰
	RuleUser    ropedomain.RuleCode = 1
	RuleDeposit ropedomain.RuleCode = 2
	RuleCex     ropedomain.RuleCode = 3
	//* EOA-Cont의 룰
	RuleCreator         ropedomain.RuleCode = 4
	RuleCreatedContract ropedomain.RuleCode = 5
)

var RuleLegend = map[ropedomain.RuleCode]string{
	RuleUser:            "RuleUser",
	RuleDeposit:         "RuleDeposit",
	RuleCex:             "RuleCex",
	RuleCreator:         "RuleCreator",
	RuleCreatedContract: "RuleCreatedContract",
}

// ///////////////////////////////////////////////////////////////////////////
type RawBadgerProvider interface {
	RawBadgerDB() *badger.DB
}

func NewBagerEeGraphDB(mode mode.ProcessingMode) (ropeapp.RopeDB, error) {
	db, err := ropeapp.NewRopeDB(mode, "EeGraphDB", TraitLegend, RuleLegend, PolyTraitLegend)
	if err != nil {
		return nil, fmt.Errorf("failed to open rope db")
	}
	return db, nil
}
