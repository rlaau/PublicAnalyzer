package roperepo

import (
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
)

//!!!
//* 이 파일의 코드 넘버들은 DB의 해석에 쓰임
//* 절대로, 코드 넘버를 삭제하거나 수정하지 말것
//* 오직 추가만 가능함
//!!!
//* 각 관계는 주석을 통해 버전을 명시함
//* 버전 컨트롤은 아직 구현하지 못함.

//TODO 현재는 버전 컨트롤 기능이 없음!!!
//TODO 추후 버전 컨트롤을 자동화 할 것!!

// ! append- only
// 코드의 명시적인 해석이 쉽도록 일부러 iota 안썼음
// iota안쓰면 실수가 날 수 있지 않냐 하는데, 이건 걍 실수를 하면 안됨 적어도 지금은
// TODO 추후 버전 컨트롤을 만들 것
const (
	// * Triplet의 트레이트
	TraitDepositAndUser ropedomain.TraitCode = 1
	TraitCexAndDeposit  ropedomain.TraitCode = 2
	//* Creation의 트레이트
	TraitCreatorAndCreatedContract ropedomain.TraitCode = 3
)

// 트레이트의 해석 방법.
// *이 부분의 문자열 값은 바꾸기 가능. 의미만 잘 전달되면 됨
var TraitLegend = map[ropedomain.TraitCode]string{
	TraitDepositAndUser:            "TraitDepositAndUser",
	TraitCexAndDeposit:             "TraitCexAndDeposit",
	TraitCreatorAndCreatedContract: "TraitCreatorAndCreatedContract",
}

// ! append- only
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

// ! append-only
const (
	// * Triplet의 룰
	RuleUser    ropedomain.RuleCode = 1
	RuleDeposit ropedomain.RuleCode = 2
	RuleCex     ropedomain.RuleCode = 3
	//* Creation의 룰
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
