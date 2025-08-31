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
	//* Triplet의 예외 상황 마킹용 트레이트
	//Tx의 from이 cex인 경우. 받은 대상은 특별한 것이라 추정
	TraitCexToOther ropedomain.TraitCode = 3
	//Tx의 from이 Deposit인데, to가 cex가 아닐 경우
	TraitDepositToOther ropedomain.TraitCode = 4

	//* Creation의 트레이트
	TraitCreatorAndCreatedContract ropedomain.TraitCode = 5

	//*임의의 예시를 위한 트레이트 코드
	TraitFraudCondition1 ropedomain.TraitCode = 1001
	TraitFraudCondition2 ropedomain.TraitCode = 1002
)

// 트레이트의 해석 방법.
// *이 부분의 문자열 값은 바꾸기 가능. 의미만 잘 전달되면 됨
var TraitLegend = map[ropedomain.TraitCode]string{
	TraitDepositAndUser: "TraitDepositAndUser",
	TraitCexAndDeposit:  "TraitCexAndDeposit",

	TraitCexToOther:     "TraitCexToOther",
	TraitDepositToOther: "TraitDepositToOther",

	TraitCreatorAndCreatedContract: "TraitCreatorAndCreatedContract",
}

// ! append- only
const (
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
	TrapFraud ropedomain.TrapCode = 1
)

var TrapLegend = map[ropedomain.TrapCode]ropedomain.TrapNameAndTraits{
	TrapFraud: {
		Name:   "TrapFraud",
		Traits: []ropedomain.TraitCode{TraitFraudCondition1, TraitFraudCondition2},
	},
}

// ! append-only
const (
	// * Triplet의 룰
	RuleUser    ropedomain.RuleCode = 1
	RuleDeposit ropedomain.RuleCode = 2
	RuleCex     ropedomain.RuleCode = 3

	RuleRecievingFromCex ropedomain.RuleCode = 4
	//디포짓에서 tx를 받았고&&동시에 cex주소도 아닌 경우
	RuleRecievingFromDeposit ropedomain.RuleCode = 5
	//* Creation의 룰
	RuleCreator         ropedomain.RuleCode = 6
	RuleCreatedContract ropedomain.RuleCode = 7
)

var RuleLegend = map[ropedomain.RuleCode]string{
	RuleUser:    "RuleUser",
	RuleDeposit: "RuleDeposit",
	RuleCex:     "RuleCex",

	RuleRecievingFromCex:     "RuleRecievingFromCex",
	RuleRecievingFromDeposit: "RuleRecievingFromDeposit",

	RuleCreator:         "RuleCreator",
	RuleCreatedContract: "RuleCreatedContract",
}

//! append-onlt

const (
	AuthorSystem ropedomain.AuthorCode = 1
	AuthorKMH    ropedomain.AuthorCode = 2
	AuthorKSW    ropedomain.AuthorCode = 3
	AuthorJHL    ropedomain.AuthorCode = 4
)

var AuthorLegend = map[ropedomain.AuthorCode]string{
	AuthorSystem: "AuthorSystem",
	AuthorKMH:    "AuthorKMH",
	AuthorKSW:    "AuthorKSW",
	AuthorJHL:    "AuthorJHL",
}
