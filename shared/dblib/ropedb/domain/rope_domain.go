package domain

import (
	chaintimer "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// ------------------------------------------------------------
// 1) 간소화된 도메인
// ------------------------------------------------------------

type TraitID uint64
type RopeID uint64

type TraitCode uint16
type RuleCode uint16

// * 실제 사용 시, RopeDB를 임포트하는 패키지는
// * 자기만의 트레이트코드, 룰코드 선언 후 사용
// const (
// 	TraitDual TraitCode = iota
// 	TraitCexAndDeposit
// )

// const (
// 	RuleCustomer RuleCode = iota
// 	RuleDeposit
// 	RuleCEX
// )

// Vertex: 요약 저장(빠른 R/W)
type Vertex struct {
	Address shareddomain.Address
	Ropes   []RopeRef     // (RopeID, Trait) 목록
	Links   []PartnerLink // (Partner, Trait, TraitID)
}

// 도메인 로직 상, 한 Vertex내에서 TraitCode-RopeID는 1:1 매핑임
type RopeRef struct {
	ID    RopeID
	Trait TraitCode
}

type PartnerLink struct {
	Partner shareddomain.Address
	Trait   TraitCode
	TraitID TraitID
}

// RopeMark: Rope에 대한 저장 문서(조회 시 조립의 근간)
type RopeMark struct {
	ID       RopeID
	Trait    TraitCode
	Size     uint32 // Members 길이(정합성 위해 함께 유지)
	Members  []shareddomain.Address
	Volume   uint32
	LastSeen chaintimer.ChainTime
}

// TraitMark: Trait(엣지)에 대한 저장 문서
type TraitMark struct {
	ID       TraitID
	Trait    TraitCode
	RuleA    RuleCode
	RuleB    RuleCode
	Score    int32
	Volume   uint32
	LastSeen chaintimer.ChainTime
}

// Rope: 조회 전용 뷰(저장 X)
type Rope struct {
	ID    RopeID
	Trait TraitCode
	Nodes []Vertex
}

// ------------------------------------------------------------
// 2) 단일화 이벤트(Upsert) 커맨드
//   - Create/Update 구분 없이 하나로 처리
//   - 존재하면 증분, 없으면 생성
// ------------------------------------------------------------

// 한 Tx/이벤트로 들어오는 도메인 입력(버텍스 간 Trait 관계 이벤트)
type TraitEvent struct {
	Trait    TraitCode
	AddressA shareddomain.Address
	RuleA    RuleCode
	AddressB shareddomain.Address
	RuleB    RuleCode
	TxID     shareddomain.TxId
	Time     chaintimer.ChainTime
	Score    int32 // 이번 이벤트가 의미하는 점수 증분
}

func NewTraitEvent(trait TraitCode, ar1, ar2 AddressAndRule, txScala TxScala) TraitEvent {
	if ar2.Address.IsSmallerThan(ar1.Address) {
		return TraitEvent{
			Trait:    trait,
			AddressA: ar2.Address,
			RuleA:    ar2.Rule,
			AddressB: ar1.Address,
			RuleB:    ar1.Rule,
			TxID:     txScala.TxId,
			Time:     txScala.Time,
			Score:    txScala.ScoreInc,
		}
	}
	return TraitEvent{
		Trait:    trait,
		AddressA: ar1.Address,
		RuleA:    ar1.Rule,
		AddressB: ar2.Address,
		RuleB:    ar2.Rule,
		TxID:     txScala.TxId,
		Time:     txScala.Time,
		Score:    txScala.ScoreInc,
	}

}

type AddressAndRule struct {
	Address shareddomain.Address
	Rule    RuleCode
}

type TxScala struct {
	TxId     shareddomain.TxId
	Time     chaintimer.ChainTime
	ScoreInc int32
}

// 내부 비동기 큐용: TraitMark Upsert
type TraitMarkUpsert struct {
	TraitID     TraitID
	Trait       TraitCode
	AddressA    shareddomain.Address
	RuleA       RuleCode
	AddressB    shareddomain.Address
	RuleB       RuleCode
	ScoreDelta  int32
	VolumeDelta uint32
	LastSeen    chaintimer.ChainTime
}

// 내부 비동기 큐용: RopeMark Upsert
type RopeMarkUpsert struct {
	RopeID      RopeID
	Trait       TraitCode
	AddMembers  []shareddomain.Address // dedup해서 추가
	SizeDelta   int32                  // 보통 len(AddMembers)와 동일하게 씀
	VolumeDelta uint32                 // 트랜잭션 볼륨 증분
	LastSeen    chaintimer.ChainTime

	// (옵션) 병합: 나열된 Source Rope들을 대상 RopeID로 흡수
	MergeFrom []RopeID
}
