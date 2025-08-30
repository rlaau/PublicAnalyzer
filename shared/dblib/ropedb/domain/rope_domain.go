package domain

import (
	"slices"

	chaintimer "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// 이벤트를 승인한 측
// 권한을 가진 사람은 "트레이트"를 승인함
// 권한 이벤트는 "트레이트"로 알 수 있음
type AuthorCode int

// 트랩의 코드
// 트랩은 로프에 걸쳐 전파됨
// PolyTrait가 "속성의 합집합"이라면
// Trap은 "속성의 교집합"임
type Trap struct {
	TrapCode   TraitCode
	TraitCodes []TraitCode
}
type TrapVertex struct {
	Address shareddomain.Address
	Trap    TrapCode
}
type TrapCode int

type TrapNameAndTraits struct {
	Name   string
	Traits []TraitCode
}

type PolyTrait struct {
	PolyTraitCode PolyTraitCode
	TraitCodes    []TraitCode
}
type PolyNameAndTraits struct {
	Name   string
	Traits []TraitCode
}

type TraitID uint64
type RopeID uint64

type PolyTraitCode uint64
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

type Vertex struct {
	Address   shareddomain.Address
	PolyRopes []PolyRopeRef
	Ropes     []RopeRef  // (RopeID, Trait) 목록
	Traits    []TraitRef // (Partner, Trait, TraitID)
	Traps     SatisfyingTraps
}
type SatisfyingTraps []TrapCode

func (s *SatisfyingTraps) Add(t TrapCode) bool {
	st := *s
	if slices.Contains(st, t) {
		return false
	}
	*s = append(st, t)
	return true
}

func (s SatisfyingTraps) IsContatin(t TrapCode) bool {
	for _, trap := range s {
		//이미 존재 시 리턴
		if trap == t {
			return true
		} else {
			continue
		}
	}
	return false

}

type PolyRopeRef struct {
	Id PolyRopeID
	//로프는 트레이트에 걸치고, 폴리로프는 폴리 트레이트에 걸침
	PolyTrait PolyTraitCode
}

type PolyRopeID uint64

type PolyRopeMark struct {
	ID        PolyRopeID
	PolyTrait PolyTraitCode
	//로프는 트레이트에 거치고
	//폴리 로프는 폴리 트레이트에 걸침
	//폴리 트레이트는 트레이트에 걸침
	//그러므로 폴리 로프는 로프에 걸침
	Ropes []RopeID
}

// 도메인 로직 상, 한 Vertex내에서 TraitCode-RopeID는 1:1 매핑임
type RopeRef struct {
	ID    RopeID
	Trait TraitCode
}

type TraitRef struct {
	//*추가됨. 권한 코드
	TraitID     TraitID
	Trait       TraitCode
	Partner     shareddomain.Address
	MyRule      RuleCode // 내 역할에 대한 룰 코드
	PartnerRule RuleCode // 파트너 역할에 대한 룰 코드
}

// RopeMark: Rope에 대한 저장 문서(조회 시 조립의 근간)
type RopeMark struct {
	ID           RopeID
	TrapVertexes []TrapVertex
	Trait        TraitCode
	Size         uint32 // Members 길이(정합성 위해 함께 유지)
	Members      []shareddomain.Address
	Volume       uint32
	LastSeen     chaintimer.ChainTime
}

// TraitMark: Trait(엣지)에 대한 저장 문서
// 20+8+2(20+8)+8+8= 100
type TraitMark struct {
	//*추가됨. 권한 코드
	AuthorCodes []AuthorCode
	LastTx      shareddomain.TxId

	ID    TraitID
	Trait TraitCode

	AddressA shareddomain.Address
	RuleA    RuleCode
	AddressB shareddomain.Address
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
	Author AuthorCode
	TxID   shareddomain.TxId

	Trait    TraitCode
	AddressA shareddomain.Address
	RuleA    RuleCode
	AddressB shareddomain.Address
	RuleB    RuleCode
	Time     chaintimer.ChainTime
	Score    int32 // 이번 이벤트가 의미하는 점수 증분
}

func NewTraitEvent(author AuthorCode, trait TraitCode, ar1, ar2 AddressAndRule, txScala TxScala) TraitEvent {
	if ar2.Address.LessThan(ar1.Address) {
		return TraitEvent{
			Author:   author,
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
		Author:   author,
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
	AuthorCode  AuthorCode
	LastTx      shareddomain.TxId
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
	RopeID       RopeID
	TrapVertexes []TrapVertex
	Trait        TraitCode
	AddMembers   []shareddomain.Address // dedup해서 추가
	SizeDelta    int32                  // 보통 len(AddMembers)와 동일하게 씀
	VolumeDelta  uint32                 // 트랜잭션 볼륨 증분
	LastSeen     chaintimer.ChainTime

	// (옵션) 병합: 나열된 Source Rope들을 대상 RopeID로 흡수
	MergeFrom []RopeID
}

func (t1 TrapVertex) IsSame(t2 TrapVertex) bool {
	if t1.Address == t2.Address && t1.Trap == t2.Trap {
		return true
	}
	return false
}
