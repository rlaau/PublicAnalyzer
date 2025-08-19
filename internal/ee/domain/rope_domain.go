package domain

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
)

type Vertex struct {
	Address domain.Address
	// 새로운 그룹인지는 "새로운 트레이트인지" 판단해서 판별 가능
	// 만약 "새로운 트레이트"라면, 오토 인크리먼트 id++ 해서 판별
	// 유니언파인드의 루트노드 통한 그룹 판별 로직을 여기에 적용한 것
	RopeInfos []RopeInfo
	// 세부 엣지들
	// 여기서 "트레이트"를 통해서 세부 사항을 건너뜀
	PartnerAndTrait []PartnerAndTrait
}

// n1, n2가 같은 로프에 속하는지 판별
func InSameRope(n1 *Vertex, n2 *Vertex) bool {
	return hasCommonRopeID(n1.RopeInfos, n2.RopeInfos)
}
func hasCommonRopeID(a, b []RopeInfo) bool {
	seen := make(map[RopeID]struct{}, len(a))
	for _, x := range a {
		seen[x.RopeID] = struct{}{}
	}
	for _, y := range b {
		if _, ok := seen[y.RopeID]; ok {
			return true
		}
	}
	return false
}

// Rope는 DB저장되지 않음. DB에서 꺼내온 서브그래프임.
type Rope struct {
	RopeID    RopeID
	RopeTrait TraitCode
	Nodes     []Vertex
}

// auto inc하는 trait Id
type TraitID uint64

type TraitCode int

const (
	Dual TraitCode = iota
	CexAndDeposit
)

// 트레이토 이어진 로프
// 로프는 단일 트레이트를 간선으로 하는 서브그래프임.
type RopeInfo struct {
	RopeID    RopeID
	RopeTrait TraitCode
}

type RopeMark struct {
	RopeID     RopeID
	RopeTrait  TraitCode
	RopeSize   uint32
	RopeVolume uint32
	LastSeen   chaintimer.ChainTime
}
type RopeID uint64

type PartnerAndTrait struct {
	//상대 주소
	PartnerAdress domain.Address
	//상대 주소와의 트레이트 코드
	TraitCode TraitCode
	//필요시 참조할 트레이트ID
	TraitID TraitID
}
type TraitEvent struct {
	TraitCode TraitCode
	RuleA     RuleCode
	RuleB     RuleCode
	TxID      domain.TxId
	EventTime chaintimer.ChainTime
	Score     int32
}
type TraitMark struct {
	TraitID   TraitID
	TxID      domain.TxId
	TraitCode TraitCode
	//앞선 주소의 역할
	RuleA RuleCode
	//뒤에오는 주소의 역할
	RuleB RuleCode
	//그 트레이트의 세기
	Score    int32
	LastSeen chaintimer.ChainTime
}

type RuleCode int

const (
	Customer RuleCode = iota
	Deposit
	CEX
)
