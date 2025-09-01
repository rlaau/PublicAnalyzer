package app

import (
	"time"

	chaintimer "github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/proto"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/shopspring/decimal"
)

// ------------------------------------------------------------
// Domain → Proto 변환기들
// ------------------------------------------------------------

// TraitMark → TraitMarkProto
func TraitMarkToProto(tm domain.TraitMark) *proto.TraitMarkProto {
	authorCodes := make([]uint32, len(tm.AuthorCodes))
	for i, ac := range tm.AuthorCodes {
		authorCodes[i] = uint32(ac)
	}

	return &proto.TraitMarkProto{
		AuthorCodes: authorCodes,
		LastTx:      tm.LastTx.String(),
		TraitId:     uint64(tm.ID),
		Trait:       uint32(tm.Trait),
		AddressA:    tm.AddressA.String(),
		RuleA:       uint32(tm.RuleA),
		AddressB:    tm.AddressB.String(),
		RuleB:       uint32(tm.RuleB),
		Score:       float64(tm.Score),
		Volume:      tm.Volume,
		ValueOrNull: proto.DecimalToInt128(tm.ValueOrNull),
		LastSeen:    tm.LastSeen.Unix(),
	}
}

// TraitMarkProto → TraitMark
func ProtoToTraitMark(tp *proto.TraitMarkProto) domain.TraitMark {
	authorCodes := make([]domain.AuthorCode, len(tp.AuthorCodes))
	for i, ac := range tp.AuthorCodes {
		authorCodes[i] = domain.AuthorCode(ac)
	}
	tid, _ := shareddomain.ParseTxId(tp.LastTx)
	return domain.TraitMark{
		AuthorCodes: authorCodes,
		LastTx:      tid,
		ID:          domain.TraitID(tp.TraitId),
		Trait:       domain.TraitCode(tp.Trait),
		AddressA:    shareddomain.MustParseAddressFromString(tp.AddressA),
		RuleA:       domain.RuleCode(tp.RuleA),
		AddressB:    shareddomain.MustParseAddressFromString(tp.AddressB),
		RuleB:       domain.RuleCode(tp.RuleB),
		Score:       int32(tp.Score),
		Volume:      tp.Volume,
		ValueOrNull: proto.Int128ToDecimal(tp.ValueOrNull),
		LastSeen:    chaintimer.ChainTime(time.Unix(tp.LastSeen, 0)),
	}
}

// RopeMark → RopeMarkProto
func RopeMarkToProto(rm domain.RopeMark) *proto.RopeMarkProto {
	members := make([]string, len(rm.Members))
	for i, member := range rm.Members {
		members[i] = member.String()
	}

	trapVertices := make([]*proto.TrapVertexProto, len(rm.TrapVertexes))
	for i, tv := range rm.TrapVertexes {
		trapVertices[i] = &proto.TrapVertexProto{
			Address: tv.Address.String(),
			Trap:    uint32(tv.Trap),
		}
	}

	return &proto.RopeMarkProto{
		RopeId:       uint64(rm.ID),
		Trait:        uint32(rm.Trait),
		Size:         rm.Size,
		Members:      members,
		Volume:       rm.Volume,
		ValueOrNull:  proto.DecimalToInt128(rm.ValueOrNull),
		LastSeen:     rm.LastSeen.Unix(),
		TrapVertices: trapVertices,
	}
}

// RopeMarkProto → RopeMark
func ProtoToRopeMark(rp *proto.RopeMarkProto) domain.RopeMark {
	members := make([]shareddomain.Address, len(rp.Members))
	for i, member := range rp.Members {
		members[i] = shareddomain.MustParseAddressFromString(member)
	}

	trapVertices := make([]domain.TrapVertex, len(rp.TrapVertices))
	for i, tv := range rp.TrapVertices {
		trapVertices[i] = domain.TrapVertex{
			Address: shareddomain.MustParseAddressFromString(tv.Address),
			Trap:    domain.TrapCode(tv.Trap),
		}
	}

	return domain.RopeMark{
		ID:           domain.RopeID(rp.RopeId),
		Trait:        domain.TraitCode(rp.Trait),
		Size:         rp.Size,
		Members:      members,
		Volume:       rp.Volume,
		ValueOrNull:  proto.Int128ToDecimal(rp.ValueOrNull),
		LastSeen:     chaintimer.ChainTime(time.Unix(rp.LastSeen, 0)),
		TrapVertexes: trapVertices,
	}
}

// PolyRopeMark → PolyRopeMarkProto
func PolyRopeMarkToProto(prm domain.PolyRopeMark) *proto.PolyRopeMarkProto {
	ropes := make([]uint64, len(prm.Ropes))
	for i, rope := range prm.Ropes {
		ropes[i] = uint64(rope)
	}

	return &proto.PolyRopeMarkProto{
		PolyRopeId: uint64(prm.ID),
		PolyTrait:  uint32(prm.PolyTrait),
		Ropes:      ropes,
	}
}

// PolyRopeMarkProto → PolyRopeMark
func ProtoToPolyRopeMark(pp *proto.PolyRopeMarkProto) domain.PolyRopeMark {
	ropes := make([]domain.RopeID, len(pp.Ropes))
	for i, rope := range pp.Ropes {
		ropes[i] = domain.RopeID(rope)
	}

	return domain.PolyRopeMark{
		ID:        domain.PolyRopeID(pp.PolyRopeId),
		PolyTrait: domain.PolyTraitCode(pp.PolyTrait),
		Ropes:     ropes,
	}
}

// Vertex → VertexProto
func VertexToProto(v domain.Vertex) *proto.VertexProto {
	ropes := make([]*proto.RopeRefProto, len(v.Ropes))
	for i, r := range v.Ropes {
		ropes[i] = &proto.RopeRefProto{
			Id:    uint64(r.ID),
			Trait: uint32(r.Trait),
		}
	}

	traits := make([]*proto.TraitRefProto, len(v.Traits))
	for i, t := range v.Traits {
		traits[i] = &proto.TraitRefProto{
			TraitId:     uint64(t.TraitID),
			Trait:       uint32(t.Trait),
			Partner:     t.Partner.String(),
			MyRule:      uint32(t.MyRule),
			PartnerRule: uint32(t.PartnerRule),
		}
	}

	polyRopes := make([]*proto.PolyRopeRefProto, len(v.PolyRopes))
	for i, pr := range v.PolyRopes {
		polyRopes[i] = &proto.PolyRopeRefProto{
			Id:        uint64(pr.Id),
			PolyTrait: uint32(pr.PolyTrait),
		}
	}

	traps := make([]uint32, len(v.Traps))
	for i, trap := range v.Traps {
		traps[i] = uint32(trap)
	}

	return &proto.VertexProto{
		Address:   v.Address.String(),
		Ropes:     ropes,
		Traits:    traits,
		PolyRopes: polyRopes,
		Traps:     traps,
	}
}

// VertexProto → Vertex
func ProtoToVertex(vp *proto.VertexProto) domain.Vertex {
	ropes := make([]domain.RopeRef, len(vp.Ropes))
	for i, r := range vp.Ropes {
		ropes[i] = domain.RopeRef{
			ID:    domain.RopeID(r.Id),
			Trait: domain.TraitCode(r.Trait),
		}
	}

	traits := make([]domain.TraitRef, len(vp.Traits))
	for i, t := range vp.Traits {
		traits[i] = domain.TraitRef{
			TraitID:     domain.TraitID(t.TraitId),
			Trait:       domain.TraitCode(t.Trait),
			Partner:     shareddomain.MustParseAddressFromString(t.Partner),
			MyRule:      domain.RuleCode(t.MyRule),
			PartnerRule: domain.RuleCode(t.PartnerRule),
		}
	}

	polyRopes := make([]domain.PolyRopeRef, len(vp.PolyRopes))
	for i, pr := range vp.PolyRopes {
		polyRopes[i] = domain.PolyRopeRef{
			Id:        domain.PolyRopeID(pr.Id),
			PolyTrait: domain.PolyTraitCode(pr.PolyTrait),
		}
	}

	traps := make([]domain.TrapCode, len(vp.Traps))
	for i, trap := range vp.Traps {
		traps[i] = domain.TrapCode(trap)
	}

	return domain.Vertex{
		Address:   shareddomain.MustParseAddressFromString(vp.Address),
		PolyRopes: polyRopes,
		Ropes:     ropes,
		Traits:    traits,
		Traps:     domain.SatisfyingTraps(traps),
	}
}

// ------------------------------------------------------------
// 고성능 Decimal 처리 최적화
// ------------------------------------------------------------

// DecimalPool - decimal 연산을 위한 객체 풀
var decimalPool = &struct {
	zeros  chan decimal.Decimal
	others chan decimal.Decimal
}{
	zeros:  make(chan decimal.Decimal, 100),
	others: make(chan decimal.Decimal, 500),
}

// GetDecimal - 풀에서 decimal 가져오기
func GetDecimal() decimal.Decimal {
	select {
	case d := <-decimalPool.zeros:
		return d
	case d := <-decimalPool.others:
		return d
	default:
		return decimal.Zero
	}
}

// PutDecimal - 풀에 decimal 반환
func PutDecimal(d decimal.Decimal) {
	if d.IsZero() {
		select {
		case decimalPool.zeros <- d:
		default:
		}
	} else {
		select {
		case decimalPool.others <- d:
		default:
		}
	}
}

// FastDecimalAdd - 최적화된 decimal 덧셈
func FastDecimalAdd(a, b decimal.Decimal) decimal.Decimal {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	return a.Add(b)
}
