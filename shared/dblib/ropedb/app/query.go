package app

import (
	"fmt"
	"slices"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

func (b *BadgerRopeDB) ViewRopeByNode(a shareddomain.Address) (*domain.Rope, error) {
	v := b.getOrCreateVertex(a)
	if len(v.Ropes) == 0 {
		return nil, nil
	}
	ref := v.Ropes[0]
	return b.ViewRope(ref.ID)
}

// v가 trap일 경우 true. 아니면 false
func (b *BadgerRopeDB) needToAddTrap(v *domain.Vertex, trapCode domain.TrapCode) bool {
	//1. 해당 트랩을 이미 소유하고 있는지 검증
	if v.Traps.IsContatin(trapCode) {
		//소유했다면 추가하지 않음
		return false
	}
	//2. 소유하지 않은경우, 만족 여부 검증
	ownedTraits := make(map[domain.TraitCode]struct{})
	for _, t := range v.Traits {
		ownedTraits[t.Trait] = struct{}{}
	}
	//트랩 코드가 요구하는 모든 트레이트를 만족해야 트랩이고, 아니면 false
	for _, trapTraitCode := range b.trapToTraits[trapCode] {
		if _, exists := ownedTraits[trapTraitCode]; !exists {
			return false
		}
		continue
	}
	// 결국 전부 만족하면 트랩을 추가해야 함
	requiredRuleOrZero := b.trapToRequiredRuleOrNot[trapCode]
	if len(requiredRuleOrZero) == 0 {
		return true
	}

	//트랩이 되기 위해 특별히 지녀야할 역할 있을 경우
	//역할 존재 확인
	return slices.ContainsFunc(requiredRuleOrZero, v.IsContainRule)
}

// getPolyRopeFromVertex: Vertex에서 특정 PolyTrait의 PolyRope ID 가져오기
func (b *BadgerRopeDB) getPolyRopeFromVertex(v *domain.Vertex, polyTrait domain.PolyTraitCode) (domain.PolyRopeID, bool) {
	for _, pr := range v.PolyRopes {
		if pr.PolyTrait == polyTrait {
			return pr.Id, true
		}
	}
	return 0, false
}

// setPolyRopeInVertex: Vertex에 PolyRope 설정
func (b *BadgerRopeDB) setPolyRopeInVertex(v *domain.Vertex, polyRopeID domain.PolyRopeID, polyTrait domain.PolyTraitCode) {
	for i, pr := range v.PolyRopes {
		if pr.PolyTrait == polyTrait {
			v.PolyRopes[i].Id = polyRopeID
			return
		}
	}
	// 없으면 새로 추가
	v.PolyRopes = append(v.PolyRopes, domain.PolyRopeRef{
		Id:        polyRopeID,
		PolyTrait: polyTrait,
	})
}

// calculatePolyRopeSize: PolyRope의 실제 멤버 수 계산
func (b *BadgerRopeDB) calculatePolyRopeSize(ropeIDs []domain.RopeID) int {
	totalSize := 0
	for _, ropeID := range ropeIDs {
		ropeMark := b.getRopeMark(ropeID)
		totalSize += int(ropeMark.Size)
	}
	return totalSize
}

// updateAllVerticesPolyRope: source PolyRope에 속한 모든 Vertex를 target PolyRope로 업데이트
func (b *BadgerRopeDB) updateAllVerticesPolyRope(sourcePolyRopeID, targetPolyRopeID domain.PolyRopeID, polyTrait domain.PolyTraitCode) {
	// source PolyRope의 모든 Rope 가져오기
	srcMark := b.getPolyRopeMark(sourcePolyRopeID)

	// 각 Rope의 모든 멤버 Vertex 업데이트
	for _, ropeID := range srcMark.Ropes {
		ropeMark := b.getRopeMark(ropeID)
		for _, addr := range ropeMark.Members {
			v := b.getOrCreateVertex(addr)
			b.setPolyRopeInVertex(v, targetPolyRopeID, polyTrait)
			b.putVertex(v)
		}
	}
}

func (b *BadgerRopeDB) ViewRope(id domain.RopeID) (*domain.Rope, error) {
	rm := b.getRopeMark(id)
	if rm.ID == 0 {
		return nil, nil
	}
	nodes := make([]domain.Vertex, 0, len(rm.Members))
	for _, addr := range rm.Members {
		nodes = append(nodes, *b.getOrCreateVertex(addr))
	}
	return &domain.Rope{ID: rm.ID, Trait: rm.Trait, Nodes: nodes}, nil
}

func (b *BadgerRopeDB) ViewInSameRope(a1, a2 shareddomain.Address) (bool, error) {
	v1 := b.getOrCreateVertex(a1)
	v2 := b.getOrCreateVertex(a2)
	return inSameRope(v1, v2), nil
}

func (b *BadgerRopeDB) ViewInSameRopeByPolyTrait(a1, a2 shareddomain.Address, polyTrait domain.PolyTraitCode) (bool, error) {
	v1 := b.getOrCreateVertex(a1)
	v2 := b.getOrCreateVertex(a2)
	return inSamePolyRopeByPolyTrait(v1, v2, polyTrait), nil
}

// ViewAllTraitMarkByString returns all TraitMark records with the trait name matching the given string
func (b *BadgerRopeDB) ViewAllTraitMarkByString(s string) ([]domain.TraitMark, error) {
	// Find trait code by string using legend
	var targetCode domain.TraitCode
	found := false
	for code, name := range b.traitLegend {
		if name == s {
			targetCode = code
			found = true
			break
		}
	}

	if !found {
		return []domain.TraitMark{}, nil // return empty slice if string not found in legend
	}

	return b.ViewAllTraitMarkByCode(targetCode)
}

func (b *BadgerRopeDB) GetPolyTraitLegend() map[domain.PolyTraitCode]domain.PolyNameAndTraits {
	return b.polyTraits
}

// GetRuleLegend returns the rule legend for UI display
func (b *BadgerRopeDB) GetRuleLegend() map[string]LegendItem {
	legend := make(map[string]LegendItem)
	for code, name := range b.ruleLegend {
		legend[fmt.Sprintf("%d", code)] = LegendItem{
			Name:  name,
			Color: b.generateRuleColor(code),
			Count: 0,
		}
	}
	return legend
}

// GetTraitLegend returns the trait legend for UI display
func (b *BadgerRopeDB) GetTraitLegend() map[string]LegendItem {
	legend := make(map[string]LegendItem)
	for code, name := range b.traitLegend {
		legend[fmt.Sprintf("%d", code)] = LegendItem{
			Name:  name,
			Color: b.generateTraitColor(code),
			Count: 0,
		}
	}
	return legend
}
func (b *BadgerRopeDB) AreTrapsInMyPolyRope(v *domain.Vertex, polyTrait domain.PolyTraitCode) (bool, []domain.TrapVertex) {
	var targetPolyRope domain.PolyRopeID
	found := false
	for _, polyRef := range v.PolyRopes {
		if polyRef.PolyTrait == polyTrait {
			targetPolyRope = polyRef.Id
			found = true
			break
		}
	}
	if !found {
		return false, []domain.TrapVertex{}
	}

	pMark := b.getPolyRopeMark(targetPolyRope)
	rids := pMark.Ropes
	var totalTV []domain.TrapVertex

	for _, rid := range rids {
		if found, tvs := b.IsTrapRope(rid); found {
			totalTV = append(totalTV, tvs...)
		}
	}
	if len(totalTV) == 0 {
		return false, []domain.TrapVertex{}
	}
	return true, MergeTrapVerticesUniquePairs(totalTV)

}
func (b *BadgerRopeDB) AreTrapsInMyRopes(v *domain.Vertex) (bool, []domain.TrapVertex) {
	var trapVetexes [][]domain.TrapVertex
	for _, rref := range v.Ropes {
		if isTrap, trapVx := b.IsTrapRope(rref.ID); isTrap {
			trapVetexes = append(trapVetexes, trapVx)
		}
	}
	uniqueTrapVertexes := MergeTrapVerticesUniquePairs(trapVetexes...)
	if len(uniqueTrapVertexes) == 0 {
		return false, []domain.TrapVertex{}
	} else {
		return true, uniqueTrapVertexes
	}
}

// 해당 로프에 트랩 버텍스 있는지, 있다면 어떤게 있는지 조회
func (b *BadgerRopeDB) IsTrapRope(rid domain.RopeID) (bool, []domain.TrapVertex) {
	rope := b.getRopeMark(rid)
	if len(rope.TrapVertexes) == 0 {
		return false, []domain.TrapVertex{}
	} else {
		return true, rope.TrapVertexes
	}
}

func (b *BadgerRopeDB) HandleTrapDetection(v *domain.Vertex) {

	var addedTrap []domain.TrapCode
	//추가된 트랩이 있는지 체크
	for trap := range b.trapToTraits {
		if b.needToAddTrap(v, trap) {
			//트랩 추가가 필요할 시 v, 리스트에 추가
			v.Traps.Add(trap)
			addedTrap = append(addedTrap, trap)
		}
	}
	//추가된 게 있다면 로프마크에 전파
	if len(addedTrap) > 0 {
		b.PropagateTrap(v, addedTrap)
	}

}
func (b *BadgerRopeDB) PropagateTrap(v *domain.Vertex, trapCodes []domain.TrapCode) error {

	//1. 전파할 트랩을 수합
	var trapAndVertexes []domain.TrapVertex
	for _, trapCode := range trapCodes {
		trapAndVertexes = append(trapAndVertexes, domain.TrapVertex{
			Address: v.Address,
			Trap:    trapCode,
		})
	}
	//2. 자신이 속한 모든 로프에 트랩 inc전파
	for _, rref := range v.Ropes {
		_ = b.busRope.Publish(
			domain.RopeMarkUpsert{
				RopeID:       rref.ID,
				TrapVertexes: trapAndVertexes,
			},
		)
	}
	return nil
}

// TODO 테스트 용으로 퍼블릭하게 만들다보니 의도치 않게 전환해버림... 추후 리팩토링 할 것
func (b *BadgerRopeDB) GetOrCreateVertex(a shareddomain.Address) *domain.Vertex {
	return b.getOrCreateVertex(a)
}

// TODO 테스트 용으로 퍼블릭하게 만들다보니 의도치 않게 전환해버림... 추후 리팩토링 할 것
func (b *BadgerRopeDB) RopeIDByTrait(v *domain.Vertex, t domain.TraitCode) (domain.RopeID, bool) {
	return b.ropeIDByTrait(v, t)
}

// TODO 테스트 용으로 퍼블릭하게 만들다보니 의도치 않게 전환해버림... 추후 리팩토링 할 것

func (b *BadgerRopeDB) GetRopeMark(rid domain.RopeID) domain.RopeMark {
	return b.getRopeMark(rid)
}
