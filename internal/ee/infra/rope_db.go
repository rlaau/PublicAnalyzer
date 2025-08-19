package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type RopeDB interface {
	MarkTraitEvent(a1 shareddomain.Address, a2 shareddomain.Address, traitMark *domain.TraitEvent)
	GetNodeRope(a shareddomain.Address) *domain.Rope
	GetByRopeID(id domain.RopeID) *domain.Rope
	InSameRope(a1, a2 shareddomain.Address) bool
	getNextRopeID() domain.RopeID
	getNextTraitID() domain.TraitID
	Close() error
}

type BedgerRopeDB struct {
	///
}

func (b *BedgerRopeDB) MarkTraitEvent(a1 shareddomain.Address, a2 shareddomain.Address, tm *domain.TraitEvent) {
	//1. DB에서 a1,a2기반 vertex꺼내오기
	//2. a1,a2의 PartnerAndTrait에서 traitEvent와 동인한 traitCode 있는지 조회
	//3-1. 트레이트가 없다면 PartnerAndTrait에 추가
	//3-2. 트레이트가 있다면 PartnerAndTrait는 건들지 않음.
	//4-1. TraitMark를 추가 혹은 업데이트.(Tx를 최신 Tx로, Score 업데이트, LastSeen업데이트)
	//4-1. 추가 시엔 벳져로프DB내의 TraitID Incrementer통해서 다음 id부여받은 후 트레이트마크 생성
	//5. 이제 트레이트 기반으로 1) 기존 로프를 이을 지 2) 새 로프 생성할 지 결정
	//5-1. 이때 isIsolatedTrait(a1,a2,tm.TraitCode)==true시 => id= createRope()이후, 각 버텍스에 RopeInfo추가
	//5-2. 만약 a1,a2중 하나 만 로프에 이어져 있을 시.=> 나머지 하나의 버텍스의 RopeInfo에, 다른 놈의 RopeInfo정보를 추가해서 로프를 잇고, 로프 마크 업데이트(사이즈+1,볼륨+1, lastSeen은 now).
	//5-3. 만약 a1,a2둘다 자기만의 로프가 있다면=> 로프마크 기반 사이즈 비교 후 작은 놈을 큰 놈에 흡수. 이때 mapRope로 작은 로프 속한 버텍스들의 로프ID만 변경

}

func (b *BedgerRopeDB) isIsolatedTrait(a1 shareddomain.Address, a2 shareddomain.Address, tc domain.TraitCode) bool {
	// a1과 a2의 버텍스의 두 PartnerAndTrait이 둘다 해당 tc를 가지고 있지 않으면, 이 트레이트는 고립된 것임
	//즉, 이 트레이트로 다른 로프에 이을 수 없다는 뜻
	//그러므로, 고릡된 트레이트는 로프를 생성할 필요가 생김
	return true
}

func (b *BedgerRopeDB) createRope(t domain.TraitCode) domain.RopeID {
	//1. BedgerDb내의 ropeID카운터로 새 로프 ID생성 후 받아옴
	//2. RopeMark{RopeId, t,2, 1, chaintimer.Now()}를 BedgerRopeDB내의 RopeInfoDB에 저장
	return 0
}
func (b *BedgerRopeDB) addToRope(a shareddomain.Address) {}

func (b *BedgerRopeDB) mapRope(startAddress shareddomain.Address, traitCode domain.TraitCode, mapFunc func(v *domain.Vertex)) {
	// 시작 주소로부터 해서, 그 주소가 속한 로프의 모든 버텍스를 순회하며 매핑해서 DB에 저장
	// 순회 로직은, startAddress의 버텍스에서 PartnerAndTrait참조하면서 인자로 받은 traitCode로 이어진 버텍슽에 맵 적용 후 재귀적 전파
}
