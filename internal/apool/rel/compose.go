package rel

import (
	"fmt"
	"path/filepath"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	triplet "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

func DefaultTestingTripletConfig(processName string) *triplet.TripletConfig {
	return &triplet.TripletConfig{
		Name:           "Simple-Triplet",
		Mode:           mode.TestingModeProcess,
		BusCapLimit:    50_000,
		WorkerCount:    2,
		IsolatedDBPath: filepath.Join(computation.FindTestingStorageRootPath(), processName),

		AutoCleanup:         false, // ←★ 결과 보존 위해 비활성화
		ResultReporting:     true,
		StatsInterval:       2_000_000_000, // 2초
		HealthCheckInterval: 3_000_000_000, // 3초
	}
}

// ! TODO 현재는 극도로 미완인 함수임
// TODO 추후 mode기반 컨피겨 분기(현재는 테스팅 컨피겨 뿐임)
// TODO creation까지 달기
// TODO 지금은, 형식이 이렇다는 거임
func ComposeRelPool(mode mode.ProcessingMode, processName string, apool iface.ApoolPort) *RelationPool {
	preRelPool, err := CreateRelationPoolFrame(mode, apool)
	if err != nil {
		panic("relPool Frame생성 중 에러")
	}
	var tripletCfg *triplet.TripletConfig
	if mode.IsTest() {
		tripletCfg = DefaultTestingTripletConfig(processName)
	} else {
		//TODO 추후 프로덕션 설정 구현할 것
		panic("현재 프로덕션 설정은 구현되지 않음")
	}
	preTriplet, err := app.CreateTriplet(tripletCfg, preRelPool)
	if err != nil {
		panic("preTriplet생성 중 에러")
	}
	//TODO 실구현 할것
	var preCreation iface.CreationPort = nil

	preRelPool.Register(preTriplet, preCreation)
	fmt.Println("rel pool에 두 모듈 등록 완료")
	return preRelPool

}
