package app

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// TripletConfig 분석기 설정
type TripletConfig struct {
	// 기본 설정
	Name string              // 분석기 이름
	Mode mode.ProcessingMode // 동작 모드 (production/testing)

	// 성능 설정
	BusCapLimit       int   // 채널 버퍼 크기
	WorkerCount       int   // 워커 고루틴 수
	MaxProcessingTime int64 // 트랜잭션 처리 최대 시간 (나노초)

	// 모니터링 설정
	StatsInterval       int64 // 통계 출력 간격 (나노초)
	HealthCheckInterval int64 // 헬스체크 간격 (나노초)

	// *IsolatedDBPath는 모듈 데이터베이스의 루트 경로임
	// 로직: findRoot를 통해 루트를 찾고, 그 안에 새로운 데이터 폴더를 생성하거나 찾는 것.
	// 테스트 시엔 고립된 DB통해서 얻고, 상용 시엔 다른 path에서 얻기
	IsolatedDBPath string // 데이터 저장 경로

	// 테스트 모드 전용 설정
	AutoCleanup     bool // 종료 시 자동 정리 여부
	ResultReporting bool // 결과 리포팅 여부
}

// AnalyzerFactory 분석기 팩토리 함수
type AnalyzerFactory func(config *TripletConfig) (CommonTriplet, error)

// CreateTriplet 설정에 따라 적절한 분석기 생성
func CreateTriplet(config *TripletConfig, relPool iface.RelPort) (CommonTriplet, error) {
	if config.Mode.IsTest() {
		return NewTestingEOAAnalyzer(config, relPool)
	} else {
		return NewProductionEOAAnalyzer(config, relPool)
	}
}
