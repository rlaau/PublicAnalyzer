package rel

import (
	triplet "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

func DefaultTestingTripletConfig(processName string) *triplet.TripletConfig {
	return &triplet.TripletConfig{
		Name:        "Simple-Triplet",
		Mode:        mode.TestingModeProcess,
		BusCapLimit: 200_000,
		WorkerCount: 2,
		AutoCleanup:         false, // ←★ 결과 보존 위해 비활성화
		ResultReporting:     true,
		StatsInterval:       2_000_000_000, // 2초
		HealthCheckInterval: 3_000_000_000, // 3초
	}
}
