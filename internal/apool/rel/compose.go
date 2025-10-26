package rel

import (
	triplet "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

func DefaultTestingTripletConfig(processName string) *triplet.TripletConfig {
	return &triplet.TripletConfig{
		Name:        "Simple-Triplet",
		Mode:        mode.TestingModeProcess,
		WorkerCount: 2,
	}
}
