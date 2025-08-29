package rel

import (
	triplet "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

var DefaultTripletConfig = &triplet.TripletConfig{
	Name: "Simple-Triplet",
	Mode: mode.TestingModeProcess,
}

// func ComposeRel(mode mode.ProcessingMode, name string) RelationPool {

// }
