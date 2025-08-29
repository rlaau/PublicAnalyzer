package iface

import (
	shared "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/sharedface"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type RelPort interface {
	Register(triplet shared.TripletPort, creation shared.CreationPort) error
	//형제 모듈에 접근
	GetCreationPort() shared.CreationPort
	EnqueueToCreation(v shared.CreationEventMsg) error
	//자신 모듈을 소비
	DequeueTriplet() <-chan shared.TripletEventMsg
	ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction
	RopeDB() ropeapp.RopeDB
}
