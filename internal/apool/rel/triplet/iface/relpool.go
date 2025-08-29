package iface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/iface"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type RelPort interface {
	Register(triplet iface.TripletPort, creation iface.CreationPort) error
	//형제 모듈에 접근
	GetCreationPort() iface.CreationPort
	EnqueueToCreation(v iface.CreationEventMsg) error
	//자신 모듈을 소비
	DequeueTriplet() <-chan iface.TripletEventMsg
	ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction
	RopeDB() ropeapp.RopeDB
}
