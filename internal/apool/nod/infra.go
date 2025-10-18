package nod

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/fanout"
)

func NewNodPoolFanoutManager(name string, inputCap, outputCap int) *fanout.FanoutInMemory[domain.MarkedTransaction] {
	eoBusRequest := fanout.BusRequest{
		BusName:   string(CoBusName),
		WantClose: false,
	}
	coBusRequest := fanout.BusRequest{
		BusName:   string(EoBusName),
		WantClose: false,
	}
	busRequests := []fanout.BusRequest{eoBusRequest, coBusRequest}
	return fanout.NewFanoutInMemory[domain.MarkedTransaction](name, busRequests, inputCap, outputCap)
}
