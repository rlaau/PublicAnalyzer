package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/fanout"
)

func NewNodPoolFanoutManager(name string, inputCap, outputCap int) *fanout.FanoutInMemory[domain.MarkedTransaction] {
	eoBusRequest := fanout.BusRequest{
		BusName:   string(iface.CoBusName),
		WantClose: false,
	}
	coBusRequest := fanout.BusRequest{
		BusName:   string(iface.EoBusName),
		WantClose: false,
	}
	busRequests := []fanout.BusRequest{eoBusRequest, coBusRequest}
	return fanout.NewFanoutInMemory[domain.MarkedTransaction](name, busRequests, inputCap, outputCap)
}
