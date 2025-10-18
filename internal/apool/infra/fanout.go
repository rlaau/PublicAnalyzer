package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/fanout"
)

func NewApoolFanoutManager(name string, inputCap, outputCap int) *fanout.FanoutInMemory[domain.MarkedTransaction] {
	relBusRequest := fanout.BusRequest{
		BusName:   string(iface.RelBusName),
		WantClose: false,
	}
	nodBusRequest := fanout.BusRequest{
		BusName:   string(iface.NodBusName),
		WantClose: false,
	}
	busRequests := []fanout.BusRequest{relBusRequest, nodBusRequest}
	return fanout.NewFanoutInMemory[domain.MarkedTransaction](name, busRequests, inputCap, outputCap)
}
