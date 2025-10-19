package infra

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/fanout"
)

func NewRelFanoutManager(name string, inputCap, outputCap int) *fanout.FanoutInMemory[domain.MarkedTransaction] {
	tripletBusRequest := fanout.BusRequest{
		BusName:   string(iface.TripletBusName),
		WantClose: false,
	}
	// creationBusRequest := fanout.BusRequest{
	// 	BusName:   string(iface.CreationBusName),
	// 	WantClose: false,
	// }
	busRequests := []fanout.BusRequest{tripletBusRequest}
	return fanout.NewFanoutInMemory[domain.MarkedTransaction](name, busRequests, inputCap, outputCap)
}
