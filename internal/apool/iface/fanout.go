package iface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type ApoolChildName string

const (
	RelBusName ApoolChildName = "RelBus"
	NodBusName ApoolChildName = "NodBus"
)

type ApoolFanoutManager interface {
	gface.Module
	Publish(domain.MarkedTransaction) error
	GetBus(name string) *eventbus.EventBus[domain.MarkedTransaction]
	OffBus(name string) bool
	OnBus(name string) bool
}
