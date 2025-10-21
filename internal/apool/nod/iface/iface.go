package iface

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type NodChildBusName string

const (
	CoBusName NodChildBusName = "CoBus"
	EoBusName NodChildBusName = "EoBus"
)

type NodPoolFanoutManager interface {
	gface.Module
	Publish(domain.MarkedTransaction) error
	GetBus(name string) *eventbus.EventBus[domain.MarkedTransaction]
	OffBus(name string) bool
	OnBus(name string) bool
}
