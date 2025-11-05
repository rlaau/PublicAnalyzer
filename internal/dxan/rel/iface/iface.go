package iface

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type ClusterDB interface {
	app.RopeDB
}

type RelChildName string

const (
	TripletBusName  RelChildName = "TripletBus"
	CreationBusName RelChildName = "CreationBus"
)

type RelPoolFanout interface {
	gface.Module
	Publish(ctx context.Context, tx domain.MarkedTransaction) error
	GetBus(name string) *eventbus.EventBus[domain.MarkedTransaction]
	OffBus(name string) bool
	OnBus(name string) bool
}
