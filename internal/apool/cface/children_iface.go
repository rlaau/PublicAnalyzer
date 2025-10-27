package cface

import (
	"context"

	nsface "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/cface"
	rsface "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/cface"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/gface"
)

type RelPort interface {
	gface.Module
	gface.Child
	DispatchTraitEvent(tv ropedomain.TraitEvent) error
	GetGraphStats() map[string]any
	GetTripletPort() rsface.TripletPort
	GetCreationPort() rsface.CreationPort
	EnqueueToTriplet(ctx context.Context, v rsface.TripletEventMsg) error
	EnqueueToCreation(ctx context.Context, v rsface.CreationEventMsg) error
	DequeueTriplet() <-chan rsface.TripletEventMsg
	DequeueCreation() <-chan rsface.CreationEventMsg
	ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction
	ConsumeCreationTxByFanout() <-chan domain.MarkedTransaction
}

type RelMsg any

type NodPort interface {
	gface.Module
	gface.Child
	GetCoPort() nsface.CoPort
	GetEoPort() nsface.EoPort
}

type NodMsg any
