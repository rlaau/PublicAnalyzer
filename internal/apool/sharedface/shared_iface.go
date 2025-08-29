package sharedface

import (
	"context"
	"io"

	nsface "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/sharedface"
	rsface "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/sharedface"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type RelPort interface {
	RopeDB() ropeapp.RopeDB
	GetTripletPort() rsface.TripletPort
	GetCreationPort() rsface.CreationPort
	EnqueueToTriplet(v rsface.TripletEventMsg)
	EnqueueToCreation(v rsface.CreationEventMsg)
	DequeueTriplet() <-chan rsface.TripletEventMsg
	DequeueCreation() <-chan rsface.CreationEventMsg
	ConsumeTripletTxByFanout() <-chan domain.MarkedTransaction
	ConsumeCreationTxByFanout() <-chan domain.MarkedTransaction
	Start(ctx context.Context) error
	io.Closer
}

type RelMsg any

type NodPort interface {
	GetCoPort() nsface.CoPort
	Start(ctx context.Context) error
	io.Closer
}

type NodMsg any
