package app

import (
	"context"

	sdomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type TripletModel interface {
	ComsumeTransaction(ctx context.Context) error
	ProcessTransaction(ctx context.Context, tx sdomain.MarkedTransaction) error
}
