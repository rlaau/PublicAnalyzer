package infra

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/dto"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/iface"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

type RawBadgerProvider interface {
	RawBadgerDB() *badger.DB
}

func NewClusterDB(ctx context.Context, mode mode.ProcessingMode) (iface.ClusterDB, error) {
	db, err := ropeapp.NewRopeDB(ctx, mode, "RelGraphDB", dto.TraitLegend, dto.RuleLegend, dto.PolyTraitLegend, dto.TrapLegend, dto.AuthorLegend)
	if err != nil {
		return nil, fmt.Errorf("failed to open rope db: %w", err)
	}
	return db, nil
}
