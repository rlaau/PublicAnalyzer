package infra

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/protocol"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
)

type RawBadgerProvider interface {
	RawBadgerDB() *badger.DB
}

func NewClusterDB() (iface.ClusterDB, error) {
	db, err := ropeapp.NewRopeDB("RelGraphDB", protocol.TraitLegend, protocol.RuleLegend, protocol.PolyTraitLegend, protocol.TrapLegend, protocol.AuthorLegend)
	if err != nil {
		return nil, fmt.Errorf("failed to open rope db: %w", err)
	}
	return db, nil
}
