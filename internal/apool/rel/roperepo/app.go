package roperepo

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

type RawBadgerProvider interface {
	RawBadgerDB() *badger.DB
}

func NewRelGraphDB(mode mode.ProcessingMode) (ropeapp.RopeDB, error) {
	db, err := ropeapp.NewRopeDB(mode, "RelGraphDB", TraitLegend, RuleLegend, PolyTraitLegend)
	if err != nil {
		return nil, fmt.Errorf("failed to open rope db")
	}
	return db, nil
}
