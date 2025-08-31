package main

import (
	aapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool"
	nodapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod"
	coapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/co/app"
	eoapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/eo/app"
	relapp "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

func GetRelPool() (*relapp.RelationPool, error) {
	apool := &aapp.AnalyzerPool{}
	//* nodPool을 강제로 생성
	nodPool := &nodapp.NodPool{}
	//* CO생성 및 nodPool에 강제 등록
	co := coapp.NewCo(coapp.CoCfg{Mode: mode.ProductionModeProcess}, nodPool)
	eo := eoapp.NewEo(mode.ProductionModeProcess, nodPool)
	nodPool.SetCoPort(co)
	nodPool.SetEoPort(eo)
	apool.RegisterPorts(nodPool, nil)
	relPool := relapp.ComposeRelPool(mode.ProductionModeProcess, "stream_tx_to_triplet", apool)
	apool.RegisterPorts(nodPool, relPool)
	return relPool, nil

}

