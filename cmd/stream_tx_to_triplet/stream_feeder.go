package main

import (
	"time"

	txSteamerApp "github.com/rlaaudgjs5638/chainAnalyzer/internal/streamfeeder/app"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

func GetStreamFeeder() (*txSteamerApp.TxStreamFeeder, *tools.KafkaCountingBackpressure, error) {

	config := txSteamerApp.TxStreamFeederConfig{
		//카프카 설정
		KafkaBrokers: []string{kafka.DefaultKafkaPort},
		Topic:        kafka.ProductionTxTopic,
		//백프레셔 설정
		InitialBatchSize:  1000,
		InitialInterval:   1 * time.Second,
		DirectChannelSize: 150_000,
		MaxBatchSize:      50_000,
		MaxInterval:       30 * time.Second,
		//스트리머를 위한 포지션
		PositionDataDir: computation.ComputeThisProductionStorage("stream_feeder", "position"),
		SaveInterval:    10 * time.Second,

		//리트라이
		MaxRetries: 3,
		RetryDelay: 1 * time.Second,
	}
	backPressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    150_000,
			TargetSaturation: 0.5,
		},
		tools.SeedProducingConfig{SeedInterval: 1, SeedBatchSize: 1000},
		computation.ComputeThisProductionStorage("stream_feeder", "backpressuere"),
	)
	feeder, error := txSteamerApp.NewTxStreamFeeder(config, backPressure)
	return feeder, backPressure, error
}
