package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/chainfeeder/app"
	cf "github.com/rlaaudgjs5638/chainAnalyzer/internal/chainfeeder/app"
	k "github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"
)

func main() {
	ctx := context.Background()

	// 백프레셔(모니터) 준비
	backpressure := tools.LoadKafkaCountingBackpressure(
		tools.RequestedQueueSpec{
			QueueCapacity:    100000, // 모니터 쪽 스펙 (관찰 값)
			TargetSaturation: 0.5,
		},
		tools.SeedProducingConfig{SeedInterval: 1, SeedBatchSize: 1000},
		"./bp_state",
	)

	// Prefetcher 설정 (시간 기반 페이지네이션 버전)
	pcfg := app.PrefetcherConfig{
		ProjectID:   os.Getenv("GOOGLE_CLOUD_PROJECT"),
		StartAt:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		BatchLimit:  10000,
		OutCapacity: 64, // 배치 64개 = 64*10k = 640k 로컬 버퍼
		Checkpoint:  "./prefetch_ckpt.json",
	}

	// Kafka Producer 설정 (Buffered)
	kcfg := k.BackpressureProducerConfig{
		Brokers:          []string{"localhost:9092"},
		Topic:            "chain-rawtx",
		Mode:             k.ModeBuffered,
		InitialBatchSize: 2000, // 백프레셔 seed
		InitialInterval:  500 * time.Millisecond,
		MaxBufferSize:    200_000,         // 내부 버퍼(Message 수)
		MaxBatchSize:     20_000,          // 안전 상한
		MaxInterval:      3 * time.Second, // 안전 상한
	}

	feeder, err := cf.NewChainFeeder(ctx, cf.FeederConfig{
		Prefetch:           pcfg,
		Producer:           kcfg,
		HighWatermark:      160_000, // 80%
		LowWatermark:       80_000,  // 40%
		DrainCheckInterval: 200 * time.Millisecond,
	}, backpressure)
	if err != nil {
		log.Fatal(err)
	}

	if err := feeder.Start(); err != nil {
		log.Fatal(err)
	}
	defer feeder.Stop()

	// 애플리케이션 계속 실행
	select {}
}
