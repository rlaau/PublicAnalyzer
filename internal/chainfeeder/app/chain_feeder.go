package app

import (
	"context"
	"fmt"
	"log"
	"time"

	k "github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/tools"

	// 너가 만든 프리페쳐 모듈
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/chainfeeder/domain"
)

type FeederConfig struct {
	// Prefetcher + Producer 설정 그대로 품기
	Prefetch PrefetcherConfig
	Producer k.BackpressureProducerConfig

	// 워터마크 (프로듀서 내부 버퍼 크기 기준, "메시지 수")
	// ※ Producer.MaxBufferSize와 같은 단위를 사용 (Message[T] 개수)
	HighWatermark int // 이 이상이면 Prefetcher pause
	LowWatermark  int // 이 이하면 Prefetcher resume

	// 로깅 빈도/주기
	DrainCheckInterval time.Duration // 버퍼 드레인 확인 주기
}

type ChainFeeder struct {
	cfg        FeederConfig
	prefetcher *Prefetcher
	producer   *k.KafkaBatchProducerWithBackpressure[domain.RawTransaction]
}

func NewChainFeeder(ctx context.Context, cfg FeederConfig, bp tools.CountingBackpressure) (*ChainFeeder, error) {
	if cfg.DrainCheckInterval <= 0 {
		cfg.DrainCheckInterval = 200 * time.Millisecond
	}
	if cfg.Producer.Mode != k.ModeBuffered {
		return nil, fmt.Errorf("ChainFeeder requires Producer ModeBuffered")
	}
	if cfg.Producer.MaxBufferSize <= 0 {
		cfg.Producer.MaxBufferSize = 100000 // 합리적 디폴트
	}
	// 워터마크 디폴트: 80/40%
	if cfg.HighWatermark <= 0 {
		cfg.HighWatermark = int(float64(cfg.Producer.MaxBufferSize) * 0.8)
	}
	if cfg.LowWatermark <= 0 {
		cfg.LowWatermark = int(float64(cfg.Producer.MaxBufferSize) * 0.4)
	}

	// Prefetcher
	p, err := NewPrefetcher(ctx, cfg.Prefetch)
	if err != nil {
		return nil, fmt.Errorf("NewPrefetcher: %w", err)
	}

	// Kafka Producer (Buffered)
	prod := k.NewKafkaBatchProducerWithBackpressure[domain.RawTransaction](cfg.Producer, bp)

	return &ChainFeeder{
		cfg:        cfg,
		prefetcher: p,
		producer:   prod,
	}, nil
}

func (f *ChainFeeder) Start() error {
	if err := f.producer.Start(); err != nil {
		return fmt.Errorf("producer.Start: %w", err)
	}
	f.prefetcher.Start()

	// 브리징 루프
	go f.bridge()
	return nil
}

func (f *ChainFeeder) Stop() {
	_ = f.producer.Stop()
	f.prefetcher.Stop()
}

// ===== 브리지 루프 =====
func (f *ChainFeeder) bridge() {
	high := f.cfg.HighWatermark
	low := f.cfg.LowWatermark
	checkEvery := f.cfg.DrainCheckInterval

	log.Printf("[ChainFeeder] bridge start (high=%d, low=%d, bufcap=%d, batch=%d)",
		high, low, f.cfg.Producer.MaxBufferSize, f.cfg.Prefetch.BatchLimit)

	for batch := range f.prefetcher.Out() {
		// 1) High watermark 넘으면 Prefetcher 일단 pause (다음 배치부터 멈추게 됨)
		if f.producer.GetBufferSize() >= high {
			log.Printf("[ChainFeeder] producer buffer >= high(%d). Pausing prefetcher.", high)
			// Prefetcher는 Out 채널에 push해놓은 현재 배치까지는 넘어올 수 있음.
			// 이후 재개는 low 미만으로 내려갈 때
			// (우린 Resume만 제공하니 이 타이밍에선 그냥 대기만)
		}

		// 2) Kafka 메시지로 변환하여 큐잉
		msgs := make([]k.Message[domain.RawTransaction], len(batch))
		for i, tx := range batch {
			msgs[i] = k.Message[domain.RawTransaction]{
				Key:   []byte(tx.TxId), // 파티션/중복 방지에 유리
				Value: tx,
			}
		}

		// 3) 큐잉. 버퍼가 꽉 찬 경우 QueueMessages가 에러를 반환할 수 있음
		for {
			if err := f.producer.QueueMessages(msgs); err != nil {
				// 내부 버퍼 오버플로우. 드레인될 때까지 대기
				log.Printf("[ChainFeeder] Queue overflow: %v (buf=%d). waiting...", err, f.producer.GetBufferSize())
				time.Sleep(checkEvery)
				continue
			}
			break
		}

		// 4) 버퍼가 enough drain 되었으면 Prefetcher 재개
		if f.producer.GetBufferSize() <= low {
			f.prefetcher.Resume()
		}
	}
	log.Printf("[ChainFeeder] bridge stop")
}
