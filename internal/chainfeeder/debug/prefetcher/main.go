package main

import (
	"context"
	"log"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/chainfeeder/app"
)

func main() {
	ctx := context.Background()

	p, err := app.NewPrefetcher(ctx, app.PrefetcherConfig{
		ProjectID:   "YOUR_GCP_PROJECT",
		BatchLimit:  1000,
		StartAt:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		OutCapacity: 4,                      // 체인피더 입력 대기열
		Checkpoint:  "./prefetch_ckpt.json", // 마지막 (ts,txid) 기록
	})
	if err != nil {
		log.Fatal(err)
	}

	p.Start()
	defer p.Stop()

	// 체인피더의 내부 버퍼 예시 (여기선 단순 길이 변수로 가정)
	const feederSoftCap = 200_000
	curFeederBuffered := 0

	for batch := range p.Out() {
		// 1) 내 버퍼에 적재 (여기선 그냥 카운터)
		curFeederBuffered += len(batch)
		// 2) … 실제론 여기서 체인피더의 내부 큐/버퍼에 push
		// 3) 버퍼가 너무 커졌으면 잠깐 소비 시뮬 후 Resume
		if curFeederBuffered > feederSoftCap {
			log.Printf("[feeder] buffer high watermark: %d, draining...", curFeederBuffered)
			time.Sleep(800 * time.Millisecond) // 처리/소비
			curFeederBuffered = curFeederBuffered / 3
			p.Resume() // 프리페쳐 재개
		}
	}
}
