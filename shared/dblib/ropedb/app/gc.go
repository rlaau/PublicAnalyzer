package app

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ------------------------------------------------------------
// 9) GC (Garbage Collection) 관련 로직
// ------------------------------------------------------------

const (
	gcInterval       = 3 * time.Minute
	gcThreshold      = 0.4
	gcPublishTrigger = 100_000 // 10만 건마다 트리거
)

// startGC: GC 타이머 시작
func (b *BadgerRopeDB) startGC() {
	b.gcTicker = time.NewTicker(gcInterval)
}

// gcWorker: 주기적으로 GC 조건 체크 후 실행
func (b *BadgerRopeDB) gcWorker(ctx context.Context) error {

	// 20초마다 상태 출력용 타이머
	statusTicker := time.NewTicker(20 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("gcWorker: 컨텍스트 캔슬로 인해 종료됨")
			return nil
		case <-b.gcTicker.C:
			if b.shouldRunGC() {
				b.runBadgerGC()
			}
		case <-statusTicker.C:
			// 20초마다 현재 퍼블리시 카운터 및 DB 통계 출력
			publishCount := atomic.LoadInt64(&b.publishCounter)
			stats := b.GetGraphStats()
			fmt.Printf("[RopeDB Status] 퍼블리시: %d | 버텍스: %v | 로프: %v | 트레이트: %v | 폴리로프: %v\n",
				publishCount, stats["nodes"], stats["ropes"], stats["traits"], stats["polyRopes"])
		}
	}
}

func (b *BadgerRopeDB) shouldRunGC() bool {
	if !b.gcRunning.TryLock() {
		return false
	}
	defer b.gcRunning.Unlock()

	// publish 카운터 체크
	publishCount := atomic.LoadInt64(&b.publishCounter)
	if publishCount >= gcPublishTrigger {
		atomic.StoreInt64(&b.publishCounter, 0)
		return true
	}

	// 두 DB 크기 중 하나라도 크면
	lsmM, vlogM := b.dbM.Size()
	lsmV, vlogV := b.dbV.Size()
	//2GB기준
	if (lsmM+vlogM) > 2*1024*1024*1024 || (lsmV+vlogV) > 2*1024*1024*1024 {
		return true
	}
	return false
}

func (b *BadgerRopeDB) runBadgerGC() {
	// 두 DB 모두 GC 시도, 40% 기준
	for _, db := range []*badger.DB{b.dbM, b.dbV} {
		for {
			if err := db.RunValueLogGC(gcThreshold); err != nil {
				break
			}
		}
	}
}
