package app

import (
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ------------------------------------------------------------
// 9) GC (Garbage Collection) 관련 로직
// ------------------------------------------------------------

const (
	gcInterval       = 5 * time.Minute
	gcThreshold      = 0.4
	gcPublishTrigger = 100_000 // 10만 건마다 트리거
)

// startGC: GC 타이머 시작
func (b *BadgerRopeDB) startGC() {
	b.gcTicker = time.NewTicker(gcInterval)
}

// gcWorker: 주기적으로 GC 조건 체크 후 실행
func (b *BadgerRopeDB) gcWorker() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.gcTicker.C:
			if b.shouldRunGC() {
				b.runBadgerGC()
			}
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
