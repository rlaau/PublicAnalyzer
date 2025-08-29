// eventbus_persist_test.go
package eventbus_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
)

// 테스트용 이벤트 타입
type testEvt struct {
	ID  int    `json:"id"`
	Msg string `json:"msg"`
}

func TestEventBus_PersistAndReload_JSONL(t *testing.T) {
	// 1) 테스트 루트 정리
	root := filepath.Join(computation.FindTestingStorageRootPath(), "event_bus_test")
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("cleanup root: %v", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	jsonlPath := filepath.Join(root, "cc.jsonl")

	// 2) 첫 번째 버스 생성
	bus1, err := eventbus.NewWithRoot[testEvt](
		func() string { return root },
		"cc.jsonl",
		/*capLimit*/ 6,
	)
	if err != nil {
		t.Fatalf("NewWithRoot bus1: %v", err)
	}

	// 총 6개 발행
	all := []testEvt{
		{ID: 1, Msg: "one"},
		{ID: 2, Msg: "two"},
		{ID: 3, Msg: "three"},
		{ID: 4, Msg: "four"},
		{ID: 5, Msg: "five"},
	}
	for i, e := range all {
		if err := bus1.Publish(e); err != nil {
			t.Fatalf("publish #%d: %v", i+1, err)
		}
	}

	// 앞 2개 소비 → pending 4개 남김
	got1 := recv(t, bus1.Dequeue(), 2*time.Second)
	t.Logf("[bus1] want=%+v got=%+v", all[0], got1)
	if got1 != all[0] {
		t.Fatalf("unexpected first: got=%+v want=%+v", got1, all[0])
	}
	got2 := recv(t, bus1.Dequeue(), 2*time.Second)
	t.Logf("[bus1] want=%+v got=%+v", all[1], got2)
	if got2 != all[1] {
		t.Fatalf("unexpected second: got=%+v want=%+v", got2, all[1])
	}

	// Close → 남은 3개가 JSONL로 저장되어야 함
	bus1.Close()

	// 파일 존재 확인
	if _, err := os.Stat(jsonlPath); err != nil {
		t.Fatalf("expected JSONL to exist after bus1.Close(), stat err=%v", err)
	}

	// 3) 두 번째 버스: backlog 로드(즉시 파일 삭제) 및 out으로 재송출 확인
	bus2, err := eventbus.NewWithRoot[testEvt](
		func() string { return root },
		"cc.jsonl",
		/*capLimit*/ 5,
	)
	if err != nil {
		t.Fatalf("NewWithRoot bus2: %v", err)
	}

	// 로드 후 파일 삭제되었는지 확인
	if _, err := os.Stat(jsonlPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected JSONL removed after bus2 load, stat err=%v", err)
	}

	// backlog 순서 확인
	wantPersist := all[2:] // {3,4,5}
	for i := range wantPersist {
		got := recv(t, bus2.Dequeue(), 2*time.Second)
		t.Logf("[bus2 #%d] want=%+v got=%+v", i, wantPersist[i], got)
		if got != wantPersist[i] {
			t.Fatalf("mismatch at %d: got=%+v want=%+v", i, got, wantPersist[i])
		}
	}

	// 더 이상 pending 없음 → Close 후 파일 새로 생기면 안 됨
	bus2.Close()
	if _, err := os.Stat(jsonlPath); err == nil {
		t.Fatalf("expected no JSONL after bus2.Close() with empty pending, but file exists")
	}
}

func TestEventBus_PublishAfterClose_ReturnsError(t *testing.T) {
	root := filepath.Join(computation.FindTestingStorageRootPath(), "event_bus_test_after_close")
	_ = os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	b, err := eventbus.NewWithRoot[testEvt](
		func() string { return root },
		"cc.jsonl",
		1,
	)
	if err != nil {
		t.Fatalf("NewWithRoot: %v", err)
	}
	b.Close()

	if err := b.Publish(testEvt{ID: 42, Msg: "nope"}); err == nil {
		t.Fatalf("expected error on Publish after Close, got nil")
	}
}

// --- helpers ---

// recv: 각 수신마다 독립 타임아웃을 사용해, 이전 컨텍스트 만료로 인한 교착을 방지.
func recv[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed unexpectedly")
		}
		return v
	case <-timer.C:
		t.Fatalf("timeout receiving from channel (>%v)", timeout)
		var zero T
		return zero
	}
}
