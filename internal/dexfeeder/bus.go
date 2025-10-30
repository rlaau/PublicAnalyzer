package dexfeeder

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/eventbus"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/txstreamer"
)

// DexStreamBus 는 TxStreamer에서 읽은 RawTransaction을 EventBus에 밀어 넣는 컴포넌트다.
// 포지션 관리와 StreamFrom/StreamBetween/StreamTo 를 지원한다.
type DexStreamBus struct {
	// Dependencies
	txStreamer   *txstreamer.TxStreamer
	bus          *eventbus.EventBus[shareddomain.RawTransaction]
	positionRepo StreamPositionRepository

	// State
	currentPosition StreamPosition
	isRunning       bool
	stopChan        chan struct{}

	// Stats
	totalPublished uint64
	totalErrors    uint64

	// Configuration
	saveInterval time.Duration
	busRelPath   string
	closed       atomic.Bool
}

// DexStreamBusConfig 는 StreamBus 설정값이다.
type DexStreamBusConfig struct {
	// EventBus

	BufferSize int // 이벤트버스 내부 채널 버퍼 크기 (예: 4096)

	// Position persistence
	//PositionDataDir string        // 포지션 저장 디렉토리
	SaveInterval time.Duration // 포지션 주기 저장(기본 1s)
}

// NewDexStreamBus 는 StreamBus 인스턴스를 생성한다.
func NewDexStreamBus(cfg DexStreamBusConfig) (*DexStreamBus, error) {

	busRelPath := "streambus/rawtx.jsonl"
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 4096
	}
	if cfg.SaveInterval <= 0 {
		cfg.SaveInterval = 1 * time.Second
	}

	txStreamer := txstreamer.NewTxStreamer()

	// EventBus (RawTransaction)
	bus := eventbus.NewInMemory[shareddomain.RawTransaction](cfg.BufferSize, "RawTransactionBus")

	// Position repo
	positionRepo := NewFileStreamPositionRepository()

	sb := &DexStreamBus{
		txStreamer:   txStreamer,
		bus:          bus,
		positionRepo: positionRepo,
		saveInterval: cfg.SaveInterval,
		busRelPath:   busRelPath,
		stopChan:     make(chan struct{}),
	}

	// 기존 포지션 로드
	if positionRepo.HasPosition() {
		pos, err := positionRepo.LoadPosition()
		if err != nil {
			log.Printf("Failed to load existing position: %v", err)
		} else {
			sb.currentPosition = pos
			log.Printf("Loaded existing position: %s", pos.String())
		}
	}

	return sb, nil
}

// Start: 별도 워커는 없지만 상태 플래그만 잡아준다.
func (s *DexStreamBus) Start() error {
	if s.isRunning {
		return fmt.Errorf("StreamBus is already running")
	}
	s.isRunning = true
	log.Printf("StreamBus started (bus=%s)", s.busRelPath)
	return nil
}

func (s *DexStreamBus) Bus() *eventbus.EventBus[shareddomain.RawTransaction] {
	return s.bus
}
func (s *DexStreamBus) Close() error {
	if !s.isRunning {
		return nil
	}
	if s.closed.Swap(true) {
		return nil
	}
	log.Printf("Stopping StreamBus...")

	// stop 시그널
	close(s.stopChan)

	// 포지션 저장
	if s.currentPosition.IsValid() {
		if err := s.positionRepo.SavePosition(s.currentPosition); err != nil {
			log.Printf("Error saving final position: %v", err)
		} else {
			log.Printf("Final position saved: %s", s.currentPosition.String())
		}
	}

	// 버스 종료
	s.bus.Close()

	s.isRunning = false
	log.Printf("StreamBus stopped. total_published=%d total_errors=%d",
		atomic.LoadUint64(&s.totalPublished), atomic.LoadUint64(&s.totalErrors))
	return nil
}

// StreamFrom: 특정 포지션에서 시작해 현재 날짜부터 계속 스트림
func (s *DexStreamBus) StreamFrom(ctx context.Context, startDate string, startIndex int64) error {
	if !s.isRunning {
		return fmt.Errorf("StreamBus is not running")
	}
	s.currentPosition = NewStreamPosition(startDate, startIndex)
	return s.streamFromCurrentPosition(ctx)
}

// StreamTo: 현재(혹은 설정된) 포지션부터 endDate(포함)까지 스트림
func (s *DexStreamBus) StreamTo(ctx context.Context, endDate string) error {
	if !s.isRunning {
		return fmt.Errorf("StreamBus is not running")
	}
	if !s.currentPosition.IsValid() {
		s.currentPosition = NewStreamPosition(endDate, 0)
	}
	target := NewStreamTarget(s.currentPosition.Date, endDate)
	return s.streamWithinTarget(ctx, target)
}

// StreamBetween: [startDate, endDate] 구간 스트림
func (s *DexStreamBus) StreamBetween(ctx context.Context, startDate, endDate string) error {
	if !s.isRunning {
		return fmt.Errorf("StreamBus is not running")
	}
	target := NewStreamTarget(startDate, endDate)
	if !target.IsValid() {
		return fmt.Errorf("invalid date range: %s to %s", startDate, endDate)
	}
	s.currentPosition = NewStreamPosition(startDate, 0)
	log.Printf("Starting to stream (bus) from %s to %s", startDate, endDate)
	return s.streamWithinTarget(ctx, target)
}

func (s *DexStreamBus) streamWithinTarget(ctx context.Context, target StreamTarget) error {
	dates, err := target.GetDateRange()
	if err != nil {
		return fmt.Errorf("failed to get date range: %w", err)
	}
	log.Printf("Streaming %d days of data into EventBus", len(dates))

	positionTicker := time.NewTicker(s.saveInterval)
	defer positionTicker.Stop()

	for _, date := range dates {
		select {
		case <-s.stopChan:
			log.Printf("Streaming stopped by user")
			return nil
		default:
		}

		if s.currentPosition.Date != date {
			s.currentPosition.Date = date
			s.currentPosition.Index = 0
		}

		log.Printf("Streaming (bus) for date: %s, start index: %d",
			s.currentPosition.Date, s.currentPosition.Index)

		if err := s.streamDate(ctx, date, positionTicker); err != nil {
			return fmt.Errorf("failed to stream date %s: %w", date, err)
		}

		s.currentPosition.Index = 0
	}
	next, err := s.getNextDate(target.EndDate)
	if err == nil {
		s.currentPosition.Date = next
		s.currentPosition.Index = 0
		if err := s.saveCurrentPosition(); err != nil {
			log.Printf("Error saving final completed position: %v", err)
		}
	}
	log.Printf("Steam Bus: Completed streaming target range into EventBus")
	return nil
}

func (s *DexStreamBus) streamFromCurrentPosition(ctx context.Context) error {
	if !s.currentPosition.IsValid() {
		return fmt.Errorf("invalid current position: %v", s.currentPosition)
	}
	log.Printf("Starting to stream (bus) from position: %s", s.currentPosition.String())

	positionTicker := time.NewTicker(s.saveInterval)
	defer positionTicker.Stop()

	for {
		select {
		case <-s.stopChan:
			log.Printf("Streaming stopped by user")
			return nil
		default:
		}

		if err := s.streamDate(ctx, s.currentPosition.Date, positionTicker); err != nil {
			log.Printf("Error streaming date %s: %v", s.currentPosition.Date, err)
			if nextDate, err := s.getNextDate(s.currentPosition.Date); err == nil {
				s.currentPosition.Date = nextDate
				s.currentPosition.Index = 0
				log.Printf("Moving to next date: %s", nextDate)
			} else {
				log.Printf("Cannot determine next date, stopping: %v", err)
				return err
			}
		}
	}
}

func (s *DexStreamBus) streamDate(ctx context.Context, date string, positionTicker *time.Ticker) error {
	// 날짜 단위 타이밍/카운터
	dayStart := time.Now()
	var publishedThisDate uint64
	for {
		select {
		case <-s.stopChan:
			// 중단 시점에서도 지금까지 수집한 수치 출력 (부분 완료)
			elapsed := time.Since(dayStart)
			rate := 0.0
			if sec := elapsed.Seconds(); sec > 0 {
				rate = float64(publishedThisDate) / sec
			}
			log.Printf("Stream bus: Streaming stopped by user (date=%s) — published %d tx in %s (%.2f tx/s)",
				date, publishedThisDate, elapsed, rate)
			return nil
		case <-ctx.Done():
			log.Printf("StreamBus: ctx에 의해 종료됨 \n")
			return nil
		case <-positionTicker.C:
			if err := s.saveCurrentPosition(); err != nil {
				log.Printf("Error saving position: %v", err)
			}
		default:
		}

		// 1만건 배치씩 읽기(트랜잭션 경로 동일)
		transactions, err := s.txStreamer.GetTenThousandsTx(date, s.currentPosition.Index)
		if err != nil {
			return fmt.Errorf("failed to get transactions: %w", err)
		}
		if len(transactions) == 0 {
			elapsed := time.Since(dayStart)
			rate := 0.0
			if sec := elapsed.Seconds(); sec > 0 {
				rate = float64(publishedThisDate) / sec
			}
			log.Printf("Stream bus - Completed streaming date: %s — published %d tx in %s (%.2f tx/s)",
				date, publishedThisDate, elapsed, rate)
			return nil
		}

		// 이벤트버스로 밀어넣기: 버퍼가 찰 때까지 최대한 채움
		if err := s.publishToBus(ctx, transactions); err != nil {
			atomic.AddUint64(&s.totalErrors, 1)
			return fmt.Errorf("failed to publish to stream bus: %w", err)
		}

		// 포지션 갱신(다음 읽을 인덱스)
		s.currentPosition.Index += int64(len(transactions))
	}
}

func (s *DexStreamBus) publishToBus(ctx context.Context, batch []shareddomain.RawTransaction) error {
	// EventBus.Publish 가 버퍼가 꽉 차면 블록된다고 가정.
	// 이 경우 자연스럽게 컨슈머가 빼갈 때까지 대기하면서 “큐를 꽉 채우는” 동작이 된다.
	for i := range batch {
		if err := s.bus.Enqueue(ctx, batch[i]); err != nil {
			// Publish 가 에러를 반환한다면 중단
			return err
		}
	}
	atomic.AddUint64(&s.totalPublished, uint64(len(batch)))
	log.Printf("[Stream Bus] Published %d transactions (total=%d)", len(batch), atomic.LoadUint64(&s.totalPublished))
	log.Printf("[BusBackpressure] rawtx queue %d/%d", s.bus.Len(), s.bus.Cap())
	return nil
}

func (s *DexStreamBus) saveCurrentPosition() error {
	if !s.currentPosition.IsValid() {
		return fmt.Errorf("invalid current position")
	}
	return s.positionRepo.SavePosition(s.currentPosition)
}

func (s *DexStreamBus) getNextDate(currentDate string) (string, error) {
	date, err := time.Parse("2006-01-02", currentDate)
	if err != nil {
		return "", fmt.Errorf("invalid date format: %w", err)
	}
	nextDate := date.AddDate(0, 0, 1)
	return nextDate.Format("2006-01-02"), nil
}

// GetCurrentPosition 현재 포지션
func (s *DexStreamBus) GetCurrentPosition() StreamPosition { return s.currentPosition }

// IsRunning 실행 여부
func (s *DexStreamBus) IsRunning() bool { return s.isRunning }

// GetStats 상태/통계
func (s *DexStreamBus) GetStats() map[string]any {
	return map[string]any{
		"is_running":       s.isRunning,
		"current_position": s.currentPosition,
		"bus_path":         s.busRelPath,
		"total_published":  atomic.LoadUint64(&s.totalPublished),
		"total_errors":     atomic.LoadUint64(&s.totalErrors),
	}
}
