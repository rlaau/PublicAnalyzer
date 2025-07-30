package app

import (
	"context"
	"io"

	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// EOAAnalyzer 인터페이스 - 테스트용과 프로덕션용 공통 인터페이스
// ! 두 구현체는 데이터 저장 방식과 생명주기에서만 차이가 있음
type EOAAnalyzer interface {
	// 분석기 생명주기 관리
	Start(ctx context.Context) error
	Stop() error

	// 트랜잭션 처리
	ProcessTransaction(tx *shareddomain.MarkedTransaction) error
	ProcessTransactions(txs []*shareddomain.MarkedTransaction) error

	// 상태 조회
	GetStatistics() map[string]interface{}
	IsHealthy() bool
	GetChannelStatus() (usage int, capacity int)

	// 리소스 관리
	io.Closer
}

// AnalyzerMode 분석기 모드 정의
type AnalyzerMode string

const (
	ProductionMode AnalyzerMode = "production" // 프로덕션 모드 - 영구 저장
	TestingMode    AnalyzerMode = "testing"    // 테스트 모드 - 임시 저장, 자동 정리
)

// EOAAnalyzerConfig 분석기 설정
type EOAAnalyzerConfig struct {
	// 기본 설정
	Name string       // 분석기 이름
	Mode AnalyzerMode // 동작 모드 (production/testing)

	// 성능 설정
	ChannelBufferSize int   // 채널 버퍼 크기
	WorkerCount       int   // 워커 고루틴 수
	MaxProcessingTime int64 // 트랜잭션 처리 최대 시간 (나노초)

	// 모니터링 설정
	StatsInterval       int64 // 통계 출력 간격 (나노초)
	HealthCheckInterval int64 // 헬스체크 간격 (나노초)

	// *FileDBPath는 모듈 데이터베이스의 루트 경로임
	// 로직: findRoot를 통해 루트를 찾고, 그 안에 새로운 데이터 폴더를 생성하거나 찾는 것.
	// 테스트 시엔 고립된 DB통해서 얻고, 상용 시엔 다른 path에서 얻기
	FileDBPath    string // 데이터 저장 경로
	GraphDBPath   string // 그래프 DB 경로
	PendingDBPath string // 펜딩 관계 DB 경로
	CEXFilePath   string // CEX 주소 파일 경로

	// 테스트 모드 전용 설정
	AutoCleanup     bool // 종료 시 자동 정리 여부
	ResultReporting bool // 결과 리포팅 여부
}

// ProductionConfig 프로덕션 모드 기본 설정
func ProductionConfig(name string) *EOAAnalyzerConfig {
	return &EOAAnalyzerConfig{
		Name:                name + "-Production",
		Mode:                ProductionMode,
		ChannelBufferSize:   200_000,
		WorkerCount:         8,
		MaxProcessingTime:   200_000_000,    // 200ms in nanoseconds
		StatsInterval:       60_000_000_000, // 60s in nanoseconds
		HealthCheckInterval: 30_000_000_000, // 30s in nanoseconds
		FileDBPath:          "data/ee",
		GraphDBPath:         "data/ee/graph",
		PendingDBPath:       "data/ee/pending",
		AutoCleanup:         false,
		ResultReporting:     false,
	}
}

// TestingConfig 테스트 모드 기본 설정
func TestingConfig(name string) *EOAAnalyzerConfig {
	return &EOAAnalyzerConfig{
		Name:                name + "-Testing",
		Mode:                TestingMode,
		ChannelBufferSize:   500,
		WorkerCount:         2,
		MaxProcessingTime:   100_000_000,    // 100ms in nanoseconds
		StatsInterval:       5_000_000_000,  // 5s in nanoseconds
		HealthCheckInterval: 10_000_000_000, // 10s in nanoseconds
		FileDBPath:          "test_data/ee",
		GraphDBPath:         "test_data/ee/graph",
		PendingDBPath:       "test_data/ee/pending",
		AutoCleanup:         true,
		ResultReporting:     true,
	}
}

// AnalyzerFactory 분석기 팩토리 함수
type AnalyzerFactory func(config *EOAAnalyzerConfig) (EOAAnalyzer, error)

// CreateAnalyzer 설정에 따라 적절한 분석기 생성
func CreateAnalyzer(config *EOAAnalyzerConfig, ctx context.Context) (EOAAnalyzer, error) {
	switch config.Mode {
	case ProductionMode:
		return NewProductionEOAAnalyzer(config, ctx)
	case TestingMode:
		return NewTestingEOAAnalyzer(config, ctx)
	default:
		return nil, ErrInvalidAnalyzerMode{Mode: string(config.Mode)}
	}
}

// 에러 타입 정의
type ErrInvalidAnalyzerMode struct {
	Mode string
}

func (e ErrInvalidAnalyzerMode) Error() string {
	return "invalid analyzer mode: " + e.Mode
}
