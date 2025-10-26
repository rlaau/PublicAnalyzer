package app

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

// TripletConfig 분석기 설정
type TripletConfig struct {
	// 기본 설정
	Name string              // 분석기 이름
	Mode mode.ProcessingMode // 동작 모드 (production/testing)

	// 성능 설정
	WorkerCount int // 워커 고루틴 수
}

// AnalyzerFactory 분석기 팩토리 함수
type AnalyzerFactory func(config *TripletConfig) (TripletAnalyzerDesign, error)
