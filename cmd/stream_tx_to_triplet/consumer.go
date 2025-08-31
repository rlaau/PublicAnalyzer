package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

func GetRawTxConsumer() *kafka.KafkaBatchConsumer[*domain.RawTransaction] {
	mode := mode.ProductionModeProcess
	name := "production-ingest"
	// Transaction Consumer 초기화 - 모드에 따라 다른 토픽 사용
	kafkaBrokers := []string{kafka.DefaultKafkaPort}
	isTestMode := mode.IsTest()
	groupID := fmt.Sprintf("triplet-analyzer-%s", strings.ReplaceAll(name, " ", "-"))
	// 배치 모드 Consumer 초기화 (고성능)
	batchSize := 100                      // 100개씩 배치 처리
	batchTimeout := 20 * time.Millisecond // 20ms 타임아웃
	var topic string
	if isTestMode {
		topic = kafka.TestFedTxTopic // 테스트용 토픽
	} else {
		topic = kafka.ProductionTxTopic // 프로덕션용 토픽
	}

	consumerConfig := kafka.KafkaBatchConfig{
		Brokers:      kafkaBrokers,
		Topic:        topic,
		GroupID:      groupID,
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
	}
	batchConsumer := kafka.NewKafkaBatchConsumer[*domain.RawTransaction](consumerConfig)
	log.Printf("📡 Batch consumer initialized (test mode: %v, batch size: %d)", isTestMode, batchSize)
	return batchConsumer
}
