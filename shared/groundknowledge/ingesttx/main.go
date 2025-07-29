package main

import (
	"log"
)

func main() {
	// 2025-01-01부터 2025-06-01까지 트랜잭션 데이터 수집
	startDate := "2025-01-01"
	endDate := "2025-06-01"
	projectID := "whaletracker-453817"

	ingester := NewParallelTransactionIngester(projectID, startDate, endDate)

	// 예상 트랜잭션 수 확인
	log.Printf("예상 트랜잭션 수 확인 중...")
	count, err := ingester.GetEstimatedTransactionCount()
	if err != nil {
		log.Printf("WARNING: 예상 트랜잭션 수 확인 실패: %v", err)
		log.Printf("데이터 수집을 계속 진행합니다...")
	} else {
		log.Printf("예상 트랜잭션 수: %d개", count)
	}

	// 트랜잭션 데이터 병렬 수집
	log.Printf("트랜잭션 데이터 수집 시작: %s ~ %s", startDate, endDate)
	if err := ingester.IngestTransactionsParallel(); err != nil {
		log.Fatalf("트랜잭션 데이터 수집 실패: %v", err)
	}

	log.Printf("트랜잭션 데이터 수집 완료!")
}
