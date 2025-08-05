package app

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

// TransactionJob implements workerpool.Job interface for transaction processing
type TransactionJob struct {
	tx       *shareddomain.MarkedTransaction
	analyzer *SimpleEOAAnalyzer
	workerID int
}

// NewTransactionJob creates a new transaction processing job
func NewTransactionJob(tx *shareddomain.MarkedTransaction, analyzer *SimpleEOAAnalyzer, workerID int) *TransactionJob {
	return &TransactionJob{
		tx:       tx,
		analyzer: analyzer,
		workerID: workerID,
	}
}

// Do implements workerpool.Job interface
func (j *TransactionJob) Do(ctx context.Context) error {
	return j.analyzer.processSingleTransactionJob(j.tx, j.workerID)
}

// processSingleTransactionJob processes a single transaction (refactored from processSingleTransaction)
func (a *SimpleEOAAnalyzer) processSingleTransactionJob(tx *shareddomain.MarkedTransaction, workerID int) error {
	processedCount := atomic.AddInt64(&a.stats.TotalProcessed, 1)

	// 처음 몇 개 트랜잭션은 디버깅 로그 출력
	if processedCount <= 5 {
		log.Printf("🔄 Worker %d: processing tx #%d | From: %s | To: %s",
			workerID, processedCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	}

	// EOA-EOA 트랜잭션만 처리
	//TODO 이건 추후 제거 가능. 어차피 EE트랜잭션만 카프카 큐에 보내줄 거라서..
	//TODO 뭐, 놔둬도 상관 없긴 함.
	if tx.TxSyntax[0] != shareddomain.EOAMark || tx.TxSyntax[1] != shareddomain.EOAMark {
		if processedCount <= 5 {
			log.Printf("⏭️  Worker %d: skipping non-EOA tx #%d", workerID, processedCount)
		}
		return nil
	}

	// DualManager를 통한 트랜잭션 처리
	//* 모나드 처리로 로직 간명화
	_, err := fp.NewMonadFlow[*domain.MarkedTransaction]().
		RegisterInput(tx).
		Then(a.dualManager.CheckTransaction).
		Then(a.dualManager.HandleAddress).
		Then(a.dualManager.AddToWindowBuffer).
		Run()

	if err != nil {
		atomic.AddInt64(&a.stats.ErrorCount, 1)
		errorCount := atomic.LoadInt64(&a.stats.ErrorCount)
		if errorCount <= 5 { // 처음 5개 에러는 모두 로깅 (디버깅용)
			log.Printf("⚠️ Worker %d: processing error #%d: %v | From: %s | To: %s",
				workerID, errorCount, err, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
		} else if errorCount%20 == 1 { // 이후에는 20번째마다 로깅
			log.Printf("⚠️ Worker %d: processing error #%d: %v", workerID, errorCount, err)
		}
		return err
	}

	successCount := atomic.AddInt64(&a.stats.SuccessCount, 1)

	// 처음 몇 개 성공은 로깅
	if successCount <= 5 {
		log.Printf("✅ Worker %d: success #%d | From: %s | To: %s",
			workerID, successCount, tx.From.String()[:10]+"...", tx.To.String()[:10]+"...")
	}

	//TODO 배포 환경에선 제거 가능 or 개량 가능
	//TODO 동일 로직 중복 처리 및 분석이라 성능 저하 가능
	a.analyzeTransactionResult(tx)

	return nil
}
