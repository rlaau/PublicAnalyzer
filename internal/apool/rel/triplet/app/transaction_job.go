package app

import (
	"context"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/fp"
)

// TransactionJob implements workerpool.Job interface for transaction processing
type TransactionJob struct {
	tx              shareddomain.MarkedTransaction
	tripletAnalyzer *TripletAnalyzer
	workerID        int
}

// NewTransactionJob creates a new transaction processing job
func NewTransactionJob(tx shareddomain.MarkedTransaction, analyzer *TripletAnalyzer, workerID int) *TransactionJob {
	return &TransactionJob{
		tx:              tx,
		tripletAnalyzer: analyzer,
		workerID:        workerID,
	}
}

// Do implements workerpool.Job interface
func (j *TransactionJob) Do(ctx context.Context) error {
	return j.tripletAnalyzer.processSingleTransactionJob(j.tx, j.workerID)
}

// processSingleTransactionJob processes a single transaction (refactored from processSingleTransaction)
func (a *TripletAnalyzer) processSingleTransactionJob(tx shareddomain.MarkedTransaction, workerID int) error {
	// EOA-EOA 트랜잭션만 처리

	if tx.TxSyntax[0] != shareddomain.EOAMark || tx.TxSyntax[1] != shareddomain.EOAMark {
		return nil
	}

	_, err := fp.NewMonadFlow[domain.MarkedTransaction]().
		RegisterInput(tx).
		Then(a.dualManager.CheckTransaction).
		Then(a.dualManager.HandleAddress).
		Then(a.dualManager.AddToWindowBuffer).
		Run()
	if err != nil {
		log.Printf("Triplet WorkerPool 에러 발생: %s", err.Error())
		return err
	}

	return nil
}
