package app

import (
	"context"
	"fmt"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/nod/cface"
	ldomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/apool/rel/triplet/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

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
	tx := j.tx
	tripletAnalyzer := j.tripletAnalyzer
	if isValidTx := checkTransactionSyntax(tx); !isValidTx {
		return nil
	}
	tripletEvent := decideTripletEvent(tx, tripletAnalyzer.relPool.GetApooPort().GetNodPort().GetEoPort())
	errOrNil := handleTripletEvent(tripletEvent, tripletAnalyzer.relPool, tripletAnalyzer.infraPort.GetTimeBucketManager())

	return errOrNil
}

func checkTransactionSyntax(tx shareddomain.MarkedTransaction) (isValidSyntax bool) {
	if tx.TxSyntax[0] != domain.EOAMark || tx.TxSyntax[1] != domain.EOAMark {
		isValidSyntax = false
		return
	}
	isValidSyntax = true
	return true
}

type tripletEvent struct {
	kind tripletKind
	tx   domain.MarkedTransaction
}
type tripletKind int

const (
	TxFromCEX tripletKind = iota
	TxToCEX
	TxFromDeposit
	TxToDepostFromUser //Dual == depositAddr-userAddr의 연결
	TxNotSpecial
)

// handleAddrss의 파생1
func decideTripletEvent(tx shareddomain.MarkedTransaction, eo cface.EoPort) tripletEvent {
	fromAddr := tx.From
	toAddr := tx.To
	if eo.IsCex(fromAddr) {
		return tripletEvent{kind: TxFromCEX, tx: tx}
	}
	if eo.IsCex(toAddr) {
		return tripletEvent{kind: TxToCEX, tx: tx}
	}
	if isDeposit, _ := eo.IsDepositAddress(fromAddr); isDeposit {
		return tripletEvent{kind: TxFromDeposit, tx: tx}
	}
	if isDeposit, _ := eo.IsDepositAddress(toAddr); isDeposit {
		return tripletEvent{kind: TxToDepostFromUser, tx: tx}
	}
	return tripletEvent{kind: TxNotSpecial}
}

// handleAddrss의 파생2
func handleTripletEvent(tev tripletEvent, rel pface.RelPort, timeBuckerManager iface.TimeBuckerManager) error {
	switch tev.kind {
	case TxFromCEX:
		return nil
	case TxToCEX:
		//to Addr이 CEX인 겨웅, from Addr을 새롭게 발견된 Deposit으로 취급함.
		cex := ldomain.CEX(tev.tx.To)
		deposit := ldomain.Deposit(tev.tx.From)
		depositDetection := ldomain.BuildDepositDetection(cex, deposit, tev.tx.BlockTime)
		// 우선 eo에다가 deposit발견 정보 전파
		if err := rel.GetApooPort().GetNodPort().GetEoPort().SaveDetectedDeposit(depositDetection); err != nil {
			return fmt.Errorf("handleTripeltEvent: TxToCEX에서 디포짓 저장 도중 에러 %w", err)
		}
		//이후 클러스터에 cex와 deposit의 연결을 전달해서 cex-deposit의 연결 저장
		cexAndDepositEvent := ldomain.BuildCexAndDepositEvent(cex, deposit, tev.tx)
		if err := rel.DispatchTraitEvent(cexAndDepositEvent); err != nil {
			return fmt.Errorf("handleTripletEvent: TxToCEX에서 Cex-Depost의 트레이트를 roperepo에 저장 중 에러 발생. %w", err)
		}
		// 마지막으로 그동안 "버퍼링"해 두었던 relations들에서 이번에 발견된 deposit과 거래한 주소들을 가져와서 rel의 ropeRepo에 저장함
		// 프로그램은 디포짓을 발견하기 전까진 그 거래가 디포짓인지 아닌지 모르기에 버퍼에 저장해둠.
		// 그러다가 어떤 주소가 디포짓이라고 발견될 경우, 버퍼에서 해당 주소와 거래한 주소를 꺼내어 이것이 실은 디포짓과 거래했었다고 추가하는 것.
		fromScalas := timeBuckerManager.GetFromScalas(domain.Address(deposit))
		// 각  <deposit-from> 쌍에 대해서 해당 관계를 발행
		if len(fromScalas) > 0 {
			for _, fromScala := range fromScalas {
				user := ldomain.User(fromScala.FromAddress)
				dualEvent := ldomain.BuildDualEventWithFromScala(deposit, user, fromScala)
				if err := rel.DispatchTraitEvent(dualEvent); err != nil {
					return fmt.Errorf("handleTripletEvent에서 new doposit- from scala users의 trait을 roperepo에 디스패치 중 에러 발생 %w", err)
				}
			}
		}
	case TxFromDeposit:
		return nil
	case TxToDepostFromUser:
		deposit := ldomain.Deposit(tev.tx.To)
		user := ldomain.User(tev.tx.From)
		dualEvent := ldomain.BuildDualEventWithTx(deposit, user, tev.tx)
		if err := rel.DispatchTraitEvent(dualEvent); err != nil {
			return fmt.Errorf("handleTripletEvent에서 old deposit-user의 trait을 roperepo에 디스패치 중 에러 발생 %w", err)

		}
	case TxNotSpecial:
		toAddr := tev.tx.To
		fromAddr := tev.tx.From
		// 도메인 로직 위임: NewFromScala 사용
		fromScala := ldomain.NewFromScala(
			fromAddr,
			tev.tx.TxID,
			tev.tx.BlockTime,
			tev.tx.ValueOrNull,
		)
		timeBuckerManager.AddIfAbsent(toAddr, fromScala)
		return nil
	default:
		return fmt.Errorf("handleTripletEvent: 매칭되는 이벤트를 받지 못함.")
	}
	return nil
}
