package app

import (
	"context"
	"fmt"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/cface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/protocol"
	ldomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/pface"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type TransactionJob struct {
	tx              shareddomain.MarkedTransaction
	tripletAnalyzer *TripletAnalyzer
	workerID        int
}

// ! JobModel을 TransactionJob에 강제
var _ JobModel = (*TransactionJob)(nil)

// NewTransactionJob creates a new transaction processing job
func NewTransactionJob(tx shareddomain.MarkedTransaction, analyzer *TripletAnalyzer, workerID int) TransactionJob {
	return TransactionJob{
		tx:              tx,
		tripletAnalyzer: analyzer,
		workerID:        workerID,
	}
}

// Do implements workerpool.Job interface
func (j TransactionJob) Do(ctx context.Context) error {
	tx := j.tx
	tripletAnalyzer := j.tripletAnalyzer
	if isValidTx := checkTransactionSyntax(tx); !isValidTx {
		return nil
	}
	tripletSignal := j.decideTripletSignal(tx, tripletAnalyzer.relPool.GetDxanPort().GetNodPort().GetEoPort())
	switch tripletSignal.kind {
	case TxFromCEX:
		return j.ignoreTxFromCexOrTxFromDeposit()
	case TxToCEX:
		return j.handleTxToCEX(tripletSignal, j.tripletAnalyzer.relPool, j.tripletAnalyzer.infraPort.GetTimeBucketManager())
	case TxFromDepositWithoutCex:
		return j.ignoreTxFromCexOrTxFromDeposit()
	case TxToDepostFromUser:
		return j.handleTxToDepositFromUser(tripletSignal, j.tripletAnalyzer.relPool)
	case TxUndefined:
		return j.handleTxUndefined(tripletSignal, j.tripletAnalyzer.infraPort.GetTimeBucketManager())
	default:
		return fmt.Errorf("handleTripletEvent: 매칭되는 이벤트를 받지 못함.")
	}
}

func checkTransactionSyntax(tx shareddomain.MarkedTransaction) (isValidSyntax bool) {
	if tx.TxSyntax[0] != shareddomain.EOAMark || tx.TxSyntax[1] != shareddomain.EOAMark {
		isValidSyntax = false
		return
	}
	isValidSyntax = true
	return true
}

// decideTripletSignal는 tx 받아서 트리플렛 커멘드 발행.
func (j TransactionJob) decideTripletSignal(tx shareddomain.MarkedTransaction, eo cface.EoPort) tripletSignal {
	fromAddr := tx.From
	toAddr := tx.To
	if eo.IsCex(fromAddr) {
		return tripletSignal{kind: TxFromCEX, tripletTx: tx}
	}
	if eo.IsCex(toAddr) {
		return tripletSignal{kind: TxToCEX, tripletTx: tx}
	}
	if isDeposit, _ := eo.IsDepositAddress(fromAddr); isDeposit {
		return tripletSignal{kind: TxFromDepositWithoutCex, tripletTx: tx}
	}
	if isDeposit, _ := eo.IsDepositAddress(toAddr); isDeposit {
		return tripletSignal{kind: TxToDepostFromUser, tripletTx: tx}
	}
	return tripletSignal{kind: TxUndefined}
}

func (j TransactionJob) handleTxToCEX(tSig tripletSignal, rel pface.RelPort, timeBuckerManager iface.TimeBuckerManagerPort) error {
	cex := ldomain.CexAddress(tSig.tripletTx.To)
	deposit := ldomain.DepositAddress(tSig.tripletTx.From)
	depositDetection := buildDepositDetectionEvt(cex, deposit, tSig.tripletTx.BlockTime)
	// 우선 eo에다가 deposit발견 정보 전파
	if err := rel.GetDxanPort().GetNodPort().GetEoPort().SaveDetectedDeposit(depositDetection); err != nil {
		return fmt.Errorf("handleTripeltEvent: TxToCEX에서 디포짓 저장 도중 에러 %w", err)
	}
	//이후 클러스터에 cex와 deposit의 연결을 전달해서 cex-deposit의 연결 저장
	cexAndDepositEvent := buildCexAndDepositEvent(cex, deposit, tSig.tripletTx)
	if err := j.dispatchTripletEvent(cexAndDepositEvent); err != nil {
		return fmt.Errorf("handleTripletEvent: TxToCEX에서 Cex-Depost의 트레이트를 roperepo에 저장 중 에러 발생. %w", err)
	}
	// 마지막으로 그동안 "버퍼링"해 두었던 relations들에서 이번에 발견된 deposit과 거래한 주소들을 가져와서 rel의 ropeRepo에 저장함
	// 프로그램은 디포짓을 발견하기 전까진 그 거래가 디포짓인지 아닌지 모르기에 버퍼에 저장해둠.
	// 그러다가 어떤 주소가 디포짓이라고 발견될 경우, 버퍼에서 해당 주소와 거래한 주소를 꺼내어 이것이 실은 디포짓과 거래했었다고 추가하는 것.
	fromScalas := timeBuckerManager.GetFromScalas(domain.Address(deposit))
	// 각  <deposit-from> 쌍에 대해서 해당 관계를 발행
	if len(fromScalas) > 0 {
		for _, fromScala := range fromScalas {
			user := ldomain.UserAddress(fromScala.FromAddress)
			dualEvent := buildDualEventWithFromScala(deposit, user, fromScala)
			if err := j.dispatchTripletEvent(dualEvent); err != nil {
				return fmt.Errorf("handleTripletEvent에서 new doposit- from scala users의 trait을 roperepo에 디스패치 중 에러 발생 %w", err)
			}
		}
	}
	return nil
}

func (j TransactionJob) handleTxToDepositFromUser(tSig tripletSignal, rel pface.RelPort) error {
	deposit := ldomain.DepositAddress(tSig.tripletTx.To)
	user := ldomain.UserAddress(tSig.tripletTx.From)
	dualEvent := buildDualEventWithTx(deposit, user, tSig.tripletTx)
	if err := j.dispatchTripletEvent(dualEvent); err != nil {
		return fmt.Errorf("handleTripletEvent에서 old deposit-user의 trait을 roperepo에 디스패치 중 에러 발생 %w", err)

	}
	return nil
}

func (j TransactionJob) handleTxUndefined(tSig tripletSignal, timeBucketManager iface.TimeBuckerManagerPort) error {
	toAddr := tSig.tripletTx.To
	fromAddr := tSig.tripletTx.From
	// 도메인 로직 위임: NewFromScala 사용
	fromScala := ldomain.NewFromScala(
		fromAddr,
		tSig.tripletTx.TxID,
		tSig.tripletTx.BlockTime,
		tSig.tripletTx.ValueOrNull,
	)
	timeBucketManager.AddIfAbsent(toAddr, fromScala)
	return nil
}

func (j TransactionJob) ignoreTxFromCexOrTxFromDeposit() error {
	return nil
}

func (j TransactionJob) dispatchTripletEvent(tripletEvent ropedomain.TraitEvent) error {
	return j.tripletAnalyzer.relPool.DispatchTraitEvent(tripletEvent)
}

// 여기서부턴 이벤트 빌드 계산들.

var buildDepositDetectionEvt buildDepositDetectionEvent = func(cex ldomain.CexAddress, deposit ldomain.DepositAddress, t chaintimer.ChainTime) domain.DepositDetection {
	return domain.DepositDetection{
		CEXAddress:     domain.Address(cex),
		DepositAddress: domain.Address(deposit),
		TxCount:        1,
		DetectedAt:     t,
	}
}

var buildCexAndDepositEvent buildCexAndDepositTraitEvent = func(cex ldomain.CexAddress, deposit ldomain.DepositAddress, tx domain.MarkedTransaction) ropedomain.TraitEvent {
	trait := protocol.TraitCexAndDeposit
	cexAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(cex),
		Rule:    protocol.RuleCex,
	}
	depositAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    protocol.RuleDeposit,
	}
	txScala := ropedomain.TxScala{
		TxId:        tx.TxID,
		Time:        tx.BlockTime,
		ScoreInc:    1,
		ValueOrNull: tx.ValueOrNull,
	}
	return ropedomain.NewTraitEvent(
		protocol.AuthorSystem,
		trait,
		cexAndRule,
		depositAndRule,
		txScala,
	)
}

var buildDualEventWithFromScala buildDualTraitEventWithFromScala = func(deposit ldomain.DepositAddress, user ldomain.UserAddress, fromScala ldomain.FromScala) ropedomain.TraitEvent {
	trait := protocol.TraitDepositAndUser
	depositAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    protocol.RuleDeposit,
	}
	userAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(user),
		Rule:    protocol.RuleUser,
	}
	txScala := ropedomain.TxScala{
		TxId:        fromScala.LastTxId,
		Time:        fromScala.LastTime,
		ScoreInc:    fromScala.Volume,
		ValueOrNull: fromScala.ValueOrNull,
	}
	return ropedomain.NewTraitEvent(
		protocol.AuthorSystem,
		trait,
		depositAndRule,
		userAndRule,
		txScala,
	)
}

var buildDualEventWithTx buildDualTraitEventWithTx = func(deposit ldomain.DepositAddress, user ldomain.UserAddress, tx domain.MarkedTransaction) ropedomain.TraitEvent {
	trait := protocol.TraitDepositAndUser
	depositRule := ropedomain.AddressAndRule{
		Address: domain.Address(deposit),
		Rule:    protocol.RuleDeposit,
	}
	userAndRule := ropedomain.AddressAndRule{
		Address: domain.Address(user),
		Rule:    protocol.RuleUser,
	}
	txScala := ropedomain.TxScala{
		TxId:        tx.TxID,
		Time:        tx.BlockTime,
		ScoreInc:    1,
		ValueOrNull: tx.ValueOrNull,
	}
	return ropedomain.NewTraitEvent(
		protocol.AuthorSystem,
		trait,
		depositRule,
		userAndRule,
		txScala,
	)
}
