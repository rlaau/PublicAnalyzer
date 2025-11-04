package app

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/nod/cface"
	ldomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/iface"
	"github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/pface"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
	ropedomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// a_model.go: app이 지녀야 할 핵심 기능을 모델링 함
// 가능한 도메인 용어만을 사용. 컴퓨터나 인프라 관련은 모델링에서 다루지 않음
// 오직 핵심 동작이 무엇인지만을 전달하기 위함임. (이것만으로도 70%의 동작은 설명 가능)
// 그럼에서 ctx등 CS 용어가 들어가는 것은 어쩔 수 없음.
// 이 파일에서 상태를 지니는 클래스는 정의하지 않음. 최대한 데이터와 동작 위주로 기술하기 위함임.
// 그런 점에서 struct는 오직 DTO수준의 정적 데이터 정의용으로만 사용해야 함. 그래야 도메인 로직에 집중 가능

// TripletModel은 TripletModelriplet이 지녀야 할 가장 핵심 도메인 로직
type TripletModel interface {
	// processTransaction은 go루틴 상에서 지속적으로
	//(트랜잭션 소비->job으로 변환->job디스패치)를 함
	processTransaction(ctx context.Context) error

	//processTransaction은 다음 동작으로 구성됨
	//getMyTx는 upstream(여기선 relpool)로부터 트랜잭션 받아옴
	getMyTx() <-chan domain.MarkedTransaction
	// makeTransactionJob은 받아온 tx로부터 txJob생성
	makeTransactionJob(tx domain.MarkedTransaction) TransactionJob
	//dispatchJob은 만들어진 txJob을 자신 내부 워커풀에 전달
	dispatchJob(ctx context.Context, job TransactionJob) error
}

// Job이 지녀야 할 핵심 비즈니스 로직 모델
type JobModel interface {
	//Job의 Do는 다음과 같이 구성됨

	//decideTripletSignal은 tx받아서 kind를 판별 후 tripletSignal을 리턴함
	decideTripletSignal(tx domain.MarkedTransaction, eo cface.EoPort) tripletSignal

	//handleXXX는 판별된 종류에 따라 만들어진 cmd를 받아서 할 일 함
	//handleTxToCEX는 CEX로 송금된 내역을 바탕으로, 송금자가 Deposit임을 밝혀내어 이를 건너 모듈에 전파함.
	//이후 자신의 타임버킷에서 버퍼링된 트랜잭션을 바탕으로 이벤트를 만들어 발행함.
	handleTxToCEX(tSig tripletSignal, rel pface.RelPort, timeBuckerManager iface.TimeBuckerManagerPort) error
	//handleTxToDepositFromUser는 User가 Deposit에 송금한 트랜잭션에 이벤트를 만들어 발행함.
	handleTxToDepositFromUser(tSig tripletSignal, rel pface.RelPort) error
	//handleTxUndefined는 아직 정체 모르는 tx에 대해선 타임버킷에 버퍼링함.
	handleTxUndefined(tSig tripletSignal, timeBucketManager iface.TimeBuckerManagerPort) error
	//ignoreTxFromCexOrTxFromDeposit는 TxFromCEX, TxFromDeposit에 대해서 아무 행동도 하지 않음
	ignoreTxFromCexOrTxFromDeposit() error

	//dispatchTripletEvent는 handleXXX가 이벤트 발행 시 호출됨.
	dispatchTripletEvent(tripletEvent ropedomain.TraitEvent) error
}

// tripletSignal는 트랜잭션의 종류를 판단해서 신호를 보냄.
type tripletSignal struct {
	kind      tripletKind
	tripletTx domain.MarkedTransaction
}

// tripletKind는 triplet app에서 처리하는 트랜잭션의 종류임.
type tripletKind int

// tripletKind의 enum들
const (
	TxFromCEX tripletKind = iota
	TxToCEX
	TxFromDepositWithoutCex
	TxToDepostFromUser //Dual == depositAddr-userAddr의 연결
	TxUndefined
)

// buildDepositDetectionEvent는 handleTxToCEX과정에서 Deposit발견 이벤트를 만들어냄.
type buildDepositDetectionEvent = func(cex ldomain.CexAddress, deposit ldomain.DepositAddress,
	t chaintimer.ChainTime) domain.DepositDetection

// buildCexAndDepositTraitEvent는 handleTxToCEX과정에서 새로운 CEX, Deposit관계 생성에 대한 이벤트 만들어냄.
type buildCexAndDepositTraitEvent = func(cex ldomain.CexAddress, deposit ldomain.DepositAddress,
	tx domain.MarkedTransaction) ropedomain.TraitEvent

// buildDualTraitEventWithFromScala는 handleTxToDepositFromUser,
// 혹은 handleTxToCEX 과정에서 Deposit-User의 관계 생성에 대한 이벤트 만들어냄.
type buildDualTraitEventWithFromScala = func(deposit ldomain.DepositAddress, user ldomain.UserAddress,
	fromScala ldomain.FromScala) ropedomain.TraitEvent

// buildDualTraitEventWithTx는 handleTxToDepositFromUser,
// 혹은 handleTxToCEX 과정에서 Deposit-User의 관계 생성에 대한 이벤트 만들어냄.
type buildDualTraitEventWithTx = func(deposit ldomain.DepositAddress, user ldomain.UserAddress,
	tx domain.MarkedTransaction) ropedomain.TraitEvent
