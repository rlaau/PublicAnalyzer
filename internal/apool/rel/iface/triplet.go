package iface

import (
	"context"
	"io"

	"github.com/dgraph-io/badger/v4"
	ropeapp "github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/app"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

type TripletEventMsg any
type TripletPort interface {
	// 분석기 생명주기 관리
	Start(ctx context.Context) error
	Stop() error

	// 트랜잭션 처리
	ProcessTransaction(tx *shareddomain.MarkedTransaction) error
	ProcessTransactions(txs []*shareddomain.MarkedTransaction) error

	// 상태 조회
	GetStatistics() map[string]any
	IsHealthy() bool
	GetChannelStatus() (usage int, capacity int)
	//그래, 맞다. 인터페이스는 이따구로 쓰면 안되지/
	//근데 이거 고치려면 또 리팩토링 해야함. 또!!!
	//그건 나중에 하자고.
	GraphDB() *badger.DB
	RopeDB() ropeapp.RopeDB

	GetRopeDBStats() map[string]any

	// 리소스 관리
	io.Closer
}
