package app

import (
	"context"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/txingester"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
)

// TestingIngester는 테스트용 트랜잭션을 수집하는 앱임.
// 인터페이스화 해서 테스트/상용환경 갈아끼우기 용이하게 구성
type TxIngester interface {
	IngestTransaction(context.Context) error
	CheckContractCreation(domain.RawTransaction) bool
	MarkTransaction(domain.RawTransaction) domain.MarkedTransaction
}

func NewTxIngester(isTesting bool, kafkaClient *kafka.Client, cceService txingester.CCEService) TxIngester {

	if isTesting {
		return NewTestingIngester(kafkaClient, cceService)
	} else {
		panic("실제 트랜잭션 수집 앱은 아직 구현되지 않았습니다.")
	}
}
