package txingester

import (
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/kafka"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/monitor/kfkmtr"
)

// Infrastructure provides access to external services for txIngester module
type Infrastructure struct {
	CCEService        CCEService
	KafkaMonitor      kfkmtr.KafkaMonitor
	TransactionSource string
	KafkaProducer     kafka.Producer[domain.MarkedTransaction]      // 제너릭 Producer 사용
	BatchProducer     kafka.BatchProducer[domain.MarkedTransaction] // 제너릭 배치 프로듀서

}

// NewInfrastructure creates a new Infrastructure with CCE service dependency
func NewInfrastructure(cceService CCEService, kafkaMonitor kfkmtr.KafkaMonitor, txSrc string, kafkaProducer *kafka.KafkaProducer[domain.MarkedTransaction], batchProducer *kafka.KafkaBatchProducer[domain.MarkedTransaction]) *Infrastructure {
	return &Infrastructure{
		CCEService:        cceService,
		KafkaMonitor:      kafkaMonitor,
		TransactionSource: txSrc,
		KafkaProducer:     kafkaProducer,
		BatchProducer:     batchProducer,
	}
}
