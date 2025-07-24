package txingester

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring"

// Infrastructure provides access to external services for txIngester module
type Infrastructure struct {
	CCEService      CCEService
	ConsumerMonitor monitoring.TPSMeter // Consumer TPS 모니터링을 위한 TPSMeter
	ProducerMonitor monitoring.TPSMeter // Producer TPS 모니터링을 위한 TPSMeter
}

// NewInfrastructure creates a new Infrastructure with CCE service dependency
func NewInfrastructure(cceService CCEService, consumerMonitor, producerMonitor monitoring.TPSMeter) *Infrastructure {
	return &Infrastructure{
		CCEService:      cceService,
		ConsumerMonitor: consumerMonitor,
		ProducerMonitor: producerMonitor,
	}
}
