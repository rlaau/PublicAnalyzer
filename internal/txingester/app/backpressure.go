package app

import "github.com/rlaaudgjs5638/chainAnalyzer/shared/monitoring/meter/cntmtr"

type SpeedSignal struct {
	ConsumingIntervalSecond int
	ConsumingPerInterval    int
}

type CoutingBackpressure interface {
	CountConsuming()
	CountConsumings(i int)
	CountProducing()
	CountProducings(i int)
	RegisterBackpressureChannel(c chan SpeedSignal)
	MakeBackpressureEvent()
	decideSingnaling() SpeedSignal
}

type KafkaCountingBackpressure struct {
	// offsetMeter는 
	offsetMeter *cntmtr.IntCountMeter
}
