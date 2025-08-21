package kafka

// KafkaTopics 카프카 토픽 상수
const (
	DefaultKafkaPort  = "localhost:9092"
	TestFedTxTopic    = "fed-tx"        // 테스트용 토픽
	ProductionTxTopic = "production-tx" //"ingested-transactions" // 프로덕션용 토픽
	TestingTxTopic    = "testing-tx"
	TestControlTopic  = "test-control" // 테스트 제어용 토픽
)
