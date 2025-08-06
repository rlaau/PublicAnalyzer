package kafka

import (
	"log"
	"sync"
)

// 글로벌 Kafka 설정
var (
	globalBrokers   []string
	brokersMutex    sync.RWMutex
	brokersInitOnce sync.Once
)

// SetGlobalBrokers 글로벌 브로커 설정
func SetGlobalBrokers(brokers []string) {
	brokersMutex.Lock()
	defer brokersMutex.Unlock()
	globalBrokers = brokers
	log.Printf("[Kafka] Global brokers set to: %v", brokers)
}

// GetGlobalBrokers 글로벌 브로커 반환 (기본값: localhost:9092)
func GetGlobalBrokers() []string {
	brokersInitOnce.Do(func() {
		if len(globalBrokers) == 0 {
			globalBrokers = []string{"localhost:9092"}
			log.Printf("[Kafka] Using default brokers: %v", globalBrokers)
		}
	})

	brokersMutex.RLock()
	defer brokersMutex.RUnlock()
	return globalBrokers
}
