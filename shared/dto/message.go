package dto

import (
	"time"

	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// FedTxMessage fed-tx 토픽용 테스트 메시지 구조체
// TxFeeder → EE Analyzer 테스트 통신용 (간소화됨)
type FedTxMessage struct {
	Transaction *shareddomain.MarkedTransaction `json:"transaction"`
	Timestamp   time.Time                       `json:"timestamp"`
}

// IngestedTransactionMessage ingested-transactions 토픽용 프로덕션 메시지 구조체
// TxIngester → 각 Analyzer 모듈 통신용 (확장된 버전)
type IngestedTransactionMessage struct {
	MessageID    string                          `json:"message_id"`    // 메시지 고유 ID
	Transaction  *shareddomain.MarkedTransaction `json:"transaction"`   // 트랜잭션 데이터
	Timestamp    time.Time                       `json:"timestamp"`     // 메시지 생성 시간
	SourceModule string                          `json:"source_module"` // 발송 모듈 (예: "TxIngester")
	Priority     MessagePriority                 `json:"priority"`      // 메시지 우선순위
	Metadata     TransactionMetadata             `json:"metadata"`      // 확장 메타데이터
}

// MessagePriority 메시지 우선순위
type MessagePriority string

const (
	PriorityHigh   MessagePriority = "HIGH"   // 높은 우선순위 (의심 거래 등)
	PriorityMedium MessagePriority = "MEDIUM" // 중간 우선순위 (일반 거래)
	PriorityLow    MessagePriority = "LOW"    // 낮은 우선순위 (배치 처리)
)

// TransactionMetadata 트랜잭션 확장 메타데이터
type TransactionMetadata struct {
	BatchID       string            `json:"batch_id,omitempty"`       // 배치 ID
	RetryCount    int               `json:"retry_count"`              // 재시도 횟수
	ProcessingTag string            `json:"processing_tag,omitempty"` // 처리 태그
	CustomData    map[string]string `json:"custom_data,omitempty"`    // 커스텀 데이터
}

// TestCleanupMessage 테스트 정리용 메시지
type TestCleanupMessage struct {
	Command   string    `json:"command"` // "CLEANUP"
	Timestamp time.Time `json:"timestamp"`
}
