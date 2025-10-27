package gface

import (
	"fmt"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
)

type Event interface {
	OccurredAt() time.Time
	Message() EventMessage
}

type Tracable interface {
	TraceID() (TraceID, error)
}

type ChainEvent interface {
	Event
	OccurredAtChainTime() chaintimer.ChainTime
}
type DomainEvent interface {
	Event
	ModuleName() ModuleName
}

type GlobalEvent interface {
	Event
	BrokerName() BrokerName
}

type BrokerName string

func ParseBrokerName(s string) BrokerName { return BrokerName(s) }

type TraceID string

func ParseTraceID(s string) (TraceID, error) {
	if s == "" {
		return "", fmt.Errorf("trace id is empty")
	}
	return TraceID(s), nil
}

type EventMessage string

func ParseEventMessage(eventName string, t time.Time, msg string) EventMessage {
	return EventMessage(fmt.Sprintf("%s %s %s", eventName, t.Format(time.RFC3339Nano), msg))
}
