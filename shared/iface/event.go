// iface/event.go
package iface

import (
	"fmt"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/chaintimer"
)

type ModuleName string

func ParseModuleName(s string) ModuleName { return ModuleName(s) }

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

type Event interface {
	TraceID() (TraceID, error)
	OccurredAt() time.Time
	OccurredAtChainTime() chaintimer.ChainTime
	Message() EventMessage
}

type DomainEvent interface {
	Event
	ModuleName() ModuleName
}

type GlobalEvent interface {
	Event
	BrokerName() BrokerName
}
