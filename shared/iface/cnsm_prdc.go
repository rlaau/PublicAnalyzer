package iface

import "context"

type Closable interface {
	Close() error
	Closed() bool
	Errors() <-chan error
}


type DomainConsumer interface {
	Consumer[DomainEvent]
}

type GlobalConsumer interface {
	Consumer[GlobalEvent]
}

type Consumer[E Event] interface {
	Closable
	BrokerName() BrokerName
	Consume(ctx context.Context) (E, error)
	ConsumeBatch(ctx context.Context) ([]E, error)
}

type DomainProducer interface {
	Producer[DomainEvent]
}

type GlobalProducer interface {
	Producer[GlobalEvent]
}

type Producer[E Event] interface {
	Closable
	BrokerName() BrokerName
	Produce(ctx context.Context, event E) error
	ProduceBatch(ctx context.Context, events []E) error
}
