package iface

import "context"

type Module interface {
	Name() ModuleName
	Start(ctx context.Context) error
	Close() error
}

type SubModule interface {
	ParentName() ModuleName
}
