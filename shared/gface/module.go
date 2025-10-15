package gface

import "context"

type Module interface {
	Name() ModuleName
	Start(ctx context.Context) error
	Close() error
}

type SubModule interface {
	MainModule() ModuleName
}

type ParentModule interface {
	RegisterChildren(...ChildModule) error
}
type ChildModule interface {
	RefisterParent(Module) error
}
