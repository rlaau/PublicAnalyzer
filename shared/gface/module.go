package gface

import "context"

type Module interface {
	Name() ModuleName
	Start(ctx context.Context) error
	Closable
}

type ModuleWithErrors interface {
	Module
	WithErrors
}
type SubModule interface {
	MainModule() ModuleName
}

type Parent interface {
	RegisterChildren(...Child) error
}
type Child interface {
	RegisterParent(Module) error
}
