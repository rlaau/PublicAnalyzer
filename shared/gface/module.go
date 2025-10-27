package gface

import "context"

type Module interface {
	Name() ModuleName
	Start(ctx context.Context) error
	Closable
}
type ModuleName string

func ParseModuleName(s string) ModuleName { return ModuleName(s) }

type ModuleWithErrors interface {
	Module
	WithErrors
}
type SubModule interface {
	Module
	MainModule() ModuleName
}

type Parent interface {
	RegisterChildren(...Child) error
}
type Child interface {
	RegisterParent(Module) error
}
