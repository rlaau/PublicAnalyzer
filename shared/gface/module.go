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

type ParentModule interface {
	RegisterChildren(...ChildModule) error
}
type ChildModule interface {
	RefisterParent(Module) error
}
