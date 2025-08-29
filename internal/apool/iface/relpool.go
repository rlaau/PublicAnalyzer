package iface

import (
	"context"
	"io"
)

type RelPort interface {
	Start(ctx context.Context) error
	io.Closer
}

type RelMsg any
