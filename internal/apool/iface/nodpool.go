package iface

import (
	"context"
	"io"
)

type NodPort interface {
	Start(ctx context.Context) error
	io.Closer
}

type NodMsg any
