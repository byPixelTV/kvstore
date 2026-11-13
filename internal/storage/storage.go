package storage

import "context"

type Record struct {
	Value    []byte
	Revision uint64
}

type Store interface {
	Get(ctx context.Context, namespace, key string) (Record, bool, error)
	Set(ctx context.Context, namespace, key string, value []byte) (uint64, error)
	Delete(ctx context.Context, namespace, key string) (uint64, error)
}
