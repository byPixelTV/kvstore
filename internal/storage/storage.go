package storage

import "context"

// Record is a stored value and the revision of its last modification.
type Record struct {
	Value           []byte
	Revision        uint64
	ExpiresAtUnixMS int64
}

// ExpiringStore extends Set with an absolute wall-clock deadline in milliseconds.
type ExpiringStore interface {
	SetWithExpiration(context.Context, string, string, []byte, int64) (uint64, error)
}

// Store defines the operations supported by the storage backend.
type Store interface {
	Get(ctx context.Context, namespace, key string) (Record, bool, error)
	Set(ctx context.Context, namespace, key string, value []byte) (uint64, error)
	Delete(ctx context.Context, namespace, key string) (uint64, error)
}

// ScanItem is emitted by a consistent namespace snapshot. The first item is a
// checkpoint; subsequent items contain the namespace's keys and values.
type ScanItem struct {
	SnapshotRevision uint64
	Key              string
	Value            []byte
	Revision         uint64
	ExpiresAtUnixMS  int64
	Checkpoint       bool
}
type NamespaceScanner interface {
	ScanNamespace(ctx context.Context, namespace string, emit func(ScanItem) error) error
}
