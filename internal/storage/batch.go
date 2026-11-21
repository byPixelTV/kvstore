package storage

import "context"

// WriteOperation is one ordered SET or DELETE. Delete on a missing key has revision 0.
type WriteOperation struct {
	Namespace       string
	Key             string
	Value           []byte
	Delete          bool
	ExpiresAtUnixMS int64
}

// BatchWriter applies a whole request atomically inside one storage/Raft shard.
// The caller must ensure all operations belong to that shard.
type BatchWriter interface {
	BatchWrite(context.Context, []WriteOperation) ([]uint64, error)
}
