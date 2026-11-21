package kvstore

import (
	"context"
	"errors"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/storage"
)

var (
	ErrEmptyNamespace        = errors.New("namespace cannot be empty")
	ErrEmptyKey              = errors.New("key cannot be empty")
	ErrInvalidExpiration     = errors.New("invalid expiration")
	ErrExpirationUnsupported = errors.New("storage does not support expiration")
)

type Service struct{ store storage.Store }

func New(store storage.Store) *Service {
	if store == nil {
		panic("kvstore: storage cannot be nil")
	}
	return &Service{store: store}
}
func validateKey(namespace, key string) error {
	if namespace == "" {
		return ErrEmptyNamespace
	}
	if key == "" {
		return ErrEmptyKey
	}
	return nil
}
func (s *Service) Get(ctx context.Context, namespace, key string) (storage.Record, bool, error) {
	if err := validateKey(namespace, key); err != nil {
		return storage.Record{}, false, err
	}
	return s.store.Get(ctx, namespace, key)
}

// GetEventual reads from the selected replica when supported. Standalone
// backends return their authoritative local value. Only committed FSM data
// is exposed; cluster followers may still lag behind the leader.
func (s *Service) GetEventual(ctx context.Context, namespace, key string) (storage.Record, bool, error) {
	if err := validateKey(namespace, key); err != nil {
		return storage.Record{}, false, err
	}
	if local, ok := s.store.(interface {
		GetLocal(context.Context, string, string) (storage.Record, bool, error)
	}); ok {
		return local.GetLocal(ctx, namespace, key)
	}
	return s.store.Get(ctx, namespace, key)
}

func (s *Service) Set(ctx context.Context, namespace, key string, value []byte) (uint64, error) {
	if err := validateKey(namespace, key); err != nil {
		return 0, err
	}
	return s.store.Set(ctx, namespace, key, value)
}

func (s *Service) SupportsExpiration() bool {
	_, ok := s.store.(storage.ExpiringStore)
	return ok
}

func (s *Service) SetWithExpiration(ctx context.Context, namespace, key string, value []byte, expiresAtUnixMS int64) (uint64, error) {
	if expiresAtUnixMS == 0 {
		return s.Set(ctx, namespace, key, value)
	}
	if expiresAtUnixMS < 0 {
		return 0, ErrInvalidExpiration
	}
	if err := validateKey(namespace, key); err != nil {
		return 0, err
	}
	store, ok := s.store.(storage.ExpiringStore)
	if !ok {
		return 0, ErrExpirationUnsupported
	}
	return store.SetWithExpiration(ctx, namespace, key, value, expiresAtUnixMS)
}
func (s *Service) Delete(ctx context.Context, namespace, key string) (uint64, error) {
	if err := validateKey(namespace, key); err != nil {
		return 0, err
	}
	return s.store.Delete(ctx, namespace, key)
}

func (s *Service) Feed() (changefeed.Source, bool) {
	feed, ok := s.store.(changefeed.Source)
	return feed, ok
}

func (s *Service) ScanNamespace(ctx context.Context, namespace string, emit func(storage.ScanItem) error) error {
	if namespace == "" {
		return ErrEmptyNamespace
	}
	scanner, ok := s.store.(storage.NamespaceScanner)
	if !ok {
		return errors.New("namespace scan unsupported by backend")
	}
	return scanner.ScanNamespace(ctx, namespace, emit)
}

// BatchWrite validates all operations before they enter the storage/raft log.
// A request is atomic within one backend. The sharding router handles multi-
// shard partial outcomes separately and never retries uncertain writes.
func (s *Service) BatchWrite(ctx context.Context, operations []storage.WriteOperation) ([]uint64, error) {
	if len(operations) == 0 || len(operations) > 128 {
		return nil, errors.New("BatchWrite requires 1..128 operations")
	}
	total := 0
	for _, op := range operations {
		if err := validateKey(op.Namespace, op.Key); err != nil {
			return nil, err
		}
		if op.ExpiresAtUnixMS < 0 || (op.Delete && op.ExpiresAtUnixMS != 0) {
			return nil, ErrInvalidExpiration
		}
		if op.ExpiresAtUnixMS != 0 && !s.SupportsExpiration() {
			return nil, ErrExpirationUnsupported
		}
		if len(op.Value) > 256<<10 {
			return nil, errors.New("BatchWrite value exceeds 256 KiB")
		}
		total += len(op.Value) + len(op.Namespace) + len(op.Key) + 128
		if total > 256<<10 {
			return nil, errors.New("BatchWrite payload exceeds 256 KiB; split into smaller batches")
		}
	}
	writer, ok := s.store.(storage.BatchWriter)
	if !ok {
		return nil, errors.New("backend does not support atomic BatchWrite")
	}
	return writer.BatchWrite(ctx, operations)
}
