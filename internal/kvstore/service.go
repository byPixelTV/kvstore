package kvstore

import (
	"context"
	"errors"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/storage"
)

var (
	ErrEmptyNamespace = errors.New("namespace cannot be empty")
	ErrEmptyKey       = errors.New("key cannot be empty")
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
