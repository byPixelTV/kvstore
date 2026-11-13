package kvstore

import (
	"context"
	"errors"

	"github.com/byPixelTV/kvstore/internal/storage"
)

var (
	ErrEmptyNamespace = errors.New("namespace cannot be empty")
	ErrEmptyKey       = errors.New("key cannot be empty")
)

type Service struct {
	store storage.Store
}

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

func (s *Service) Get(
	ctx context.Context,
	namespace, key string,
) (storage.Record, bool, error) {
	if err := validateKey(namespace, key); err != nil {
		return storage.Record{}, false, err
	}

	return s.store.Get(ctx, namespace, key)
}

func (s *Service) Set(
	ctx context.Context,
	namespace, key string,
	value []byte,
) (uint64, error) {
	if err := validateKey(namespace, key); err != nil {
		return 0, err
	}

	return s.store.Set(ctx, namespace, key, value)
}

func (s *Service) Delete(
	ctx context.Context,
	namespace, key string,
) (uint64, error) {
	if err := validateKey(namespace, key); err != nil {
		return 0, err
	}

	return s.store.Delete(ctx, namespace, key)
}
