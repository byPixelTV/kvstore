package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"
)

type storageKey struct {
	namespace string
	key       string
}

type MemoryStorage struct {
	mu       sync.RWMutex
	data     map[storageKey]Record
	revision uint64
}

func New() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[storageKey]Record),
	}
}

var _ Store = (*MemoryStorage)(nil)

func (m *MemoryStorage) Get(ctx context.Context, namespace, key string) (Record, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return Record{}, false, err
	}

	record, exists := m.data[storageKey{namespace: namespace, key: key}]
	if !exists || (record.ExpiresAtUnixMS > 0 && record.ExpiredAt(time.Now().UnixMilli())) {
		return Record{}, false, nil
	}

	record.Value = bytes.Clone(record.Value)
	return record, true, nil
}

func (m *MemoryStorage) Set(ctx context.Context, namespace, key string, value []byte) (uint64, error) {
	return m.SetWithExpiration(ctx, namespace, key, value, 0)
}

func (m *MemoryStorage) SetWithExpiration(ctx context.Context, namespace, key string, value []byte, expiresAtUnixMS int64) (uint64, error) {
	if expiresAtUnixMS < 0 {
		return 0, errors.New("invalid expiration")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	m.revision++
	m.data[storageKey{namespace: namespace, key: key}] = Record{
		Value:           bytes.Clone(value),
		Revision:        m.revision,
		ExpiresAtUnixMS: expiresAtUnixMS,
	}

	return m.revision, nil
}

func (m *MemoryStorage) Delete(ctx context.Context, namespace, key string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	id := storageKey{namespace: namespace, key: key}
	if _, exists := m.data[id]; !exists {
		return 0, nil
	}

	delete(m.data, id)
	m.revision++
	return m.revision, nil
}
