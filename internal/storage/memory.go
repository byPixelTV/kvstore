package storage

import (
	"bytes"
	"context"
	"sync"
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
	if !exists {
		return Record{}, false, nil
	}

	record.Value = bytes.Clone(record.Value)
	return record, true, nil
}

func (m *MemoryStorage) Set(ctx context.Context, namespace, key string, value []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	m.revision++
	m.data[storageKey{namespace: namespace, key: key}] = Record{
		Value:    bytes.Clone(value),
		Revision: m.revision,
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
