package kvstore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/byPixelTV/kvstore/internal/storage"
)

func TestServiceSetAndGet(t *testing.T) {
	service := New(storage.New())
	ctx := context.Background()

	value := []byte("Hello World")

	revision, err := service.Set(ctx, "players", "123", value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	record, found, err := service.Get(ctx, "players", "123")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !found {
		t.Fatal("Expected key to exist")
	}

	if !bytes.Equal(record.Value, value) {
		t.Fatalf("Expected %q, got %q", value, record.Value)
	}

	if record.Revision != revision {
		t.Fatalf("Expected revision %d, got %d", revision, record.Revision)
	}
}

func TestServiceMissingKey(t *testing.T) {
	service := New(storage.New())

	record, found, err := service.Get(
		context.Background(),
		"players",
		"missing",
	)

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if found {
		t.Fatal("Expected key not to exist")
	}

	if record.Revision != 0 {
		t.Fatalf("Expected revision 0, got %d", record.Revision)
	}
}

func TestServiceNamespaces(t *testing.T) {
	service := New(storage.New())
	ctx := context.Background()

	_, err := service.Set(ctx, "players", "123", []byte("profile"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.Set(ctx, "settings", "123", []byte("settings"))
	if err != nil {
		t.Fatal(err)
	}

	for namespace, expected := range map[string]string{
		"players":  "profile",
		"settings": "settings",
	} {
		record, found, err := service.Get(ctx, namespace, "123")

		if err != nil {
			t.Fatal(err)
		}

		if !found {
			t.Fatalf("Key not found in namespace %s", namespace)
		}

		if string(record.Value) != expected {
			t.Fatalf(
				"Namespace %s: expected %q, got %q",
				namespace, expected, record.Value,
			)
		}
	}
}

func TestServiceDelete(t *testing.T) {
	service := New(storage.New())
	ctx := context.Background()

	setRevision, err := service.Set(ctx, "players", "123", []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	deleteRevision, err := service.Delete(ctx, "players", "123")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if deleteRevision <= setRevision {
		t.Fatalf(
			"Delete revision %d must be greater than Set revision %d",
			deleteRevision, setRevision,
		)
	}

	_, found, err := service.Get(ctx, "players", "123")
	if err != nil {
		t.Fatal(err)
	}

	if found {
		t.Fatal("Key still exists after deletion")
	}

	// Deleting a missing key should return revision 0.
	revision, err := service.Delete(ctx, "players", "123")
	if err != nil {
		t.Fatal(err)
	}

	if revision != 0 {
		t.Fatalf("Expected revision 0, got %d", revision)
	}
}

func TestServiceEmptyValue(t *testing.T) {
	service := New(storage.New())
	ctx := context.Background()

	_, err := service.Set(ctx, "players", "123", []byte{})
	if err != nil {
		t.Fatal(err)
	}

	record, found, err := service.Get(ctx, "players", "123")
	if err != nil {
		t.Fatal(err)
	}

	if !found || len(record.Value) != 0 {
		t.Fatalf("Expected existing empty value, got %+v", record)
	}
}

// mockStore allows testing the service without real storage.
type mockStore struct {
	calls int
	err   error
}

func (m *mockStore) Get(
	ctx context.Context,
	namespace, key string,
) (storage.Record, bool, error) {
	m.calls++
	return storage.Record{}, false, m.err
}

func (m *mockStore) Set(
	ctx context.Context,
	namespace, key string,
	value []byte,
) (uint64, error) {
	m.calls++
	return 0, m.err
}

func (m *mockStore) Delete(
	ctx context.Context,
	namespace, key string,
) (uint64, error) {
	m.calls++
	return 0, m.err
}

func TestServiceValidation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		namespace string
		key       string
		wantErr   error
	}{
		{
			name:      "Empty namespace",
			namespace: "",
			key:       "123",
			wantErr:   ErrEmptyNamespace,
		},
		{
			name:      "Empty key",
			namespace: "players",
			key:       "",
			wantErr:   ErrEmptyKey,
		},
		{
			name:      "Both empty",
			namespace: "",
			key:       "",
			wantErr:   ErrEmptyNamespace,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockStore{}
			service := New(mock)

			_, _, err := service.Get(ctx, tt.namespace, tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Get: expected %v, got %v", tt.wantErr, err)
			}

			_, err = service.Set(ctx, tt.namespace, tt.key, []byte("value"))
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Set: expected %v, got %v", tt.wantErr, err)
			}

			_, err = service.Delete(ctx, tt.namespace, tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Delete: expected %v, got %v", tt.wantErr, err)
			}

			if mock.calls != 0 {
				t.Errorf(
					"Invalid requests reached storage %d times",
					mock.calls,
				)
			}
		})
	}
}

func TestServiceStorageErrors(t *testing.T) {
	ctx := context.Background()
	storageErr := errors.New("storage unavailable")

	mock := &mockStore{err: storageErr}
	service := New(mock)

	_, _, err := service.Get(ctx, "players", "123")
	if !errors.Is(err, storageErr) {
		t.Errorf("Get: expected storage error, got %v", err)
	}

	_, err = service.Set(ctx, "players", "123", []byte("value"))
	if !errors.Is(err, storageErr) {
		t.Errorf("Set: expected storage error, got %v", err)
	}

	_, err = service.Delete(ctx, "players", "123")
	if !errors.Is(err, storageErr) {
		t.Errorf("Delete: expected storage error, got %v", err)
	}

	if mock.calls != 3 {
		t.Errorf("Expected 3 storage calls, got %d", mock.calls)
	}
}

func TestServiceNilStorage(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Expected New(nil) to panic")
		}
	}()

	New(nil)
}
