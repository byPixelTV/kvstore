package pebblestore

import (
	"slices"
	"testing"
)

func TestListNamespacesSkipsExpiredRecords(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := t.Context()
	if _, err := store.SetWithExpiration(ctx, "abandoned", "old", []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetWithExpiration(ctx, "games", "a", []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	for _, row := range [][2]string{{"games", "b"}, {"games", "c"}, {"games:prod", "b"}, {"audit", "tail"}} {
		if _, err := store.Set(ctx, row[0], row[1], []byte("live")); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"audit", "games", "games:prod"}
	if got, err := store.ListNamespaces(ctx); err != nil || !slices.Equal(got, want) {
		t.Fatalf("ListNamespaces got %v, %v; want %v", got, err, want)
	}
}
