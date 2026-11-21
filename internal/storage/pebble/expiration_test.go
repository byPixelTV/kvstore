package pebblestore

import (
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/storage"
)

func TestExpirationSurvivesReopenAndFiltersReads(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	deadline := time.Now().Add(time.Hour).UnixMilli()
	_, err = store.BatchWrite(ctx, []storage.WriteOperation{
		{Namespace: "cooldowns", Key: "active", Value: []byte("v"), ExpiresAtUnixMS: deadline},
		{Namespace: "cooldowns", Key: "expired", Value: []byte("v"), ExpiresAtUnixMS: 1},
		{Namespace: "cooldowns", Key: "persistent", Value: []byte("v")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record, found, err := store.Get(ctx, "cooldowns", "active")
	if err != nil || !found || record.ExpiresAtUnixMS != deadline {
		t.Fatalf("active = %+v, found=%t, err=%v", record, found, err)
	}
	if _, found, err := store.Get(ctx, "cooldowns", "expired"); err != nil || found {
		t.Fatalf("expired found=%t, err=%v", found, err)
	}
	scanned := make(map[string]int64)
	if err := store.ScanNamespace(ctx, "cooldowns", func(item storage.ScanItem) error {
		if !item.Checkpoint {
			scanned[item.Key] = item.ExpiresAtUnixMS
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(scanned) != 2 || scanned["active"] != deadline {
		t.Fatalf("unexpected scan: %v", scanned)
	}
	page, err := store.ReadChanges(ctx, 0, 10)
	if err != nil || len(page.Events) != 3 || page.Events[0].ExpiresAtUnixMS != deadline || page.Events[1].ExpiresAtUnixMS != 1 || page.Events[2].ExpiresAtUnixMS != 0 {
		t.Fatalf("unexpected changefeed: %+v, %v", page, err)
	}
	if _, err := store.Set(ctx, "cooldowns", "active", []byte("persistent")); err != nil {
		t.Fatal(err)
	}
	record, found, err = store.Get(ctx, "cooldowns", "active")
	if err != nil || !found || record.ExpiresAtUnixMS != 0 || string(record.Value) != "persistent" {
		t.Fatalf("persistent overwrite = %+v, found=%t, err=%v", record, found, err)
	}
}
