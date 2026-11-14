package config

import (
	"testing"
	"time"
)

func TestWriteQueueBudgetConfig(t *testing.T) {
	t.Setenv("KVSTORE_MODE", ModeStandalone)
	t.Setenv("KVSTORE_WRITE_MAX_QUEUE_DELAY", "75ms")
	t.Setenv("KVSTORE_WRITE_BATCH_TARGET", "64")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WriteMaxQueueDelay != 75*time.Millisecond || cfg.WriteBatchTarget != 64 {
		t.Fatalf("unexpected write config: %+v", cfg)
	}
}
func TestWriteQueueBudgetRejectsInvalid(t *testing.T) {
	for _, value := range []string{"-1ms", "11s", "bad"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("KVSTORE_MODE", ModeStandalone)
			t.Setenv("KVSTORE_WRITE_MAX_QUEUE_DELAY", value)
			if _, err := Load(); err == nil {
				t.Fatal("expected invalid queue budget to fail")
			}
		})
	}
}
