package cluster

import (
	"testing"
	"time"
)

func TestBatchCollectionWindow(t *testing.T) {
	for _, tt := range []struct {
		inflight int
		want     time.Duration
	}{
		{0, 1500 * time.Microsecond},
		{1, 1500 * time.Microsecond},
		{2, 3 * time.Millisecond},
		{3, 3 * time.Millisecond},
		{4, 5 * time.Millisecond},
		{8, 5 * time.Millisecond},
	} {
		got := batchCollectionWindow(1500*time.Microsecond, 5*time.Millisecond, tt.inflight)
		if got != tt.want {
			t.Errorf("inflight=%d got=%s want=%s", tt.inflight, got, tt.want)
		}
	}
	if got := batchCollectionWindow(10*time.Millisecond, 5*time.Millisecond, 4); got != 10*time.Millisecond {
		t.Fatalf("never shorten configured base window, got %s", got)
	}
}
