package changefeed

import (
	"bytes"
	"testing"
)

func TestBinaryEventCodec(t *testing.T) {
	original := Change{Revision: 42, Namespace: "a/ø", Key: "x\x00y", Value: []byte{0, 255, 3, 7}, Deleted: false}
	got, err := Decode(original.Revision, Encode(original))
	if err != nil || got.Revision != original.Revision || got.Namespace != original.Namespace || got.Key != original.Key || got.Deleted != original.Deleted || !bytes.Equal(got.Value, original.Value) {
		t.Fatalf("decoded=%+v err=%v", got, err)
	}
	for _, bad := range [][]byte{{}, {1}, {4, 0}, {1, 2}, {1, 0, 7}} {
		if _, err := Decode(1, bad); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
}

func TestNotifierSignals(t *testing.T) {
	var n Notifier
	first := n.Subscribe()
	n.Signal()
	select {
	case <-first:
	default:
		t.Fatal("old subscription not notified")
	}
	second := n.Subscribe()
	if first == second {
		t.Fatal("notification channel not rotated")
	}
}
