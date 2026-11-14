package sharding

import (
	"strings"
	"testing"
)

func TestStableSlot(t *testing.T) {
	for _, tc := range [][2]string{{"one", "a"}, {"one", "b"}, {"two", "a"}, {"ns", "long-key-🙂"}} {
		got := Shard(tc[0], tc[1])
		if got < 0 || got >= ShardCount {
			t.Fatal(got)
		}
		if again := Shard(tc[0], tc[1]); again != got {
			t.Fatalf("unstable routing: %d != %d", again, got)
		}
	}
	counts := [ShardCount]int{}
	for i := 0; i < 10000; i++ {
		counts[Shard("mc", "player:"+strings.Repeat("x", i%9)+string(rune(i)))]++
	}
	for i, n := range counts {
		if n < 2000 || n > 4500 {
			t.Fatalf("shard %d imbalance: %d", i, n)
		}
	}
}
func TestCursorRoundTrip(t *testing.T) {
	source := Cursor{5, 12345, 1 << 60}
	decoded, err := DecodeCursor(source.Encode())
	if err != nil || decoded != source {
		t.Fatalf("cursor mismatch %v %v", decoded, err)
	}
	for _, bad := range []string{"abc", "!", "AAAAAAAAAA"} {
		if _, err := DecodeCursor(bad); err == nil {
			t.Fatalf("accepted invalid %q", bad)
		}
	}
	if _, err := DecodeCursor(""); err != nil {
		t.Fatal(err)
	}
}
func TestRevisions(t *testing.T) {
	for shard := 0; shard < ShardCount; shard++ {
		a, err := EncodeRevision(shard, 1)
		if err != nil {
			t.Fatal(err)
		}
		b, err := EncodeRevision(shard, 2)
		if err != nil || b <= a {
			t.Fatal("non-monotonic per shard")
		}
	}
	if _, err := EncodeRevision(0, maxLocalRevision+1); err == nil {
		t.Fatal("expected exhaustion error")
	}
}
func TestParseLayout(t *testing.T) {
	valid := "a:1,b:1,c:1;d:1,e:1,f:1;g:1,h:1,i:1"
	if _, err := ParseLayout(valid); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "a:1,b:1,c:1", "a:1,b:1,c:1;d:1,e:1,f:1;a:1,h:1,i:1", "a:1,b:1,c:1;d:1,e:1,f:1;g:1,h:1"} {
		if _, err := ParseLayout(bad); err == nil {
			t.Fatalf("invalid layout accepted: %s", bad)
		}
	}
}
