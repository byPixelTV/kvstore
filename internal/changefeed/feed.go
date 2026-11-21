package changefeed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

// The event key is 0x03 + an unsigned big-endian revision, preserving order.
const eventPrefix = byte(3)

var FloorKey = []byte{0, 3} // reserved, never overlaps with user records
var ByteCountKey = []byte{0, 4}
var ErrHistoryUnavailable = errors.New("changefeed history unavailable; resynchronize your cache")

type Change struct {
	Revision        uint64
	Namespace       string
	Key             string
	Value           []byte
	Deleted         bool
	ExpiresAtUnixMS int64
}

type Page struct {
	Events []Change
	Head   uint64
	Oldest uint64
}

// Source is the cursor-based, persistent changefeed surface of a KV store.
type Source interface {
	ReadChanges(context.Context, uint64, int) (Page, error)
	Head(context.Context) (uint64, uint64, error)
	Notify() <-chan struct{}
}

// Notifier wakes streaming subscribers without allocating a channel per subscriber.
// Subscribe BEFORE reading a page to avoid the read/subscription race.
type Notifier struct {
	mu sync.Mutex
	ch chan struct{}
}

func (n *Notifier) Subscribe() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	return n.ch
}

func (n *Notifier) Signal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ch != nil {
		close(n.ch)
	}
	n.ch = make(chan struct{})
}

func Key(revision uint64) []byte {
	var b [9]byte
	b[0] = eventPrefix
	binary.BigEndian.PutUint64(b[1:], revision)
	return b[:]
}

// Format v1 stores flags and the length-prefixed namespace, key and value.
// Format v2 adds an absolute deadline before the namespace length.
func Encode(c Change) []byte {
	b := make([]byte, 0, 16+len(c.Namespace)+len(c.Key)+len(c.Value))
	b = append(b, 1)
	if c.Deleted {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	if c.ExpiresAtUnixMS > 0 {
		b[0] = 2
		b = binary.BigEndian.AppendUint64(b, uint64(c.ExpiresAtUnixMS))
	}
	b = binary.AppendUvarint(b, uint64(len(c.Namespace)))
	b = append(b, c.Namespace...)
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	b = binary.AppendUvarint(b, uint64(len(c.Value)))
	b = append(b, c.Value...)
	return b
}

func Decode(revision uint64, b []byte) (Change, error) {
	if len(b) < 2 || (b[0] != 1 && b[0] != 2) || b[1] > 1 {
		return Change{}, errors.New("unknown changefeed event format")
	}
	c := Change{Revision: revision, Deleted: b[1] == 1}
	version := b[0]
	b = b[2:]
	if version == 2 {
		if len(b) < 8 || c.Deleted {
			return Change{}, errors.New("invalid changefeed expiration")
		}
		c.ExpiresAtUnixMS = int64(binary.BigEndian.Uint64(b[:8]))
		if c.ExpiresAtUnixMS <= 0 {
			return Change{}, errors.New("invalid changefeed deadline")
		}
		b = b[8:]
	}
	read := func() ([]byte, error) {
		n, used := binary.Uvarint(b)
		if used <= 0 {
			return nil, errors.New("invalid changefeed field length")
		}
		b = b[used:]
		if n > uint64(len(b)) {
			return nil, errors.New("truncated changefeed event")
		}
		v := b[:int(n)]
		b = b[int(n):]
		return v, nil
	}
	ns, err := read()
	if err != nil {
		return Change{}, err
	}
	key, err := read()
	if err != nil {
		return Change{}, err
	}
	value, err := read()
	if err != nil {
		return Change{}, err
	}
	if len(b) != 0 {
		return Change{}, errors.New("trailing changefeed bytes")
	}
	c.Namespace, c.Key = string(ns), string(key)
	c.Value = append([]byte(nil), value...)
	return c, nil
}

func DecodeFloor(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid history floor length %d", len(raw))
	}
	floor := binary.BigEndian.Uint64(raw)
	if floor == 0 {
		return 0, errors.New("invalid zero history floor")
	}
	return floor, nil
}

func EncodedFloor(revision uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], revision)
	return b[:]
}

// Read iterates a bounded page. Caller must serialize metadata reads and Restore
// against this scan, so Head and Oldest refer to the same committed view.
func Read(ctx context.Context, db *pebble.DB, after, head, oldest uint64, limit int) (Page, error) {
	page := Page{Head: head, Oldest: oldest}
	if oldest == 0 {
		return page, errors.New("changefeed oldest revision is zero")
	}
	if after < oldest-1 {
		return page, ErrHistoryUnavailable
	}
	if limit <= 0 || limit > 4096 {
		return page, errors.New("changefeed page limit out of range")
	}
	if after >= head {
		return page, ctx.Err()
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte{eventPrefix}, UpperBound: []byte{eventPrefix + 1}})
	if err != nil {
		return page, err
	}
	defer iter.Close()
	page.Events = make([]Change, 0, limit)
	for ok := iter.SeekGE(Key(after + 1)); ok && len(page.Events) < limit; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return Page{}, err
		}
		key := iter.Key()
		if len(key) != 9 || key[0] != eventPrefix {
			return Page{}, errors.New("corrupt changefeed index")
		}
		rev := binary.BigEndian.Uint64(key[1:])
		if rev > head {
			break
		}
		c, err := Decode(rev, iter.Value())
		if err != nil {
			return Page{}, fmt.Errorf("decode event %d: %w", rev, err)
		}
		page.Events = append(page.Events, c)
	}
	if err := iter.Error(); err != nil {
		return Page{}, err
	}
	return page, nil
}

// Retention is deterministic across Raft replicas and bounded by both event
// count and payload bytes. These values are part of the replicated data format.
const MaxEvents uint64 = 100_000
const MaxHistoryBytes uint64 = 128 << 20

// Append atomically stores an event, its byte budget and any required evictions.
// Call only while holding the store/FSM writer lock. No write becomes visible
// before the containing Pebble batch is committed.
func Append(db *pebble.DB, batch *pebble.Batch, event Change, oldest, used uint64, hasFloor bool) (uint64, uint64, error) {
	if oldest == 0 || event.Revision < oldest {
		return 0, 0, errors.New("invalid changefeed append cursor")
	}
	raw := Encode(event)
	if uint64(len(raw)) > MaxHistoryBytes {
		return 0, 0, errors.New("event exceeds history byte limit")
	}
	if err := batch.Set(Key(event.Revision), raw, nil); err != nil {
		return 0, 0, err
	}
	if used > ^uint64(0)-uint64(len(raw)) {
		return 0, 0, errors.New("history bytes overflow")
	}
	used += uint64(len(raw))
	originalFloor := oldest
	for event.Revision-oldest+1 > MaxEvents || used > MaxHistoryBytes {
		stale, closer, err := db.Get(Key(oldest))
		if err != nil {
			return 0, 0, fmt.Errorf("read retained event %d: %w", oldest, err)
		}
		size := uint64(len(stale))
		if err := closer.Close(); err != nil {
			return 0, 0, err
		}
		if size > used {
			return 0, 0, errors.New("corrupt retained event byte count")
		}
		if err := batch.Delete(Key(oldest), nil); err != nil {
			return 0, 0, err
		}
		used -= size
		oldest++
	}
	if !hasFloor || originalFloor != oldest {
		if err := batch.Set(FloorKey, EncodedFloor(oldest), nil); err != nil {
			return 0, 0, err
		}
	}
	if err := batch.Set(ByteCountKey, EncodedFloor(used), nil); err != nil {
		return 0, 0, err
	}
	return oldest, used, nil
}
