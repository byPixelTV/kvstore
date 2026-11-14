package sharding

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only one authoritative router may serve writes while live migration is
// enabled. Metadata is durably replicated in shard 0, but this is NOT a
// multi-router distributed lock or a lease/fencing protocol.
const routeNamespace = "__kvstore_internal_routing_v1"
const routeKey = "slot-owners"

// Routes are immutable snapshots. Each migration changes a single
// (namespace,slot) owner without rehashing the remaining 4095 slots.
type routingState struct {
	Version     uint32                      `json:"version"`
	Epoch       uint64                      `json:"epoch"`
	Fingerprint string                      `json:"fingerprint"`
	Overrides   map[string]map[uint16]uint8 `json:"overrides"`
}

func (s *routingState) owner(namespace string, slot uint16) int {
	if s != nil && s.Overrides != nil {
		if bySlot := s.Overrides[namespace]; bySlot != nil {
			if owner, found := bySlot[slot]; found {
				return int(owner)
			}
		}
	}
	return int(slot) % ShardCount
}
func (s *routingState) copy() *routingState {
	next := &routingState{Version: 1, Epoch: s.Epoch, Fingerprint: s.Fingerprint, Overrides: make(map[string]map[uint16]uint8, len(s.Overrides))}
	for namespace, slots := range s.Overrides {
		bySlot := make(map[uint16]uint8, len(slots))
		for slot, owner := range slots {
			bySlot[slot] = owner
		}
		next.Overrides[namespace] = bySlot
	}
	return next
}
func (s *routingState) validate(fingerprint string) error {
	if s.Version != 1 || s.Epoch == 0 || s.Fingerprint != fingerprint {
		return errors.New("invalid routing metadata version/epoch/cluster fingerprint")
	}
	for namespace, bySlot := range s.Overrides {
		if namespace == "" || strings.HasPrefix(namespace, "__kvstore_internal_") {
			return errors.New("invalid routing metadata namespace")
		}
		for slot, owner := range bySlot {
			if slot >= SlotCount || int(owner) >= ShardCount {
				return errors.New("invalid slot owner")
			}
		}
	}
	return nil
}
func fingerprint(layout Layout) string {
	var parts []string
	for _, group := range layout.Groups {
		parts = append(parts, strings.Join(group, ","))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, ";")))
	return fmt.Sprintf("%x", sum)
}

type slotKey struct {
	namespace string
	slot      uint16
}

func (r *Router) current() *routingState {
	if s := r.routing.Load(); s != nil {
		return s
	}
	s := &routingState{Version: 1, Epoch: 1, Overrides: map[string]map[uint16]uint8{}}
	if r.routing.CompareAndSwap(nil, s) {
		return s
	}
	return r.routing.Load()
}
func (r *Router) slotGate(namespace string, slot uint16) *sync.RWMutex {
	value, _ := r.slotGates.LoadOrStore(slotKey{namespace, slot}, &sync.RWMutex{})
	return value.(*sync.RWMutex)
}
func (r *Router) checkRouteAvailable() error {
	if r.unresolved.Load() {
		return status.Error(codes.Unavailable, "routing metadata outcome uncertain; all requests fenced pending operator recovery")
	}
	return nil
}
func (r *Router) epochSignal() (uint64, <-chan struct{}) {
	r.epochMu.Lock()
	defer r.epochMu.Unlock()
	if r.epochChange == nil {
		r.epochChange = make(chan struct{})
	}
	return r.current().Epoch, r.epochChange
}
func (r *Router) install(next *routingState) {
	r.epochMu.Lock()
	if r.epochChange != nil {
		close(r.epochChange)
	}
	r.routing.Store(next)
	r.epochChange = make(chan struct{})
	r.epochMu.Unlock()
}

func encodeEpochCursor(c Cursor, epoch uint64) string {
	var raw [1 + 4 + 8 + 8*ShardCount]byte
	raw[0] = 2
	binary.BigEndian.PutUint32(raw[1:5], LayoutVersion)
	binary.BigEndian.PutUint64(raw[5:13], epoch)
	for i, rev := range c {
		binary.BigEndian.PutUint64(raw[13+i*8:21+i*8], rev)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}
func decodeEpochCursor(raw string, epoch uint64) (Cursor, error) {
	var c Cursor
	if raw == "" {
		return c, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) < 5 || binary.BigEndian.Uint32(decoded[1:5]) != LayoutVersion {
		return c, errors.New("invalid cursor encoding/version; rescan required")
	}
	switch decoded[0] {
	case 1:
		if len(decoded) != 1+4+8*ShardCount || epoch != 1 {
			return c, errors.New("stale cursor after routing change; rescan required")
		}
		for i := range c {
			c[i] = binary.BigEndian.Uint64(decoded[5+i*8 : 13+i*8])
		}
	case 2:
		if len(decoded) != 1+4+8+8*ShardCount || binary.BigEndian.Uint64(decoded[5:13]) != epoch {
			return c, errors.New("routing epoch changed; rescan required")
		}
		for i := range c {
			c[i] = binary.BigEndian.Uint64(decoded[13+i*8 : 21+i*8])
		}
	default:
		return c, errors.New("unsupported cursor encoding; rescan required")
	}
	return c, nil
}

// LoadRouting MUST succeed before the router is advertised or begins serving.
// A failed/ambiguous metadata read fails closed rather than using a default
// layout over an already migrated dataset.
func (r *Router) LoadRouting(ctx context.Context) error {
	if r.loaded.Load() {
		return nil
	}
	g := r.groups[0]
	client, err := g.selectClient()
	if err != nil {
		return err
	}
	res, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: routeNamespace, Key: routeKey})
	if err != nil {
		return fmt.Errorf("load replicated routing metadata: %w", err)
	}
	next := &routingState{Version: 1, Epoch: 1, Fingerprint: r.clusterFingerprint, Overrides: map[string]map[uint16]uint8{}}
	if res.GetFound() {
		if err := json.Unmarshal(res.GetValue(), next); err != nil {
			return fmt.Errorf("decode routing metadata: %w", err)
		}
		if err := next.validate(r.clusterFingerprint); err != nil {
			return err
		}
	}
	r.install(next)
	r.loaded.Store(true)
	return nil
}

// Save and then publish: if the router crashes after Raft commit but before
// publication, the next process loads the NEW map and correctly uses target.
func (r *Router) persistRouting(ctx context.Context, next *routingState) error {
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if len(raw) > 256<<10 {
		return errors.New("routing metadata exceeds backend value limit; migration aborted")
	}
	client, err := r.groups[0].selectClient()
	if err != nil {
		return err
	}
	_, err = client.Set(ctx, &kvstorev1.SetRequest{Namespace: routeNamespace, Key: routeKey, Value: raw})
	if err == nil {
		return nil
	}
	// A failed write is outcome-unknown. Never silently return to the old
	// owner: the new mapping could already be committed in shard 0.
	r.unresolved.Store(true)
	r.ready.Store(false)
	return fmt.Errorf("metadata commit outcome unknown (router fenced; restart/recover after quorum): %w", err)
}

func (r *Router) StateJSON() ([]byte, error) { return json.MarshalIndent(r.current(), "", "  ") }

// Ensure Router uses an atomic immutable layout pointer, rather than a
// mutex around every ordinary key lookup.
var _ = atomic.Pointer[routingState]{}
