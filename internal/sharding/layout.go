package sharding

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"strings"
)

// LayoutVersion pins both the number of shards and the 4096-slot ownership
// function. Never change either in-place on existing data volumes.
const LayoutVersion uint32 = 1
const SlotCount = 4096
const ShardCount = 3
const revisionBits = 56
const maxLocalRevision = (uint64(1) << revisionBits) - 1

// Layout groups contain the allowed backend endpoints for each Raft group.
// On first rollout, use three semicolon-separated groups, with three node
// endpoints per group. Every router must be configured identically.
// Example: s0n0:50051,s0n1:50051,s0n2:50051;...;...
type Layout struct{ Groups [ShardCount][]string }

func ParseLayout(raw string) (Layout, error) {
	var result Layout
	groups := strings.Split(raw, ";")
	if len(groups) != ShardCount {
		return result, fmt.Errorf("expected %d Raft groups, got %d", ShardCount, len(groups))
	}
	seen := map[string]bool{}
	for id, group := range groups {
		endpoints := strings.Split(group, ",")
		if len(endpoints) != 3 {
			return result, fmt.Errorf("shard %d must have exactly three endpoints", id)
		}
		for _, rawAddress := range endpoints {
			address := strings.TrimSpace(rawAddress)
			host, port, err := net.SplitHostPort(address)
			if err != nil || host == "" || port == "" || strings.ContainsAny(host, " /\\\t\n") {
				return result, fmt.Errorf("invalid endpoint %q for shard %d", address, id)
			}
			if seen[address] {
				return result, fmt.Errorf("duplicate endpoint %q across groups", address)
			}
			seen[address] = true
			result.Groups[id] = append(result.Groups[id], address)
		}
	}
	return result, nil
}

// Slot always includes the namespace, so equal names in separate namespaces
// may live on different shards. FNV-1a is deterministic across platforms.
func Slot(namespace, key string) uint16 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(namespace))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(key))
	return uint16(hash.Sum64() % SlotCount)
}
func Shard(namespace, key string) int { return int(Slot(namespace, key)) % ShardCount }

// EncodeRevision encodes the shard identity for point-operation responses;
// this is monotonic for a SINGLE key, but NOT a global total order.
func EncodeRevision(shard int, local uint64) (uint64, error) {
	if shard < 0 || shard >= ShardCount {
		return 0, errors.New("invalid shard")
	}
	if local > maxLocalRevision {
		return 0, errors.New("shard revision exhausted 56-bit encoding")
	}
	if local == 0 {
		return 0, nil
	}
	return uint64(shard+1)<<revisionBits | local, nil
}

// Cursor represents one independent durable changefeed position per group.
// It is not a global Lamport timestamp and cannot define inter-shard order.
type Cursor [ShardCount]uint64

func (c Cursor) Encode() string {
	var buf [1 + 4 + 8*ShardCount]byte
	buf[0] = 1
	binary.BigEndian.PutUint32(buf[1:5], LayoutVersion)
	for i, v := range c {
		binary.BigEndian.PutUint64(buf[5+i*8:13+i*8], v)
	}
	return base64.RawURLEncoding.EncodeToString(buf[:])
}
func DecodeCursor(encoded string) (Cursor, error) {
	var c Cursor
	if encoded == "" {
		return c, nil
	} // explicit opt-in may start at revision zero
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || (len(raw) != 1+4+8*ShardCount && len(raw) != 1+4+8+8*ShardCount) {
		return c, errors.New("invalid sharded cursor encoding")
	}
	if (raw[0] != 1 && raw[0] != 2) || binary.BigEndian.Uint32(raw[1:5]) != LayoutVersion || (raw[0] == 1 && len(raw) != 1+4+8*ShardCount) || (raw[0] == 2 && len(raw) != 1+4+8+8*ShardCount) {
		return c, errors.New("unsupported cursor/layout version; rescan required")
	}
	offset := 5
	if raw[0] == 2 {
		offset = 13
	}
	for i := range c {
		c[i] = binary.BigEndian.Uint64(raw[offset+i*8 : offset+8+i*8])
	}
	return c, nil
}
