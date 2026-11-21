package storage

import (
	"encoding/binary"
	"errors"
)

func (r Record) ExpiredAt(nowUnixMS int64) bool {
	return r.ExpiresAtUnixMS > 0 && r.ExpiresAtUnixMS <= nowUnixMS
}

// Keep the legacy [revision,value] format for persistent records. Revision zero
// is invalid for stored values, so it identifies the extended record format.
func EncodeRecord(value []byte, revision uint64, expiresAtUnixMS int64) []byte {
	header := 8
	if expiresAtUnixMS > 0 {
		header = 24
	}
	raw := make([]byte, header+len(value))
	if expiresAtUnixMS > 0 {
		binary.BigEndian.PutUint64(raw[8:16], revision)
		binary.BigEndian.PutUint64(raw[16:24], uint64(expiresAtUnixMS))
	} else {
		binary.BigEndian.PutUint64(raw[:8], revision)
	}
	copy(raw[header:], value)
	return raw
}

// DecodeRecord returns a value borrowing raw. Callers copy it before releasing
// the underlying read handle or advancing an iterator.
func DecodeRecord(raw []byte) (Record, error) {
	if len(raw) < 8 {
		return Record{}, errors.New("record has no revision")
	}
	revision := binary.BigEndian.Uint64(raw[:8])
	if revision != 0 {
		return Record{Revision: revision, Value: raw[8:]}, nil
	}
	if len(raw) < 24 {
		return Record{}, errors.New("invalid expiring record header")
	}
	revision = binary.BigEndian.Uint64(raw[8:16])
	expiresAt := int64(binary.BigEndian.Uint64(raw[16:24]))
	if revision == 0 || expiresAt <= 0 {
		return Record{}, errors.New("invalid expiring record revision or deadline")
	}
	return Record{Revision: revision, ExpiresAtUnixMS: expiresAt, Value: raw[24:]}, nil
}
