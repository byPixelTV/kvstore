package cluster

import (
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

// measuredLogStore wraps the *durable* backend, below Raft's LogCache.
// Timings therefore reflect actual WAL reads/appends rather than cache hits.
// No buffering or unsafe delayed fsync is introduced here.
type measuredLogStore struct {
	inner raft.LogStore
	stats *raftLogMetrics
}

type raftLogMetrics struct {
	writeCalls    atomic.Uint64
	writtenLogs   atomic.Uint64
	writtenBytes  atomic.Uint64
	writeNanos    atomic.Uint64
	writeMaxNanos atomic.Uint64
	writeErrors   atomic.Uint64
	readCalls     atomic.Uint64
	readNanos     atomic.Uint64
	readErrors    atomic.Uint64
	deleteCalls   atomic.Uint64
	deleteNanos   atomic.Uint64
}

type RaftLogPerformance struct {
	WriteCalls      uint64  `json:"write_calls"`
	StoredLogs      uint64  `json:"stored_logs"`
	StoredBytes     uint64  `json:"stored_bytes"`
	AvgLogsPerWrite float64 `json:"avg_logs_per_write"`
	AverageWriteMS  float64 `json:"average_write_ms"`
	MaxWriteMS      float64 `json:"max_write_ms"`
	WriteErrors     uint64  `json:"write_errors"`
	ReadMisses      uint64  `json:"backend_read_calls"`
	AverageReadMS   float64 `json:"average_backend_read_ms"`
	ReadErrors      uint64  `json:"backend_read_errors"`
	DeleteCalls     uint64  `json:"delete_calls"`
	AverageDeleteMS float64 `json:"average_delete_ms"`
}

func (s *raftLogMetrics) Snapshot() RaftLogPerformance {
	if s == nil {
		return RaftLogPerformance{}
	}
	writes := s.writeCalls.Load()
	reads := s.readCalls.Load()
	deletes := s.deleteCalls.Load()
	return RaftLogPerformance{
		WriteCalls: writes, StoredLogs: s.writtenLogs.Load(), StoredBytes: s.writtenBytes.Load(),
		AvgLogsPerWrite: average(s.writtenLogs.Load(), writes),
		AverageWriteMS:  average(s.writeNanos.Load(), writes) / 1e6,
		MaxWriteMS:      float64(s.writeMaxNanos.Load()) / 1e6,
		WriteErrors:     s.writeErrors.Load(), ReadMisses: reads,
		AverageReadMS: average(s.readNanos.Load(), reads) / 1e6,
		ReadErrors:    s.readErrors.Load(), DeleteCalls: deletes,
		AverageDeleteMS: average(s.deleteNanos.Load(), deletes) / 1e6,
	}
}
func (s *measuredLogStore) FirstIndex() (uint64, error) { return s.inner.FirstIndex() }
func (s *measuredLogStore) LastIndex() (uint64, error)  { return s.inner.LastIndex() }
func (s *measuredLogStore) GetLog(idx uint64, log *raft.Log) error {
	start := time.Now()
	err := s.inner.GetLog(idx, log)
	s.stats.readCalls.Add(1)
	s.stats.readNanos.Add(uint64(time.Since(start).Nanoseconds()))
	if err != nil {
		s.stats.readErrors.Add(1)
	}
	return err
}
func (s *measuredLogStore) StoreLog(log *raft.Log) error { return s.StoreLogs([]*raft.Log{log}) }
func (s *measuredLogStore) StoreLogs(logs []*raft.Log) error {
	start := time.Now()
	err := s.inner.StoreLogs(logs)
	duration := uint64(time.Since(start).Nanoseconds())
	s.stats.writeCalls.Add(1)
	s.stats.writeNanos.Add(duration)
	for {
		peak := s.stats.writeMaxNanos.Load()
		if duration <= peak || s.stats.writeMaxNanos.CompareAndSwap(peak, duration) {
			break
		}
	}
	if err != nil {
		s.stats.writeErrors.Add(1)
		return err
	}
	var size uint64
	for _, log := range logs {
		if log != nil {
			size += uint64(len(log.Data))
		}
	}
	s.stats.writtenLogs.Add(uint64(len(logs)))
	s.stats.writtenBytes.Add(size)
	return nil
}
func (s *measuredLogStore) DeleteRange(min, max uint64) error {
	start := time.Now()
	err := s.inner.DeleteRange(min, max)
	s.stats.deleteCalls.Add(1)
	s.stats.deleteNanos.Add(uint64(time.Since(start).Nanoseconds()))
	return err
}

var _ raft.LogStore = (*measuredLogStore)(nil)

// IsMonotonic preserves the raft-wal invariant through our metrics wrapper.
// Raft needs this for correct log handling following user snapshot restores.
func (s *measuredLogStore) IsMonotonic() bool {
	if m, ok := s.inner.(raft.MonotonicLogStore); ok {
		return m.IsMonotonic()
	}
	return false
}

var _ raft.MonotonicLogStore = (*measuredLogStore)(nil)
