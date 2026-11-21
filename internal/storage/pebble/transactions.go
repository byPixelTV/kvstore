package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/byPixelTV/kvstore/internal/transaction"
	pebbledb "github.com/cockroachdb/pebble/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errTransactionIDConflict = status.Error(codes.AlreadyExists, "transaction request ID already used for different input")

const (
	committedReceipt = byte(1)
	abortedReceipt   = byte(2)
)

// ControlTransaction runs a local plan in the same queue as point and batch
// writes. The request ID makes a retry safe after an uncertain response.
func (s *Store) ControlTransaction(ctx context.Context, cmd transaction.Control) (transaction.Reply, error) {
	if cmd.Action != transaction.Local {
		return transaction.Reply{}, status.Error(codes.InvalidArgument, "standalone storage supports local transactions")
	}
	if cmd.Plan == nil || len(cmd.Plan.Parts) != 1 || cmd.Plan.ID == "" || cmd.Plan.Fingerprint == "" {
		return transaction.Reply{}, status.Error(codes.InvalidArgument, "invalid local transaction plan")
	}
	if err := ctx.Err(); err != nil {
		return transaction.Reply{}, err
	}

	// The worker can still own a request after the caller's context expires.
	plan := *cmd.Plan
	plan.Parts = slices.Clone(plan.Parts)
	part := &plan.Parts[0]
	part.Checks = slices.Clone(part.Checks)
	for i := range part.Checks {
		part.Checks[i].Value = bytes.Clone(part.Checks[i].Value)
	}
	part.Writes = slices.Clone(part.Writes)
	for i := range part.Writes {
		part.Writes[i].Value = bytes.Clone(part.Writes[i].Value)
	}

	result := s.submitRequest(writeRequest{ctx: ctx, transaction: &plan})
	return transaction.Reply{Outcome: result.outcome}, result.err
}

func (s *Store) newWriteBatch(group []writeRequest) *pebbledb.Batch {
	for i := range group {
		if group[i].transaction != nil {
			return s.db.NewIndexedBatch()
		}
	}
	return s.db.NewBatch()
}

// stageLocalTransaction stores a receipt with the write group. The caller
// stages writes only for a new committed receipt in that same batch.
func stageLocalTransaction(batch *pebbledb.Batch, plan *transaction.Plan, nowMS int64) (transaction.Outcome, bool, error) {
	// User records start with 0x01. This metadata range is reserved for receipts.
	receiptKey := append([]byte{0x00, 0x20}, plan.ID...)
	outcome, found, err := readTransactionReceipt(batch, receiptKey, plan.Fingerprint)
	if err != nil || found {
		return outcome, false, err
	}

	outcome, receiptType := transaction.Committed, committedReceipt
	if plan.DeadlineMS > 0 && plan.DeadlineMS <= nowMS {
		outcome, receiptType = transaction.Aborted, abortedReceipt
	} else {
		for _, check := range plan.Parts[0].Checks {
			matches, err := matchesTransactionCheck(batch, check, nowMS)
			if err != nil {
				return "", false, err
			}
			if !matches {
				outcome, receiptType = transaction.Aborted, abortedReceipt
				break
			}
		}
	}

	if err := batch.Set(receiptKey, append([]byte{receiptType}, plan.Fingerprint...), nil); err != nil {
		return "", false, fmt.Errorf("stage transaction receipt: %w", err)
	}
	return outcome, true, nil
}

func readTransactionReceipt(batch *pebbledb.Batch, key []byte, fingerprint string) (transaction.Outcome, bool, error) {
	raw, closer, err := batch.Get(key)
	if errors.Is(err, pebbledb.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read transaction receipt: %w", err)
	}
	var outcome transaction.Outcome
	var receiptErr error
	switch {
	case len(raw) < 2:
		receiptErr = errors.New("invalid transaction receipt")
	case raw[0] == committedReceipt:
		outcome = transaction.Committed
	case raw[0] == abortedReceipt:
		outcome = transaction.Aborted
	default:
		receiptErr = errors.New("invalid transaction receipt outcome")
	}
	sameFingerprint := receiptErr == nil && string(raw[1:]) == fingerprint
	if err := errors.Join(receiptErr, closer.Close()); err != nil {
		return "", false, err
	}
	if !sameFingerprint {
		return "", true, errTransactionIDConflict
	}
	return outcome, true, nil
}

func matchesTransactionCheck(batch *pebbledb.Batch, check transaction.Check, nowMS int64) (bool, error) {
	expectAbsent := !check.MatchValue && check.Revision == 0
	raw, closer, err := batch.Get(encodeKey(check.Namespace, check.Key))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return expectAbsent, nil
	}
	if err != nil {
		return false, fmt.Errorf("read transaction check: %w", err)
	}
	record, err := storage.DecodeRecord(raw)
	var matches bool
	if err == nil {
		switch {
		case record.ExpiredAt(nowMS):
			matches = expectAbsent
		case check.MatchValue:
			matches = bytes.Equal(record.Value, check.Value)
		default:
			matches = record.Revision == check.Revision
		}
	}
	return matches, errors.Join(err, closer.Close())
}
