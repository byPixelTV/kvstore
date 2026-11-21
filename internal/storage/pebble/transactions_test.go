package pebblestore

import (
	"context"
	"testing"

	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/byPixelTV/kvstore/internal/transaction"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConditionalTransactionConflictIsDurable(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err = s.Set(ctx, "profiles", "alice", []byte("original")); err != nil {
		t.Fatal(err)
	}

	plan := &transaction.Plan{ID: "conflicted-request", Fingerprint: "fingerprint", Parts: []transaction.Part{{
		Checks: []transaction.Check{{Namespace: "profiles", Key: "alice", MatchValue: true, Value: []byte("different")}},
		Writes: []storage.WriteOperation{
			{Namespace: "profiles", Key: "alice", Value: []byte("changed")},
			{Namespace: "receipts", Key: "alice", Value: []byte("applied")},
		},
	}}}
	cmd := transaction.Control{Action: transaction.Local, Plan: plan}
	if result, err := s.ControlTransaction(ctx, cmd); err != nil || result.Outcome != transaction.Aborted {
		t.Fatalf("expected conflict: %+v, %v", result, err)
	}
	if r, found, err := s.Get(ctx, "profiles", "alice"); err != nil || !found || r.Revision != 1 || string(r.Value) != "original" {
		t.Fatalf("conflicted transaction changed an existing record: %+v, %v, %v", r, found, err)
	}
	if _, found, err := s.Get(ctx, "receipts", "alice"); err != nil || found {
		t.Fatalf("conflicted transaction created a record: found=%t, %v", found, err)
	}
	if head, _, err := s.Head(ctx); err != nil || head != 1 {
		t.Fatalf("conflicted transaction advanced the head: %d, %v", head, err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	if _, err := s.Set(ctx, "profiles", "alice", []byte("different")); err != nil {
		t.Fatal(err)
	}
	// The condition now matches, but the original aborted result is final.
	if result, err := s.ControlTransaction(ctx, cmd); err != nil || result.Outcome != transaction.Aborted {
		t.Fatalf("expected recorded conflict after reopen: %+v, %v", result, err)
	}
	if _, found, err := s.Get(ctx, "receipts", "alice"); err != nil || found {
		t.Fatalf("aborted retry created a record: found=%t, %v", found, err)
	}
}

func TestCommittedTransactionReceiptIsDurable(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	plan := &transaction.Plan{ID: "committed-request", Fingerprint: "fingerprint", Parts: []transaction.Part{{
		Checks: []transaction.Check{{Namespace: "profiles", Key: "alice", Revision: 0}},
		Writes: []storage.WriteOperation{
			{Namespace: "profiles", Key: "alice", Value: []byte("player")},
			{Namespace: "receipts", Key: "alice", Value: []byte("applied")},
		},
	}}}
	cmd := transaction.Control{Action: transaction.Local, Plan: plan}
	if result, err := s.ControlTransaction(ctx, cmd); err != nil || result.Outcome != transaction.Committed {
		t.Fatalf("expected commit: %+v, %v", result, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	// A receipt must be consulted before the new deadline and current
	// precondition, which would now reject this request.
	plan.DeadlineMS = 1
	if result, err := s.ControlTransaction(ctx, cmd); err != nil || result.Outcome != transaction.Committed {
		t.Fatalf("expected recorded commit after reopen: %+v, %v", result, err)
	}
	plan.Fingerprint = "different"
	if _, err := s.ControlTransaction(ctx, cmd); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected reused request ID rejection: %v", err)
	}
	page, err := s.ReadChanges(ctx, 0, 16)
	if err != nil || page.Head != 2 || len(page.Events) != 2 ||
		page.Events[0].Namespace != "profiles" || page.Events[1].Namespace != "receipts" {
		t.Fatalf("transaction changefeed or retry revisions: %+v, %v", page, err)
	}
}

func TestTransactionSeesPreviousWritesAndReceiptsInGroup(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	plan := &transaction.Plan{ID: "same-group-request", Fingerprint: "fingerprint", Parts: []transaction.Part{{
		Checks: []transaction.Check{{Namespace: "profiles", Key: "alice", Revision: 1},
			{Namespace: "profiles", Key: "alice", MatchValue: true, Value: []byte("first")}},
		Writes: []storage.WriteOperation{{Namespace: "profiles", Key: "alice", Value: []byte("second")}},
	}}}
	conflictingPlan := *plan
	conflictingPlan.Fingerprint = "different"
	makeTx := func(plan *transaction.Plan) writeRequest {
		return writeRequest{ctx: ctx, transaction: plan, reply: make(chan writeResponse, 1)}
	}
	first := writeRequest{ctx: ctx, namespace: "profiles", key: "alice", value: []byte("first"), reply: make(chan writeResponse, 1)}
	tx, retry, conflict := makeTx(plan), makeTx(plan), makeTx(&conflictingPlan)
	last := writeRequest{ctx: ctx, namespace: "profiles", key: "bob", value: []byte("last"), reply: make(chan writeResponse, 1)}

	// Submit a fixed group to exercise staged visibility without depending
	// on goroutine scheduling or a timer selecting these requests together.
	s.applyGroup([]writeRequest{first, tx, retry, conflict, last})
	if result := <-first.reply; result.err != nil || result.revision != 1 {
		t.Fatalf("first write: %+v", result)
	}
	for _, req := range []writeRequest{tx, retry} {
		if result := <-req.reply; result.err != nil || result.outcome != transaction.Committed {
			t.Fatalf("transaction did not see staged write or receipt: %+v", result)
		}
	}
	if result := <-conflict.reply; status.Code(result.err) != codes.AlreadyExists {
		t.Fatalf("expected staged fingerprint conflict: %+v", result)
	}
	if result := <-last.reply; result.err != nil || result.revision != 3 {
		t.Fatalf("fingerprint conflict changed another write's outcome: %+v", result)
	}
	record, found, err := s.Get(ctx, "profiles", "alice")
	if err != nil || !found || record.Revision != 2 || string(record.Value) != "second" {
		t.Fatalf("transaction result: %+v, %t, %v", record, found, err)
	}
}

func TestTransactionChecksUseLocalExpirationAndSupportEmptyValues(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.SetWithExpiration(ctx, "profiles", "stale", []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set(ctx, "profiles", "empty", nil); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		id    string
		check transaction.Check
		want  transaction.Outcome
	}{
		{"expired-is-absent", transaction.Check{Namespace: "profiles", Key: "stale", Revision: 0}, transaction.Committed},
		{"expired-is-not-that-value", transaction.Check{Namespace: "profiles", Key: "stale", MatchValue: true, Value: []byte("old")}, transaction.Aborted},
		{"empty-present-matches", transaction.Check{Namespace: "profiles", Key: "empty", MatchValue: true}, transaction.Committed},
		{"missing-is-not-empty", transaction.Check{Namespace: "profiles", Key: "missing", MatchValue: true}, transaction.Aborted},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			plan := &transaction.Plan{ID: tc.id, Fingerprint: "fingerprint", Parts: []transaction.Part{{Checks: []transaction.Check{tc.check}}}}
			cmd := transaction.Control{Action: transaction.Local, Plan: plan, AtMS: -1}
			if result, err := s.ControlTransaction(ctx, cmd); err != nil || result.Outcome != tc.want {
				t.Fatalf("want %s: %+v, %v", tc.want, result, err)
			}
		})
	}
	if head, _, err := s.Head(ctx); err != nil || head != 2 {
		t.Fatalf("check-only transactions advanced the head: %d, %v", head, err)
	}
}
