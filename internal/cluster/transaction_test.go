package cluster

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/byPixelTV/kvstore/internal/transaction"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func transactionLog(t *testing.T, index uint64, cmd command) *raft.Log {
	t.Helper()
	raw, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Type: raft.LogCommand, Index: index, Data: raw}
}

func applyTransactionForTest(t *testing.T, f *FSM, index uint64, control transaction.Control) applyResult {
	t.Helper()
	return f.Apply(transactionLog(t, index, command{Operation: opTransaction, Transaction: &control})).(applyResult)
}

func requireTransactionOutcome(t *testing.T, result applyResult, outcome transaction.Outcome) {
	t.Helper()
	if result.Err != nil || result.Transaction.Outcome != outcome {
		t.Fatalf("transaction response %+v, want %s", result, outcome)
	}
}

func TestLocalTransactionChecksCurrentApplyBatch(t *testing.T) {
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	part := transaction.Part{
		Checks: []transaction.Check{{Namespace: "accounts", Key: "owner", MatchValue: true, Value: []byte("first")}},
		Writes: []storage.WriteOperation{{Namespace: "accounts", Key: "owner", Value: []byte("winner")}},
	}
	plan := transaction.Plan{ID: "same-input", Fingerprint: "a", Parts: []transaction.Part{part}}
	local := transaction.Control{Action: transaction.Local, AtMS: 100, Plan: &plan}
	write := command{Operation: opSet, Namespace: "accounts", Key: "owner", Value: []byte("first")}
	logs := []*raft.Log{
		transactionLog(t, 1, write),
		transactionLog(t, 2, command{Operation: opTransaction, Transaction: &local}),
	}
	results := f.ApplyBatch(logs)
	requireTransactionOutcome(t, results[1].(applyResult), transaction.Committed)
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 3, local), transaction.Committed)

	other := plan
	other.ID = "other-input"
	local.Plan = &other
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 4, local), transaction.Aborted)
	raftEntry(t, f, 5, write)
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 6, local), transaction.Aborted)
	other.Fingerprint = "different"
	if err := applyTransactionForTest(t, f, 7, local).Err; status.Code(err) != codes.AlreadyExists {
		t.Fatalf("reusing ID returned %v", err)
	}
	page, err := f.ReadChanges(t.Context(), 0, 10)
	if err != nil || len(page.Events) != 3 || string(page.Events[1].Value) != "winner" {
		t.Fatalf("local retries leaked or lost changes: %+v, %v", page, err)
	}
	if f.Failure() != nil {
		t.Fatalf("expected transaction rejection must not poison the FSM: %v", f.Failure())
	}
}

func TestPreparedTransactionFencesBatchReadsAndSlotSeal(t *testing.T) {
	path := t.TempDir()
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	prepare := transaction.Control{
		Action: transaction.Prepare, ID: "transfer", Sequence: 34, AtMS: 100, DeadlineMS: 200,
		Part: &transaction.Part{
			Checks: []transaction.Check{{Namespace: "a", Key: "checked"}},
			Writes: []storage.WriteOperation{{Namespace: "b", Key: "written", Value: []byte("committed")}},
		},
	}
	conflict := command{Operation: opSet, Namespace: "a", Key: "checked", Value: []byte("conflict")}
	unrelated := command{Operation: opSet, Namespace: "c", Key: "unrelated", Value: []byte("allowed")}
	nested := command{Operation: opBatch, Commands: []command{
		{Operation: opSet, Namespace: "b", Key: "written", Value: []byte("conflict")},
		{Operation: opSet, Namespace: "b", Key: "must-not-leak", Value: []byte("bad")},
	}}
	logs := []*raft.Log{
		transactionLog(t, 1, command{Operation: opTransaction, Transaction: &prepare}),
		transactionLog(t, 2, command{Operation: opMicrobatch, Commands: []command{conflict, nested, unrelated}}),
		transactionLog(t, 3, command{Operation: opFence, Namespace: "a", Slot: uint32(sharding.Slot("a", "checked")), Sealed: true}),
	}
	responses := f.ApplyBatch(logs)
	p := responses[0].(applyResult)
	if p.Err != nil || !p.Transaction.Prepared {
		t.Fatalf("prepare response %+v", p)
	}
	resp := responses[1].(applyResult)
	if resp.Err != nil || len(resp.Errors) != 3 || status.Code(resp.Errors[0]) != codes.Unavailable ||
		status.Code(resp.Errors[1]) != codes.Unavailable || resp.Errors[2] != nil {
		t.Fatalf("prepared microbatch response %+v", resp)
	}
	if status.Code(responses[2].(applyResult).Err) != codes.Unavailable || f.Failure() != nil {
		t.Fatalf("prepared slot seal response %+v, failure=%v", responses[2], f.Failure())
	}
	if _, _, err := f.Get(t.Context(), "a", "checked"); status.Code(err) != codes.Unavailable {
		t.Fatalf("prepared read returned %v", err)
	}
	page, err := f.ReadChanges(t.Context(), 0, 10)
	if err != nil || len(page.Events) != 1 || page.Events[0].Key != "unrelated" {
		t.Fatalf("prepare leaked a change: %+v, %v", page, err)
	}

	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	snapshot.Release()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if err := f.ScanNamespace(t.Context(), "b", func(storage.ScanItem) error { return nil }); status.Code(err) != codes.Unavailable {
		t.Fatalf("restored scan returned %v", err)
	}
	if err := f.ScanNamespace(t.Context(), "c", func(storage.ScanItem) error { return nil }); err != nil {
		t.Fatalf("unrelated namespace is blocked: %v", err)
	}

	commit := transaction.Control{Action: transaction.Commit, ID: prepare.ID, Sequence: prepare.Sequence}
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 4, commit), transaction.Committed)
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 5, commit), transaction.Committed)
	commit.Action = transaction.Abort
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 6, commit), transaction.Committed)
	record, found, err := f.Get(t.Context(), "b", "written")
	if err != nil || !found || string(record.Value) != "committed" {
		t.Fatalf("commit value %+v, found=%v, err=%v", record, found, err)
	}
	if _, found, err := f.Get(t.Context(), "b", "must-not-leak"); err != nil || found {
		t.Fatalf("nested batch partly applied: found=%v, err=%v", found, err)
	}
	page, err = f.ReadChanges(t.Context(), 0, 10)
	if err != nil || len(page.Events) != 2 || page.Events[1].Key != "written" {
		t.Fatalf("commit retries changed history: %+v, %v", page, err)
	}
}

func TestTransactionCoordinatorDecisionAndReceiptsSurviveReopen(t *testing.T) {
	path := t.TempDir()
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	plan := transaction.Plan{ID: "registered", Fingerprint: "a", DeadlineMS: 200,
		Parts: []transaction.Part{{Checks: []transaction.Check{{Namespace: "a", Key: "key"}}}}}
	register := transaction.Control{Action: transaction.Register, AtMS: 100, Plan: &plan}
	first := applyTransactionForTest(t, f, 1, register)
	requireTransactionOutcome(t, first, transaction.Pending)
	second := applyTransactionForTest(t, f, 2, register)
	if second.Err != nil || second.Transaction.State.Sequence != first.Transaction.State.Sequence {
		t.Fatalf("duplicate registration returned %+v", second)
	}
	decision := transaction.Control{Action: transaction.DecideCommit, ID: plan.ID, Sequence: first.Transaction.State.Sequence, AtMS: 199}
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 3, decision), transaction.Committed)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.transactionCurrent()
	if err != nil || current.State == nil || current.State.Outcome != transaction.Committed || len(current.State.Plan.Parts) != 1 {
		t.Fatalf("durable decision %+v, %v", current, err)
	}
	decision.Action = transaction.DecideAbort
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 4, decision), transaction.Committed)
	decision.Action = transaction.Finish
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 5, decision), transaction.Committed)
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 6, decision), transaction.Committed)
	after := applyTransactionForTest(t, f, 7, register)
	requireTransactionOutcome(t, after, transaction.Committed)
	if len(after.Transaction.State.Plan.Parts) != 0 || after.Transaction.State.Sequence != decision.Sequence {
		t.Fatalf("unexpected final receipt %+v", after.Transaction.State)
	}
	next := plan
	next.ID = "arrived-too-late"
	register.Plan, register.AtMS = &next, next.DeadlineMS
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 8, register), transaction.Aborted)
	register.AtMS--
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 9, register), transaction.Aborted)
	if current, err = f.transactionCurrent(); err != nil || current.State != nil {
		t.Fatalf("late registration left active state %+v, %v", current, err)
	}
}

func TestParticipantRejectsExpiredComparisonAndLatePrepare(t *testing.T) {
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	raftEntry(t, f, 1, newSetCommand("n", "expiring", []byte("initial"), 150))
	control := transaction.Control{
		Action: transaction.Prepare, ID: "expires-before-decision", Sequence: 30, AtMS: 100, DeadlineMS: 200,
		Part: &transaction.Part{Checks: []transaction.Check{{Namespace: "n", Key: "expiring", Revision: 1}}},
	}
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 2, control), transaction.Aborted)
	raftEntry(t, f, 3, newSetCommand("n", "expiring", []byte("initial"), 0))
	control.Part.Checks[0].Revision = 2
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 4, control), transaction.Aborted)
	abort := transaction.Control{Action: transaction.Abort, ID: "not-prepared", Sequence: 31}
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 5, abort), transaction.Aborted)
	control.ID, control.Sequence = abort.ID, abort.Sequence
	requireTransactionOutcome(t, applyTransactionForTest(t, f, 6, control), transaction.Aborted)
}
