package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/byPixelTV/kvstore/internal/transaction"
	"github.com/cockroachdb/pebble/v2"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	transactionCoordinatorKey = []byte{5, 1}
	transactionParticipantKey = []byte{5, 2}
	transactionFinalizedKey   = []byte{5, 3}
)

func transactionReceiptKey(id string) []byte { return append([]byte{5, 4}, id...) }
func participantReceiptKey(sequence uint64) []byte {
	return append([]byte{5, 5}, encode64(sequence)...)
}

type transactionKey struct{ namespace, key string }

type participantIntent struct {
	ID         string
	Sequence   uint64
	DeadlineMS int64
	Part       transaction.Part

	keys       map[transactionKey]struct{}
	namespaces map[string]struct{}
	slots      map[sealID]struct{}
}

func (p *participantIntent) index() {
	p.keys = make(map[transactionKey]struct{}, len(p.Part.Checks)+len(p.Part.Writes))
	p.namespaces = make(map[string]struct{})
	p.slots = make(map[sealID]struct{})
	add := func(namespace, key string) {
		p.keys[transactionKey{namespace, key}] = struct{}{}
		p.namespaces[namespace] = struct{}{}
		p.slots[sealID{namespace, sharding.Slot(namespace, key)}] = struct{}{}
	}
	for _, check := range p.Part.Checks {
		add(check.Namespace, check.Key)
	}
	for _, write := range p.Part.Writes {
		add(write.Namespace, write.Key)
	}
}

func (p *participantIntent) holdsKey(namespace, key string) bool {
	if p == nil {
		return false
	}
	_, held := p.keys[transactionKey{namespace, key}]
	return held
}

func (p *participantIntent) holdsNamespace(namespace string) bool {
	if p == nil {
		return false
	}
	_, held := p.namespaces[namespace]
	return held
}

func (p *participantIntent) holdsSlot(slot sealID) bool {
	if p == nil {
		return false
	}
	_, held := p.slots[slot]
	return held
}

func preparedTransactionError() error {
	return status.Error(codes.Unavailable, "key, namespace, or slot is held by a prepared transaction; retry")
}

type transactionMetadata struct {
	coordinator *transaction.State
	participant *participantIntent
	finalized   uint64
}

type participantReceipt struct {
	ID      string
	Outcome transaction.Outcome
}

type stagedRecord struct {
	record storage.Record
	exists bool
}

// transactionBatch holds uncommitted decisions and intents beside the regular
// writes. It is published only after the batch and its Raft index are durable.
type transactionBatch struct {
	transactionMetadata
	db                  *pebble.DB
	batch               *pebble.Batch
	shadow              map[string]stagedRecord
	isSealed            func(command) bool
	receipts            map[string]*transaction.State
	participantReceipts map[uint64]*participantReceipt
}

func loadTransactionMetadata[T any](db *pebble.DB, key []byte) (*T, error) {
	raw, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var metadata T
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, fmt.Errorf("decode transaction metadata %x: %w", key, err)
	}
	return &metadata, nil
}

func (f *FSM) refreshTransactions() error {
	coordinator, err := loadTransactionMetadata[transaction.State](f.db, transactionCoordinatorKey)
	if err != nil {
		return err
	}
	participant, err := loadTransactionMetadata[participantIntent](f.db, transactionParticipantKey)
	if err != nil {
		return err
	}
	finalized, err := loadTransactionMetadata[uint64](f.db, transactionFinalizedKey)
	if err != nil {
		return err
	}
	meta := transactionMetadata{coordinator: coordinator, participant: participant}
	if finalized != nil {
		meta.finalized = *finalized
	}
	if participant != nil {
		if participant.Sequence <= meta.finalized {
			return errors.New("pending participant intent is already finalized")
		}
		participant.index()
	}
	f.transactions = meta
	return nil
}

func (t *transactionBatch) persist(key []byte, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return t.batch.Set(key, raw, nil)
}

func (t *transactionBatch) setCoordinator(state *transaction.State) error {
	if err := t.persist(transactionCoordinatorKey, state); err != nil {
		return err
	}
	t.coordinator = state
	return nil
}

func (t *transactionBatch) receipt(id string) (*transaction.State, error) {
	if receipt, ok := t.receipts[id]; ok {
		return receipt, nil
	}
	return loadTransactionMetadata[transaction.State](t.db, transactionReceiptKey(id))
}

func (t *transactionBatch) finalizeCoordinator(state *transaction.State) (*transaction.State, error) {
	receipt := &transaction.State{
		Sequence: state.Sequence,
		Outcome:  state.Outcome,
		Plan:     transaction.Plan{ID: state.Plan.ID, Fingerprint: state.Plan.Fingerprint},
	}
	if err := t.persist(transactionReceiptKey(receipt.Plan.ID), receipt); err != nil {
		return nil, err
	}
	if t.receipts == nil {
		t.receipts = make(map[string]*transaction.State)
	}
	t.receipts[receipt.Plan.ID] = receipt
	return receipt, nil
}

func (t *transactionBatch) participantReceipt(sequence uint64) (*participantReceipt, error) {
	if receipt, ok := t.participantReceipts[sequence]; ok {
		return receipt, nil
	}
	return loadTransactionMetadata[participantReceipt](t.db, participantReceiptKey(sequence))
}

func (t *transactionBatch) finalizeParticipant(id string, sequence uint64, outcome transaction.Outcome) error {
	if t.participant != nil {
		if err := t.batch.Delete(transactionParticipantKey, nil); err != nil {
			return err
		}
		t.participant = nil
	}
	receipt := &participantReceipt{ID: id, Outcome: outcome}
	if err := t.persist(participantReceiptKey(sequence), receipt); err != nil {
		return err
	}
	if err := t.persist(transactionFinalizedKey, sequence); err != nil {
		return err
	}
	t.finalized = sequence
	if t.participantReceipts == nil {
		t.participantReceipts = make(map[uint64]*participantReceipt)
	}
	t.participantReceipts[sequence] = receipt
	return nil
}

func transactionStateReply(state *transaction.State) transaction.Reply {
	if state == nil {
		return transaction.Reply{}
	}
	copyState := *state
	copyState.Plan.Parts = slices.Clone(state.Plan.Parts)
	for i := range copyState.Plan.Parts {
		part := &copyState.Plan.Parts[i]
		part.Checks = slices.Clone(part.Checks)
		for j := range part.Checks {
			part.Checks[j].Value = bytes.Clone(part.Checks[j].Value)
		}
		part.Writes = slices.Clone(part.Writes)
		for j := range part.Writes {
			part.Writes[j].Value = bytes.Clone(part.Writes[j].Value)
		}
	}
	return transaction.Reply{State: &copyState, Outcome: state.Outcome}
}

func (f *FSM) transactionCurrent() (transaction.Reply, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.failed != nil {
		return transaction.Reply{}, f.failed
	}
	return transactionStateReply(f.transactions.coordinator), nil
}

func (n *Node) ControlTransaction(ctx context.Context, control transaction.Control) (transaction.Reply, error) {
	if n.raft.State() != raft.Leader {
		if control.Action == transaction.Local {
			return n.forwardLocalTransaction(ctx, control)
		}
		return transaction.Reply{}, status.Error(codes.Unavailable, "transaction control requires the Raft leader")
	}
	if control.Action == transaction.Current {
		if err := n.readStrong(ctx); err != nil {
			return transaction.Reply{}, err
		}
		return n.fsm.transactionCurrent()
	}
	switch control.Action {
	case transaction.Local, transaction.Register, transaction.DecideCommit:
		control.AtMS = time.Now().UnixMilli()
	case transaction.Prepare:
		if control.AtMS == 0 {
			control.AtMS = time.Now().UnixMilli()
		}
	}
	result, err := n.applyDirect(ctx, command{Operation: opTransaction, Transaction: &control})
	return result.Transaction, err
}

func (n *Node) forwardLocalTransaction(ctx context.Context, control transaction.Control) (transaction.Reply, error) {
	leader, err := n.leaderPeer()
	if err != nil {
		return transaction.Reply{}, err
	}
	if leader.ID == n.cfg.NodeID {
		return transaction.Reply{}, status.Error(codes.Unavailable, "leader transition")
	}
	client, err := n.client(leader.GRPCAddr)
	if err != nil {
		return transaction.Reply{}, err
	}
	raw, err := json.Marshal(control)
	if err != nil {
		return transaction.Reply{}, err
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+n.cfg.JoinToken)
	resp, err := client.ControlTransaction(ctx, &kvstorev1.TransactionControlRequest{Payload: raw})
	if err != nil {
		return transaction.Reply{}, err
	}
	var reply transaction.Reply
	err = json.Unmarshal(resp.GetPayload(), &reply)
	return reply, err
}

func (t *transactionBatch) apply(index uint64, c *transaction.Control, applyWrite func(command) (uint64, error)) (transaction.Reply, error) {
	if c == nil {
		return transaction.Reply{}, errors.New("missing committed transaction control")
	}
	switch c.Action {
	case transaction.Local, transaction.Register:
		return t.register(index, c, applyWrite)
	case transaction.DecideCommit, transaction.DecideAbort, transaction.Finish:
		return t.decide(c)
	case transaction.Prepare, transaction.Commit, transaction.Abort:
		return t.participate(c, applyWrite)
	default:
		return transaction.Reply{}, status.Error(codes.InvalidArgument, "invalid transaction action")
	}
}

func (t *transactionBatch) register(index uint64, c *transaction.Control, applyWrite func(command) (uint64, error)) (transaction.Reply, error) {
	if c.Plan == nil || c.Plan.ID == "" || c.Plan.Fingerprint == "" || c.AtMS <= 0 ||
		(c.ID != "" && c.ID != c.Plan.ID) || len(c.Plan.Parts) == 0 || (c.Action == transaction.Local && len(c.Plan.Parts) != 1) {
		return transaction.Reply{}, status.Error(codes.InvalidArgument, "invalid transaction plan")
	}
	plan := c.Plan
	receipt, err := t.receipt(plan.ID)
	if err != nil {
		return transaction.Reply{}, err
	}
	if receipt == nil && t.coordinator != nil && t.coordinator.Plan.ID == plan.ID {
		receipt = t.coordinator
	}
	if receipt != nil {
		if receipt.Plan.Fingerprint != plan.Fingerprint {
			return transaction.Reply{}, status.Error(codes.AlreadyExists, "transaction ID was used for different input")
		}
		return transactionStateReply(receipt), nil
	}
	if c.Action == transaction.Register {
		if plan.DeadlineMS <= c.AtMS {
			receipt, err := t.finalizeCoordinator(&transaction.State{Sequence: index, Plan: *plan, Outcome: transaction.Aborted})
			return transactionStateReply(receipt), err
		}
		if t.coordinator != nil {
			return transaction.Reply{}, status.Error(codes.Aborted, "another coordinated transaction is active; retry")
		}
		state := &transaction.State{Sequence: index, Plan: *plan, Outcome: transaction.Pending}
		if err := t.setCoordinator(state); err != nil {
			return transaction.Reply{}, err
		}
		return transactionStateReply(state), nil
	}

	part := &plan.Parts[0]
	ok, err := t.checkPart(part, c.AtMS, 0)
	if err != nil {
		return transaction.Reply{}, err
	}
	outcome := transaction.Aborted
	if ok {
		if err := applyTransactionWrites(part.Writes, applyWrite); err != nil {
			return transaction.Reply{}, err
		}
		outcome = transaction.Committed
	}
	receipt, err = t.finalizeCoordinator(&transaction.State{Sequence: index, Plan: *plan, Outcome: outcome})
	if err != nil {
		return transaction.Reply{}, err
	}
	return transactionStateReply(receipt), nil
}

func (t *transactionBatch) decide(c *transaction.Control) (transaction.Reply, error) {
	if c.ID == "" || c.Sequence == 0 || (c.Action == transaction.DecideCommit && c.AtMS <= 0) {
		return transaction.Reply{}, status.Error(codes.InvalidArgument, "invalid transaction decision")
	}
	active := t.coordinator
	if active == nil || active.Plan.ID != c.ID {
		receipt, err := t.receipt(c.ID)
		if err != nil {
			return transaction.Reply{}, err
		}
		if receipt == nil || receipt.Sequence != c.Sequence {
			return transaction.Reply{}, status.Error(codes.FailedPrecondition, "coordinated transaction not found")
		}
		return transactionStateReply(receipt), nil
	}
	if active.Sequence != c.Sequence {
		return transaction.Reply{}, status.Error(codes.FailedPrecondition, "transaction sequence mismatch")
	}
	if c.Action == transaction.Finish {
		if active.Outcome == transaction.Pending {
			return transaction.Reply{}, status.Error(codes.FailedPrecondition, "transaction has no final decision")
		}
		receipt, err := t.finalizeCoordinator(active)
		if err != nil {
			return transaction.Reply{}, err
		}
		if err := t.batch.Delete(transactionCoordinatorKey, nil); err != nil {
			return transaction.Reply{}, err
		}
		t.coordinator = nil
		return transactionStateReply(receipt), nil
	}
	if active.Outcome == transaction.Pending {
		updated := *active
		updated.Outcome = transaction.Aborted
		if c.Action == transaction.DecideCommit && c.AtMS < active.Plan.DeadlineMS {
			updated.Outcome = transaction.Committed
		}
		if err := t.setCoordinator(&updated); err != nil {
			return transaction.Reply{}, err
		}
		active = &updated
	}
	return transactionStateReply(active), nil
}

func (t *transactionBatch) participate(c *transaction.Control, applyWrite func(command) (uint64, error)) (transaction.Reply, error) {
	if c.Sequence == 0 || c.ID == "" ||
		(c.Action == transaction.Prepare && (c.Part == nil || c.AtMS <= 0 || c.DeadlineMS <= 0)) {
		return transaction.Reply{}, status.Error(codes.InvalidArgument, "invalid participant transaction")
	}
	if c.Sequence <= t.finalized {
		receipt, err := t.participantReceipt(c.Sequence)
		if err != nil {
			return transaction.Reply{}, err
		}
		if receipt == nil || receipt.ID != c.ID {
			return transaction.Reply{}, status.Error(codes.FailedPrecondition, "participant transaction already superseded")
		}
		return transaction.Reply{Outcome: receipt.Outcome}, nil
	}
	intent := t.participant
	if intent != nil && (intent.ID != c.ID || intent.Sequence != c.Sequence) {
		return transaction.Reply{}, preparedTransactionError()
	}
	if c.Action == transaction.Prepare {
		if intent != nil {
			if intent.DeadlineMS != c.DeadlineMS || !reflect.DeepEqual(intent.Part, *c.Part) {
				return transaction.Reply{}, status.Error(codes.FailedPrecondition, "prepared transaction input mismatch")
			}
			return transaction.Reply{Outcome: transaction.Pending, Prepared: true}, nil
		}
		ok := false
		if c.AtMS < c.DeadlineMS {
			var err error
			ok, err = t.checkPart(c.Part, c.AtMS, c.DeadlineMS)
			if err != nil {
				return transaction.Reply{}, err
			}
		}
		if !ok {
			// A lost negative vote must stay negative, even if the state
			// now satisfies the same checks on a retried Prepare.
			if err := t.finalizeParticipant(c.ID, c.Sequence, transaction.Aborted); err != nil {
				return transaction.Reply{}, err
			}
			return transaction.Reply{Outcome: transaction.Aborted}, nil
		}
		intent = &participantIntent{ID: c.ID, Sequence: c.Sequence, DeadlineMS: c.DeadlineMS, Part: *c.Part}
		intent.index()
		if err := t.persist(transactionParticipantKey, intent); err != nil {
			return transaction.Reply{}, err
		}
		t.participant = intent
		return transaction.Reply{Outcome: transaction.Pending, Prepared: true}, nil
	}

	outcome := transaction.Aborted
	if c.Action == transaction.Commit {
		if intent == nil {
			return transaction.Reply{}, status.Error(codes.FailedPrecondition, "transaction was not prepared on this participant")
		}
		if err := applyTransactionWrites(intent.Part.Writes, applyWrite); err != nil {
			return transaction.Reply{}, err
		}
		outcome = transaction.Committed
	}
	if err := t.finalizeParticipant(c.ID, c.Sequence, outcome); err != nil {
		return transaction.Reply{}, err
	}
	return transaction.Reply{Outcome: outcome}, nil
}

func (t *transactionBatch) checkPart(part *transaction.Part, atMS, deadlineMS int64) (bool, error) {
	for _, write := range part.Writes {
		if write.Namespace == "" || write.Key == "" || write.ExpiresAtUnixMS < 0 || (write.Delete && write.ExpiresAtUnixMS != 0) {
			return false, status.Error(codes.InvalidArgument, "invalid transaction write")
		}
		if t.isSealed(command{Namespace: write.Namespace, Key: write.Key}) {
			return false, nil
		}
		if t.participant.holdsKey(write.Namespace, write.Key) {
			return false, preparedTransactionError()
		}
	}
	for _, check := range part.Checks {
		if check.Namespace == "" || check.Key == "" {
			return false, status.Error(codes.InvalidArgument, "invalid transaction comparison")
		}
		if t.isSealed(command{Namespace: check.Namespace, Key: check.Key}) {
			return false, nil
		}
		if t.participant.holdsKey(check.Namespace, check.Key) {
			return false, preparedTransactionError()
		}
		record, exists, err := t.getRecord(check.Namespace, check.Key)
		if err != nil {
			return false, err
		}
		exists = exists && !record.ExpiredAt(atMS)
		if exists && deadlineMS > 0 && record.ExpiresAtUnixMS > 0 && record.ExpiresAtUnixMS <= deadlineMS {
			return false, nil
		}
		if check.MatchValue {
			if !exists || !bytes.Equal(record.Value, check.Value) {
				return false, nil
			}
		} else if (check.Revision == 0 && exists) ||
			(check.Revision != 0 && (!exists || record.Revision != check.Revision)) {
			return false, nil
		}
	}
	return true, nil
}

func (t *transactionBatch) getRecord(namespace, key string) (storage.Record, bool, error) {
	encoded := encodeRecordKey(namespace, key)
	if record, ok := t.shadow[string(encoded)]; ok {
		return record.record, record.exists, nil
	}
	raw, closer, err := t.db.Get(encoded)
	if errors.Is(err, pebble.ErrNotFound) {
		return storage.Record{}, false, nil
	}
	if err != nil {
		return storage.Record{}, false, err
	}
	defer closer.Close()
	record, err := storage.DecodeRecord(raw)
	if err != nil {
		return storage.Record{}, false, err
	}
	record.Value = bytes.Clone(record.Value)
	return record, true, nil
}

func applyTransactionWrites(writes []storage.WriteOperation, applyWrite func(command) (uint64, error)) error {
	for _, write := range writes {
		cmd := newSetCommand(write.Namespace, write.Key, write.Value, write.ExpiresAtUnixMS)
		if write.Delete {
			cmd = command{Operation: opDelete, Namespace: write.Namespace, Key: write.Key}
		}
		if _, err := applyWrite(cmd); err != nil {
			return err
		}
	}
	return nil
}
