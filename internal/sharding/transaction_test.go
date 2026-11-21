package sharding

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/transaction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type transactionTestShard struct {
	pb.KVStoreClient
	shard int
	call  func(int, transaction.Control) (transaction.Reply, error)
}

func (s transactionTestShard) ControlTransaction(ctx context.Context, req *pb.TransactionControlRequest, _ ...grpc.CallOption) (*pb.TransactionControlResponse, error) {
	meta, _ := metadata.FromOutgoingContext(ctx)
	if !reflect.DeepEqual(meta.Get("authorization"), []string{"Bearer test-join-secret"}) {
		return nil, status.Error(codes.Unauthenticated, "wrong transaction authorization")
	}
	var control transaction.Control
	if err := json.Unmarshal(req.GetPayload(), &control); err != nil {
		return nil, err
	}
	reply, err := s.call(s.shard, control)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(reply)
	return &pb.TransactionControlResponse{Payload: payload}, err
}

type transactionStep struct {
	shard   int
	action  transaction.Action
	outcome transaction.Outcome
}

type scriptedTransaction struct {
	steps             []transactionStep
	state             *transaction.State
	fail              *transactionStep
	matchRegistration bool
}

func newTransactionTestRouter(t *testing.T, script *scriptedTransaction) *Router {
	t.Helper()
	r := &Router{fenceToken: "test-join-secret"}
	r.loaded.Store(true)
	for id := range r.groups {
		group := &group{}
		group.clients[0] = []pb.KVStoreClient{transactionTestShard{
			shard: id,
			call: func(shard int, control transaction.Control) (transaction.Reply, error) {
				if len(script.steps) == 0 {
					t.Fatalf("unexpected %s on shard %d", control.Action, shard)
				}
				step := script.steps[0]
				script.steps = script.steps[1:]
				if step.action != control.Action || step.shard != shard {
					t.Fatalf("got %s on shard %d, want %s on shard %d", control.Action, shard, step.action, step.shard)
				}
				if script.fail != nil && *script.fail == step {
					script.fail = nil
					if control.Action == transaction.Register {
						script.state = &transaction.State{Sequence: 17, Plan: *control.Plan, Outcome: transaction.Pending}
					}
					return transaction.Reply{}, status.Error(codes.Unavailable, "ambiguous transport result")
				}
				switch control.Action {
				case transaction.Register:
					if script.state == nil {
						script.state = &transaction.State{Sequence: 17, Plan: *control.Plan, Outcome: transaction.Pending}
					} else if script.matchRegistration && !reflect.DeepEqual(*control.Plan, script.state.Plan) {
						t.Fatalf("register replaced the original plan: %+v vs %+v", *control.Plan, script.state.Plan)
					}
					return transaction.Reply{State: script.state}, nil
				case transaction.Current:
					return transaction.Reply{State: script.state}, nil
				}
				state := script.state
				if state == nil || control.ID != state.Plan.ID || control.Sequence != state.Sequence {
					t.Fatalf("control escaped the registered ID/sequence: %+v", control)
				}
				switch control.Action {
				case transaction.Prepare:
					if control.Part == nil || control.Part.Shard != shard || control.DeadlineMS != state.Plan.DeadlineMS {
						t.Fatalf("wrong participant plan: %+v", control)
					}
					return transaction.Reply{Prepared: step.outcome == transaction.Pending, Outcome: step.outcome}, nil
				case transaction.DecideCommit, transaction.DecideAbort:
					state.Outcome = step.outcome
					return transaction.Reply{State: state}, nil
				case transaction.Commit, transaction.Abort:
					return transaction.Reply{Outcome: step.outcome}, nil
				case transaction.Finish:
					receipt := *state
					receipt.Plan.Parts = nil
					script.state = nil
					return transaction.Reply{State: &receipt}, nil
				default:
					t.Fatalf("unexpected action: %s", control.Action)
					return transaction.Reply{}, nil
				}
			},
		}}
		r.groups[id] = group
	}
	t.Cleanup(func() {
		if len(script.steps) != 0 {
			t.Errorf("missing transaction controls: %+v", script.steps)
		}
	})
	return r
}

func routedTestRequest() *pb.TransactionRequest {
	const namespace = "purchase"
	var keys [2]string
	for i := range keys {
		for key := 'a'; ; key++ {
			if Shard(namespace, string(key)) == i {
				keys[i] = string(key)
				break
			}
		}
	}
	revision, _ := EncodeRevision(1, 7)
	return &pb.TransactionRequest{
		RequestId: "purchase_test_0001",
		Checks: []*pb.TransactionCheck{
			{Namespace: namespace, Key: keys[1], Expected: &pb.TransactionCheck_Revision{Revision: revision}},
		},
		Operations: []*pb.BatchWriteOperation{
			{Namespace: namespace, Key: keys[0], Value: []byte("receipt")},
			{Namespace: namespace, Key: keys[1], Value: []byte("credit")},
		},
	}
}

func TestRouteTransactionUsesCurrentShardAndKeepsFingerprint(t *testing.T) {
	req := routedTestRequest()
	plan, err := transaction.ParseRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	r := &Router{}
	layout := r.current()
	routed := routeTransactionPlan(layout, plan)
	if routed.Fingerprint != plan.Fingerprint || routed.RouteConflict || len(routed.Parts) != 2 ||
		routed.Parts[0].Shard != 0 || len(routed.Parts[0].Writes) != 1 ||
		routed.Parts[1].Shard != 1 || routed.Parts[1].Checks[0].Revision != 7 {
		t.Fatalf("unexpected routed plan: %+v", routed)
	}
	namespace, checkKey := req.Checks[0].Namespace, req.Checks[0].Key
	next := layout.copy()
	next.Overrides[namespace] = map[uint16]uint8{Slot(namespace, checkKey): 2}
	moved := routeTransactionPlan(next, plan)
	if moved.Fingerprint != plan.Fingerprint || !moved.RouteConflict || moved.Parts[1].Shard != 2 ||
		transactionOwnersCurrent(next, routed.Parts) {
		t.Fatalf("routing change should invalidate the originally observed revision: %+v", moved)
	}
}

func TestTransactionDefersToDurableDecision(t *testing.T) {
	for _, test := range []struct {
		name     string
		prepare  transaction.Outcome
		decide   transaction.Action
		outcome  transaction.Outcome
		finalize transaction.Action
	}{
		{"commit", transaction.Pending, transaction.DecideCommit, transaction.Committed, transaction.Commit},
		{"negative vote", transaction.Aborted, transaction.DecideAbort, transaction.Aborted, transaction.Abort},
		{"commit requested after deadline", transaction.Pending, transaction.DecideCommit, transaction.Aborted, transaction.Abort},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := routedTestRequest()
			script := &scriptedTransaction{steps: []transactionStep{
				{0, transaction.Register, ""},
				{0, transaction.Prepare, test.prepare},
			}}
			if test.prepare == transaction.Pending {
				script.steps = append(script.steps, transactionStep{1, transaction.Prepare, transaction.Pending})
			}
			script.steps = append(script.steps,
				transactionStep{0, test.decide, test.outcome},
				transactionStep{0, test.finalize, test.outcome},
				transactionStep{1, test.finalize, test.outcome},
				transactionStep{0, transaction.Finish, ""},
			)
			r := newTransactionTestRouter(t, script)
			res, err := r.Transact(context.Background(), req)
			if err != nil || res.GetCommitted() != (test.outcome == transaction.Committed) {
				t.Fatalf("unexpected transaction result: %v, %v", res, err)
			}
		})
	}
}

func TestTransactionRecoversPartialFinalization(t *testing.T) {
	failedCommit := transactionStep{1, transaction.Commit, transaction.Committed}
	script := &scriptedTransaction{fail: &failedCommit, steps: []transactionStep{
		{0, transaction.Register, ""},
		{0, transaction.Prepare, transaction.Pending},
		{1, transaction.Prepare, transaction.Pending},
		{0, transaction.DecideCommit, transaction.Committed},
		{0, transaction.Commit, transaction.Committed},
		failedCommit,
		{0, transaction.Current, ""},
		{0, transaction.Commit, transaction.Committed},
		{1, transaction.Commit, transaction.Committed},
		{0, transaction.Finish, ""},
	}}
	r := newTransactionTestRouter(t, script)
	_, err := r.Transact(context.Background(), routedTestRequest())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("partial finalization must report an unknown result, got %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.waitTransactionWatch(ctx); err != context.DeadlineExceeded {
		t.Fatalf("watch may expose partial commit: %v", err)
	}
	if err := r.recoverTransactions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.waitTransactionWatch(ctx); err != nil {
		t.Fatalf("watch remains gated after finishing: %v", err)
	}
}

func TestTransactionSettlesUnknownRegistration(t *testing.T) {
	failedRegister := transactionStep{0, transaction.Register, ""}
	script := &scriptedTransaction{fail: &failedRegister, matchRegistration: true, steps: []transactionStep{
		failedRegister,
		{0, transaction.Register, ""},
		{0, transaction.Current, ""},
		{0, transaction.DecideAbort, transaction.Aborted},
		{0, transaction.Abort, transaction.Aborted},
		{1, transaction.Abort, transaction.Aborted},
		{0, transaction.Finish, ""},
	}}
	r := newTransactionTestRouter(t, script)
	if _, err := r.Transact(context.Background(), routedTestRequest()); status.Code(err) != codes.Unavailable {
		t.Fatalf("wanted unknown result, got %v", err)
	}
	if err := r.recoverTransactions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.uncertainRegistration != nil || r.transactionWatch.Load() != nil {
		t.Fatal("unknown registration not cleared after recovery")
	}
}

func TestTransactionReplaysCompletedReceipt(t *testing.T) {
	req := routedTestRequest()
	plan, err := transaction.ParseRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	plan.Parts = nil
	script := &scriptedTransaction{
		state: &transaction.State{Sequence: 17, Plan: plan, Outcome: transaction.Committed},
		steps: []transactionStep{{0, transaction.Register, ""}, {0, transaction.Register, ""}},
	}
	r := newTransactionTestRouter(t, script)
	for range 2 {
		res, err := r.Transact(context.Background(), req)
		if err != nil || !res.GetCommitted() {
			t.Fatalf("retry could not recover committed receipt: %v, %v", res, err)
		}
	}
	if r.transactionWatch.Load() != nil {
		t.Fatal("a compact receipt should not hold back watch delivery")
	}
}

func TestTransactionRetryAbortsStalePendingPlan(t *testing.T) {
	req := routedTestRequest()
	plan, err := transaction.ParseRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	script := &scriptedTransaction{steps: []transactionStep{
		{0, transaction.Register, ""},
		{0, transaction.DecideAbort, transaction.Aborted},
		{0, transaction.Abort, transaction.Aborted},
		{1, transaction.Abort, transaction.Aborted},
		{0, transaction.Finish, ""},
	}}
	r := newTransactionTestRouter(t, script)
	script.state = &transaction.State{Sequence: 17, Plan: routeTransactionPlan(r.current(), plan), Outcome: transaction.Pending}
	next := r.current().copy()
	write := req.Operations[0]
	next.Overrides[write.Namespace] = map[uint16]uint8{Slot(write.Namespace, write.Key): 2}
	r.install(next)
	res, err := r.Transact(context.Background(), req)
	if err != nil || res.GetCommitted() {
		t.Fatalf("stale durable plan should have aborted on its original participants: %v, %v", res, err)
	}
}

func TestMigrationWaitsForCommittedParticipants(t *testing.T) {
	req := routedTestRequest()
	plan, err := transaction.ParseRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	fail := transactionStep{1, transaction.Commit, transaction.Committed}
	script := &scriptedTransaction{fail: &fail, steps: []transactionStep{
		{0, transaction.Current, ""},
		{0, transaction.Commit, transaction.Committed},
		fail,
	}}
	r := newTransactionTestRouter(t, script)
	script.state = &transaction.State{Sequence: 17, Plan: routeTransactionPlan(r.current(), plan), Outcome: transaction.Committed}
	_, err = r.MigrateSlot(context.Background(), req.Operations[0].Namespace, Slot(req.Operations[0].Namespace, req.Operations[0].Key), 2)
	if status.Code(err) != codes.Unavailable || r.current().Epoch != 1 || r.unresolved.Load() {
		t.Fatalf("failed recovery should stop before migration starts: %v", err)
	}
	if r.transactionWatch.Load() == nil {
		t.Fatal("watch opened with a pending committed participant")
	}
}
