package sharding

import (
	"context"
	"encoding/json"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/transaction"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const transactionRecoveryTimeout = 5 * time.Second

// Transaction coordination and slot ownership changes share migrationMu. The
// authoritative plan and outcome are durable on shard 0. This mutex only
// coordinates work within the existing single active router.
func (r *Router) Transact(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	plan, err := transaction.ParseRequest(req)
	if err != nil {
		return nil, err
	}
	if r.fenceToken == "" {
		return nil, status.Error(codes.FailedPrecondition, "KVSTORE_JOIN_TOKEN required for sharded transactions")
	}
	if !r.loaded.Load() {
		return nil, status.Error(codes.FailedPrecondition, "router routing state unavailable")
	}
	if !r.migrationMu.TryLock() {
		return nil, status.Error(codes.Unavailable, "another transaction or slot migration is in progress")
	}
	defer r.migrationMu.Unlock()
	if err := r.checkRouteAvailable(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	ctx, cancel := context.WithTimeout(ctx, transactionRecoveryTimeout)
	defer cancel()

	if previous := r.uncertainRegistration; previous != nil {
		if previous.ID != plan.ID {
			if err := r.recoverTransactionLocked(ctx); err != nil {
				return nil, unknownTransactionOutcome(previous.ID, err)
			}
		} else if previous.Fingerprint != plan.Fingerprint {
			return nil, status.Error(codes.AlreadyExists, "transaction request ID was used for different input")
		}
	}
	wasUncertain := r.uncertainRegistration != nil
	if wasUncertain {
		plan = *r.uncertainRegistration
	} else {
		plan = routeTransactionPlan(r.current(), plan)
	}

	wasPaused := r.transactionWatch.Load() != nil
	r.pauseTransactionWatch()
	r.uncertainRegistration = &plan
	state, err := r.registerTransaction(ctx, &plan)
	if err != nil {
		switch status.Code(err) {
		case codes.AlreadyExists:
			r.uncertainRegistration = nil
			return nil, err
		case codes.Aborted:
			if !wasUncertain {
				r.uncertainRegistration = nil
			}
			return nil, err
		}
		return nil, unknownTransactionOutcome(plan.ID, err)
	}
	r.uncertainRegistration = nil
	if len(state.Plan.Parts) == 0 {
		// An old receipt can coexist with a different active transaction.
		// Preserve its existing watch pause until background recovery settles it.
		if !wasPaused {
			r.resumeTransactionWatch()
		}
		return &pb.TransactionResponse{Committed: state.Outcome == transaction.Committed}, nil
	}
	outcome, err := r.driveTransaction(ctx, state)
	if err != nil {
		return nil, unknownTransactionOutcome(plan.ID, err)
	}
	r.resumeTransactionWatch()
	return &pb.TransactionResponse{Committed: outcome == transaction.Committed}, nil
}

func routeTransactionPlan(layout *routingState, plan transaction.Plan) transaction.Plan {
	var parts [ShardCount]transaction.Part
	for _, check := range plan.Parts[0].Checks {
		shard := layout.owner(check.Namespace, Slot(check.Namespace, check.Key))
		if !check.MatchValue && check.Revision != 0 {
			source := int(check.Revision>>revisionBits) - 1
			check.Revision &= maxLocalRevision
			if source != shard || check.Revision == 0 {
				plan.RouteConflict = true
			}
		}
		parts[shard].Checks = append(parts[shard].Checks, check)
	}
	for _, write := range plan.Parts[0].Writes {
		shard := layout.owner(write.Namespace, Slot(write.Namespace, write.Key))
		parts[shard].Writes = append(parts[shard].Writes, write)
	}
	plan.Parts = nil
	for shard, part := range parts {
		if len(part.Checks)+len(part.Writes) != 0 {
			part.Shard = shard
			plan.Parts = append(plan.Parts, part)
		}
	}
	return plan
}

func transactionOwnersCurrent(layout *routingState, parts []transaction.Part) bool {
	for _, part := range parts {
		for _, check := range part.Checks {
			if layout.owner(check.Namespace, Slot(check.Namespace, check.Key)) != part.Shard {
				return false
			}
		}
		for _, write := range part.Writes {
			if layout.owner(write.Namespace, Slot(write.Namespace, write.Key)) != part.Shard {
				return false
			}
		}
	}
	return true
}

func (r *Router) transactionControl(ctx context.Context, shard int, request transaction.Control) (transaction.Reply, error) {
	if shard < 0 || shard >= ShardCount {
		return transaction.Reply{}, status.Error(codes.Internal, "transaction plan has invalid shard")
	}
	backend, err := r.backend(shard)
	if err != nil {
		return transaction.Reply{}, err
	}
	request.AtMS = time.Now().UnixMilli()
	payload, err := json.Marshal(request)
	if err != nil {
		return transaction.Reply{}, err
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+r.fenceToken))
	response, err := backend.ControlTransaction(ctx, &pb.TransactionControlRequest{Payload: payload})
	if err != nil {
		return transaction.Reply{}, err
	}
	var reply transaction.Reply
	if err := json.Unmarshal(response.GetPayload(), &reply); err != nil {
		return transaction.Reply{}, status.Error(codes.Internal, "invalid transaction control reply")
	}
	return reply, nil
}

func (r *Router) registerTransaction(ctx context.Context, plan *transaction.Plan) (*transaction.State, error) {
	reply, err := r.transactionControl(ctx, 0, transaction.Control{Action: transaction.Register, Plan: plan})
	if err != nil {
		return nil, err
	}
	state := reply.State
	if state == nil || state.Sequence == 0 || state.Plan.ID != plan.ID || state.Plan.Fingerprint != plan.Fingerprint {
		return nil, status.Error(codes.Internal, "coordinator returned an invalid transaction registration")
	}
	if len(state.Plan.Parts) == 0 && !decidedTransaction(state.Outcome) {
		return nil, status.Error(codes.Internal, "coordinator returned an undecided transaction receipt")
	}
	return state, nil
}

func decidedTransaction(outcome transaction.Outcome) bool {
	return outcome == transaction.Committed || outcome == transaction.Aborted
}

func unknownTransactionOutcome(id string, err error) error {
	return status.Errorf(codes.Unavailable, "transaction %s outcome unavailable; retry with the same request ID: %v", id, err)
}

func (r *Router) driveTransaction(ctx context.Context, state *transaction.State) (transaction.Outcome, error) {
	if state.Outcome == transaction.Pending {
		action := transaction.DecideAbort
		if !state.Plan.RouteConflict && transactionOwnersCurrent(r.current(), state.Plan.Parts) &&
			time.Now().UnixMilli() < state.Plan.DeadlineMS {
			action = transaction.DecideCommit
			for i := range state.Plan.Parts {
				part := &state.Plan.Parts[i]
				reply, err := r.transactionControl(ctx, part.Shard, transaction.Control{
					Action: transaction.Prepare, ID: state.Plan.ID, Sequence: state.Sequence,
					DeadlineMS: state.Plan.DeadlineMS, Part: part,
				})
				if err != nil {
					return "", err
				}
				if !reply.Prepared {
					action = transaction.DecideAbort
					break
				}
			}
		}
		var err error
		state, err = r.decideTransaction(ctx, state, action)
		if err != nil {
			return "", err
		}
	}
	if err := r.finishTransaction(ctx, state); err != nil {
		return "", err
	}
	return state.Outcome, nil
}

func (r *Router) decideTransaction(ctx context.Context, state *transaction.State, action transaction.Action) (*transaction.State, error) {
	reply, err := r.transactionControl(ctx, 0, transaction.Control{
		Action: action, ID: state.Plan.ID, Sequence: state.Sequence,
	})
	if err != nil {
		return nil, err
	}
	decision := reply.State
	if decision == nil || decision.Sequence != state.Sequence || decision.Plan.ID != state.Plan.ID ||
		decision.Plan.Fingerprint != state.Plan.Fingerprint || !decidedTransaction(decision.Outcome) {
		return nil, status.Error(codes.Internal, "coordinator returned an invalid transaction decision")
	}
	return decision, nil
}

func (r *Router) finishTransaction(ctx context.Context, state *transaction.State) error {
	if !decidedTransaction(state.Outcome) || len(state.Plan.Parts) == 0 {
		return status.Error(codes.Internal, "cannot finalize transaction without a durable decision and participants")
	}
	action := transaction.Abort
	if state.Outcome == transaction.Committed {
		action = transaction.Commit
	}
	for _, part := range state.Plan.Parts {
		// Abort all participants, even those whose Prepare wasn't acknowledged.
		// Its sequence fence prevents a delayed Prepare from locking keys later.
		reply, err := r.transactionControl(ctx, part.Shard, transaction.Control{
			Action: action, ID: state.Plan.ID, Sequence: state.Sequence,
		})
		if err != nil {
			return err
		}
		if reply.Outcome != state.Outcome {
			return status.Error(codes.Internal, "participant outcome differs from coordinator decision")
		}
	}
	reply, err := r.transactionControl(ctx, 0, transaction.Control{
		Action: transaction.Finish, ID: state.Plan.ID, Sequence: state.Sequence,
	})
	if err != nil {
		return err
	}
	receipt := reply.State
	if receipt == nil || receipt.Sequence != state.Sequence || receipt.Plan.ID != state.Plan.ID ||
		receipt.Plan.Fingerprint != state.Plan.Fingerprint || len(receipt.Plan.Parts) != 0 || receipt.Outcome != state.Outcome {
		return status.Error(codes.Internal, "coordinator returned an invalid transaction receipt")
	}
	return nil
}

func (r *Router) recoverTransactions(ctx context.Context) error {
	if !r.migrationMu.TryLock() {
		return nil
	}
	defer r.migrationMu.Unlock()
	return r.recoverTransactionLocked(ctx)
}

// recoverTransactionLocked also runs before changing slot ownership. A plan
// without a durable decision may be aborted after a leader or router restart.
// A committed plan is always replayed to its original participants.
func (r *Router) recoverTransactionLocked(ctx context.Context) error {
	if r.fenceToken == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, transactionRecoveryTimeout)
	defer cancel()
	for {
		var registrationErr error
		if plan := r.uncertainRegistration; plan != nil {
			// An absent Current reply alone cannot rule out a delayed Register.
			// Re-registering the same plan durably settles the ID before proceeding.
			_, registrationErr = r.registerTransaction(ctx, plan)
			switch status.Code(registrationErr) {
			case codes.OK, codes.AlreadyExists:
				r.uncertainRegistration = nil
			case codes.Aborted, codes.Unavailable:
				// Probe for an active transaction before retrying this plan.
			default:
				return registrationErr
			}
		}
		reply, err := r.transactionControl(ctx, 0, transaction.Control{Action: transaction.Current})
		if err != nil {
			return err
		}
		state := reply.State
		if state != nil {
			r.pauseTransactionWatch()
			if state.Outcome == transaction.Pending {
				state, err = r.decideTransaction(ctx, state, transaction.DecideAbort)
				if err != nil {
					return err
				}
			}
			if err := r.finishTransaction(ctx, state); err != nil {
				return err
			}
		}
		if r.uncertainRegistration == nil {
			r.resumeTransactionWatch()
			return nil
		}
		if state == nil {
			return registrationErr
		}
	}
}

type transactionWatchPause struct {
	done chan struct{}
}

func (r *Router) pauseTransactionWatch() {
	if r.transactionWatch.Load() == nil {
		r.transactionWatch.Store(&transactionWatchPause{done: make(chan struct{})})
	}
}

func (r *Router) resumeTransactionWatch() {
	if pause := r.transactionWatch.Swap(nil); pause != nil {
		close(pause.done)
	}
}

func (r *Router) waitTransactionWatch(ctx context.Context) error {
	if pause := r.transactionWatch.Load(); pause != nil {
		select {
		case <-pause.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
