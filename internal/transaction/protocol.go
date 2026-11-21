// Package transaction defines the transaction participant and coordinator
// protocol shared by the router and storage engines.
package transaction

import (
	"context"

	"github.com/byPixelTV/kvstore/internal/storage"
)

type Backend interface {
	ControlTransaction(context.Context, Control) (Reply, error)
}

type Outcome string

const (
	Pending   Outcome = "pending"
	Committed Outcome = "committed"
	Aborted   Outcome = "aborted"
)

type Action string

const (
	Local        Action = "local"
	Current      Action = "current"
	Register     Action = "register"
	DecideCommit Action = "decide-commit"
	DecideAbort  Action = "decide-abort"
	Finish       Action = "finish"
	Prepare      Action = "prepare"
	Commit       Action = "commit"
	Abort        Action = "abort"
)

// A revision of zero requires logical absence, including expired records.
// MatchValue requires a present record with exactly the supplied bytes.
type Check struct {
	Namespace  string
	Key        string
	Revision   uint64
	MatchValue bool
	Value      []byte
}

type Part struct {
	Shard  int
	Checks []Check
	Writes []storage.WriteOperation
}

// Plan binds a request ID to immutable input and its owners at registration.
// The fingerprint is computed from the public request before routing.
type Plan struct {
	ID            string
	Fingerprint   string
	DeadlineMS    int64
	Parts         []Part
	RouteConflict bool
}

type State struct {
	Sequence uint64
	Plan     Plan
	Outcome  Outcome
}

// AtMS is a trusted wall-clock sample replicated in the control log.
// Sequence is allocated by the coordinator and used to fence late prepares.
type Control struct {
	Action     Action
	AtMS       int64
	DeadlineMS int64
	ID         string
	Sequence   uint64
	Plan       *Plan
	Part       *Part
}

type Reply struct {
	State    *State
	Outcome  Outcome
	Prepared bool
}
