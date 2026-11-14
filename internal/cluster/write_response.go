package cluster

// writeResponse carries the result of one queued cluster write back to its caller.
type writeResponse struct {
	revision uint64
	err      error
}
