package cluster

import "time"

// batchCollectionWindow increases the collection budget only when a Raft
// pipeline is already busy. The caller ends collection early at its target
// batch size, so a hot shard does not unnecessarily pay the whole budget.
func batchCollectionWindow(base, maximum time.Duration, outstanding int) time.Duration {
	if maximum < base {
		maximum = base
	}
	if outstanding >= 4 {
		return maximum
	}
	if outstanding >= 2 {
		if base >= maximum/2 {
			return maximum
		}
		return base * 2
	}
	return base
}
