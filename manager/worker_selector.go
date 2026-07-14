package manager

import (
	"sync/atomic"
)

// WorkerSelector picks one worker from a non-empty slice of eligible workers for a task.
// All workers in the slice have already been verified to match OS/Arch and have sufficient capacity.
// The Manager always passes matched workers in stable sequential order.
// With a minimum of 1 worker in the slice.
type WorkerSelector interface {
	Select(workers []*WorkerConn) *WorkerConn
}

// WorkerSelectorFunc is a wrapper that makes a simple select func
// conform to the WorkerSelector interface
type WorkerSelectorFunc func(workers []*WorkerConn) *WorkerConn

func (wsf WorkerSelectorFunc) Select(workers []*WorkerConn) *WorkerConn {
	return wsf(workers)
}

// =====================================================================
// LeastLoadedSelector
// =====================================================================

// LeastLoadedSelector picks the worker with the highest available capacity.
// Ties are broken by earliest last-idle time (the worker that became free first).
// This is the default selector.
type LeastLoadedSelector struct{}

func (LeastLoadedSelector) Select(workers []*WorkerConn) *WorkerConn {
	best := workers[0]

	for _, w := range workers[1:] {
		wCap := w.AvailableCapacity()
		bestCap := best.AvailableCapacity()
		if wCap > bestCap {
			best = w
		} else if wCap == bestCap {
			if w.LastIdleTime().Before(best.LastIdleTime()) {
				best = w
			}
		}
	}

	return best
}

// =====================================================================
// RoundRobbinSelector
// =====================================================================

// RoundRobinSelector cycles through workers in stable sequential order.
// The Manager always passes workers in the same stable order, so this produces
// deterministic round-robin behaviour.
type RoundRobinSelector struct {
	counter atomic.Uint64
}

func (s *RoundRobinSelector) Select(workers []*WorkerConn) *WorkerConn {
	idx := s.counter.Add(1) - 1
	return workers[idx%uint64(len(workers))]
}
