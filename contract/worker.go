package contract

import (
	"sync/atomic"
)

type WorkerState int32

const (
	WorkerStateOnline       WorkerState = iota // Connected; accepting proposals and running tasks.
	WorkerStatePressure                        // Not accepting new proposals; Experiancing CPU, Memory or disk pressure
	WorkerStateDraining                        // Not accepting new proposals; finishing in-progress tasks.
	WorkerStateOffline                         // Not connected; reconnect loop active or exited.
	WorkerStateShuttingDown                    //
)

func (s WorkerState) String() string {
	switch s {
	case WorkerStateOnline:
		return "online"
	case WorkerStatePressure:
		return "pressured"
	case WorkerStateDraining:
		return "draining"
	case WorkerStateShuttingDown:
		return "shuttingDown"
	case WorkerStateOffline:
		return "offline"
	default:
		return "unknown"
	}
}

func IsValidWorkerState(v int) bool {
	return WorkerState(v).String() != "unknown"
}

// AtomicWorkerState wraps atomic.Int32 with typed helpers for the worker state machine.
type AtomicWorkerState struct {
	v atomic.Int32
}

func (a *AtomicWorkerState) Is(s WorkerState) bool {
	return a.Load() == s
}

func (a *AtomicWorkerState) Load() WorkerState {
	return WorkerState(a.v.Load())
}

func (a *AtomicWorkerState) Store(s WorkerState) {
	a.v.Store(int32(s))
}

// transition atomically moves from oldState to newState.
// Returns true if the transition succeeded.
func (a *AtomicWorkerState) Transition(from, to WorkerState) bool {
	return a.v.CompareAndSwap(int32(from), int32(to))
}
