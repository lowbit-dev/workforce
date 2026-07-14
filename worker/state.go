package worker

import "sync/atomic"

// state represents the Worker's lifecycle state.
type state int32

const (
	stateOnline       state = iota // Connected; accepting proposals and running tasks.
	stateDraining                  // Not accepting new proposals; finishing in-progress tasks.
	stateShuttingDown              //
	stateOffline                   // Not connected; reconnect loop active or exited.
)

func (s state) String() string {
	switch s {
	case stateOnline:
		return "online"
	case stateDraining:
		return "draining"
	case stateShuttingDown:
		return "shuttingDown"
	case stateOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// atomicState wraps atomic.Int32 with typed helpers for the worker state machine.
type atomicState struct {
	v atomic.Int32
}

func (a *atomicState) load() state {
	return state(a.v.Load())
}

func (a *atomicState) store(s state) {
	a.v.Store(int32(s))
}

// transition atomically moves from oldState to newState.
// Returns true if the transition succeeded.
func (a *atomicState) transition(from, to state) bool {
	return a.v.CompareAndSwap(int32(from), int32(to))
}
