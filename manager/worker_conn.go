package manager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"lowbit.dev/sets"
	"lowbit.dev/topic"
	"lowbit.dev/workforce/contract"
)

var (
	ErrWorkerInsufficientCapacity error = errors.New("worker has insufficient capacity")
	ErrWorkerAtCapacity           error = errors.New("worker at capacity")
)

// workerState mirrors the worker-side state machine but is tracked by the Manager per connection.
type workerState int32

const (
	workerStateOnline   workerState = 0
	workerStateDraining workerState = 1
	workerStateOffline  workerState = 2
)

func (s workerState) String() string {
	switch s {
	case workerStateOnline:
		return "online"
	case workerStateDraining:
		return "draining"
	case workerStateOffline:
		return "offline"
	default:
		return fmt.Sprintf("unknown(%d)", int32(s))
	}
}

// WorkerConn represents a single connected worker node.
type WorkerConn struct {
	// ---------------------------------------------------------
	// Immutable Fields (Read-only after initialization)
	// Safe to access concurrently without locks.
	// ---------------------------------------------------------
	workerID    string
	os          string
	arch        string
	capacity    int
	connectedAt time.Time

	// ---------------------------------------------------------
	// Network & I/O
	// ---------------------------------------------------------
	sendMu sync.Mutex
	conn   net.Conn
	// sendMu is replaced by the use of seriallane
	// sendMu sync.Mutex // serialises writes to the connection

	// ---------------------------------------------------------
	// Mutable State
	// ---------------------------------------------------------
	mu               sync.RWMutex
	state            workerState
	inFlight         sets.SimpleSet[string]
	occupiedCost     int
	lastIdleAt       time.Time
	lastHeartbeat    time.Time
	lastIdleNotified time.Time

	MessageReceivedBus *topic.Topic[contract.Message]
}

func NewWorkerConn(id, os, arch string, capacity int, conn net.Conn) *WorkerConn {
	now := time.Now()
	return &WorkerConn{
		workerID:           id,
		os:                 os,
		arch:               arch,
		capacity:           capacity,
		conn:               conn,
		connectedAt:        now,
		inFlight:           *sets.NewSimpleSet[string](),
		state:              workerStateOnline,
		lastIdleAt:         now,
		lastHeartbeat:      now,
		MessageReceivedBus: topic.New[contract.Message](),
	}
}

// ---------------------------------------------------------
// Helper Methods for State Access
// ---------------------------------------------------------

// State requires an RLock because the state can change.
func (w *WorkerConn) State() workerState {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.state
}

// State requires an RLock because the state can change.
func (w *WorkerConn) IsInState(state workerState) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.state == state
}

// SetState sets a new state for a worker
func (w *WorkerConn) SetState(state workerState) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.state = state
}

// Capacity returns the total capacity for a worker
func (w *WorkerConn) Capacity() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.capacity
}

// OverrideCapacity overrides the total worker capacity to the new value
func (w *WorkerConn) OverrideCapacity(capacity int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.capacity = capacity
}

// AvailableCapacity returns the remaining capacity: total capacity minus the sum of
// all in-flight job costs.
func (w *WorkerConn) AvailableCapacity() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.capacity - w.occupiedCost
}

// AssignTask adds a task and handles related state transitions atomically.
func (w *WorkerConn) AssignTask(taskID string, cost int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	avail := w.capacity - w.occupiedCost
	if avail <= 0 {
		return ErrWorkerAtCapacity
	}

	if (avail - cost) < 0 {
		return ErrWorkerInsufficientCapacity
	}

	w.inFlight.Add(taskID)
	w.occupiedCost += cost
	w.lastIdleNotified = time.Time{}

	return nil
}

// CompleteTask removes a task and reduces the occupied cost by the task's cost.
func (w *WorkerConn) CompleteTask(taskID string, cost int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.inFlight.Remove(taskID)
	w.occupiedCost -= cost
	if w.occupiedCost < 0 {
		w.occupiedCost = 0
	}
	w.lastIdleAt = time.Now()
}

// UpdateHeartbeat updates the heartbeat timestamp.
func (w *WorkerConn) UpdateHeartbeat() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastHeartbeat = time.Now()
}

// LastHeartbeat returns the time the last heartbeat was received
func (w *WorkerConn) LastHeartbeat() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.lastHeartbeat
}

// HeartbeatTimedOut returns is this worker has timed out based on the timeout duraction passed in
func (w *WorkerConn) HeartbeatTimedOut(timeout time.Duration) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return time.Since(w.lastHeartbeat) > timeout
}

// UpdateLastIdleTime updates the last idle time timestamp.
func (w *WorkerConn) UpdateLastIdleTime() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastIdleAt = time.Now()
}

// LastIdelTime returns the last idle time for this worker
func (w *WorkerConn) LastIdleTime() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.lastIdleAt
}

// IsIdle checks the map length, so it requires an RLock.
func (w *WorkerConn) IsIdle() bool {
	return w.inFlight.Size() == 0
}

// MarkIdleNotified records the current time as the last idle notification timestamp.
func (w *WorkerConn) MarkIdleNotified() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastIdleNotified = time.Now()
}

// LastIdleNotified returns the time the last idle notification was sent.
func (w *WorkerConn) LastIdleNotified() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastIdleNotified
}

// IdleNotificationPending returns true if an idle notification has not yet been sent
// since the last task assignment.
func (w *WorkerConn) IdleNotificationPending() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastIdleNotified.IsZero()
}

// ResetIdleNotified clears the idle notification timestamp, marking that a new
// notification should be sent when the worker becomes idle again.
func (w *WorkerConn) ResetIdleNotified() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastIdleNotified = time.Time{}
}

func (w *WorkerConn) Send(line string) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()

	_, err := fmt.Fprintln(w.conn, line)

	return err
}

// sendWithPayload writes a netargv message with a binary payload.
// header must be a fully formed netargv header without the "-- <n>" suffix.
// The payload length and bytes are appended atomically under sendMu.
func (w *WorkerConn) SendWithPayload(header string, payload []byte) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()

	if _, err := fmt.Fprintf(w.conn, "%s -- %d\n", header, len(payload)); err != nil {
		return err
	}

	_, err := w.conn.Write(payload)

	return err
}

// sendError sends a structured error message to the manager.
// code is a machine-readable token (e.g. "unknown_verb", "invalid_message").
// message is a human-readable description; single quotes are stripped to keep
// the netargv framing valid.
func (w *WorkerConn) SendError(code, message string) {
	safe := strings.ReplaceAll(message, "'", "")
	w.Send(fmt.Sprintf("error --code=%s --message='%s'", code, safe))
}

func (w *WorkerConn) WaitForResponse(ctx context.Context, matcher func(contract.Message) bool) contract.Message {
	result := make(chan contract.Message, 1)
	cancel := w.MessageReceivedBus.Subscribe(func(ctx context.Context, message contract.Message) error {
		if matcher(message) {
			select {
			case result <- message:
			default:
			}
		}

		return nil
	})

	defer cancel()

	select {
	case <-ctx.Done():
		return nil

	case msg := <-result:
		return msg
	}
}

// #####################################################################
//              Legacy
// #####################################################################

func (w *WorkerConn) inFlightIDs() []string {
	return w.inFlight.Snapshot()
}

func (w *WorkerConn) IsInFlight(taskID string) bool {
	return w.inFlight.Has(taskID)
}
