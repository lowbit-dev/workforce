package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"lowbit.dev/retry"
	"lowbit.dev/rungroup"
	"lowbit.dev/seriallane"
	"lowbit.dev/ulid"
	"lowbit.dev/workforce/contract"
)

// hub manages all connected WorkerConns and indexes them by platform for efficient dispatch.
type WorkerPool struct {
	mu     sync.RWMutex
	logger *slog.Logger
	lanes  *seriallane.Manager

	// workers maps workerID → WorkerConn.
	workers map[string]*WorkerConn
	// platform maps "OS/Arch" → ordered list of WorkerConns.
	platform map[string][]*WorkerConn

	// dispatchSignal is the shared event-driven wake-up channel for the dispatcher.
	// A send (non-blocking) is performed whenever a worker becomes available or a task is enqueued.
	dispatchSignal chan struct{}

	// onJobsRequeued is called (with the job IDs) when a dead connection's in-flight jobs
	// are re-queued. The dispatcher wires this to reset job status and re-add to the heap.
	onJobsRequeued func(jobIDs []string)

	// scaleDownMu guards scaleDownCooldowns.
	scaleDownMu sync.Mutex
	// scaleDownCooldowns tracks the last OnIdleWorker call time per workerID.
	scaleDownCooldowns map[string]time.Time
}

func NewWorkerPool(logger *slog.Logger, dispatchSignal chan struct{}, onJobsRequeued func([]string)) *WorkerPool {
	return &WorkerPool{
		lanes:              seriallane.New(time.Minute * 5), // TODO: probably wanna get this passed in
		workers:            make(map[string]*WorkerConn),
		platform:           make(map[string][]*WorkerConn),
		logger:             logger,
		dispatchSignal:     dispatchSignal,
		onJobsRequeued:     onJobsRequeued,
		scaleDownCooldowns: make(map[string]time.Time),
	}
}

// TODO: probably add a register which returns a new Worker conn instead accepts one

// register adds or replaces a worker. If a worker with the same ID already exists,
// its connection is closed and its in-flight tasks are re-queued (§3.2).
func (h *WorkerPool) register(w *WorkerConn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := h.workers[w.workerID]; ok {
		h.logger.Warn("worker ID collision — replacing existing connection",
			"worker_id", w.workerID,
			"old_remote", old.conn.RemoteAddr(),
			"new_remote", w.conn.RemoteAddr(),
		)

		h.removeConnLocked(old) // TODO: this may be done a bit cleaner, ok for now
		old.conn.Close()

		if ids := old.inFlightIDs(); len(ids) > 0 && h.onJobsRequeued != nil {
			go h.onJobsRequeued(ids)
		}
	}

	key := platformKey(w.os, w.arch)
	h.workers[w.workerID] = w
	h.platform[key] = append(h.platform[key], w)
}

// unregister removes a worker connection from the hub and re-queues its in-flight tasks.
func (h *WorkerPool) unregister(w *WorkerConn) {
	h.mu.Lock()
	ids := w.inFlightIDs()
	h.removeConnLocked(w)
	h.mu.Unlock()

	if len(ids) > 0 && h.onJobsRequeued != nil {
		h.onJobsRequeued(ids)
	}
}

// removeConnLocked removes w from the workers map and platform index. Must hold h.mu write lock.
func (h *WorkerPool) removeConnLocked(w *WorkerConn) {
	delete(h.workers, w.workerID)
	key := platformKey(w.os, w.arch)
	list := h.platform[key]

	for i, conn := range list {
		if conn == w {
			h.platform[key] = append(list[:i], list[i+1:]...)
			break
		}
	}
}

func (p *WorkerPool) GetWorker(id string) (*WorkerConn, bool) {
	p.mu.RLock()
	w, ok := p.workers[id]
	p.mu.RUnlock()

	return w, ok
}

// eligibleWorkers returns workers whose platform key is in the platforms set and
// whose availableCapacity >= cost, in stable sequential order.
// If platforms is empty, all online workers with sufficient capacity are returned.
// If cost == 0, all matching workers are returned regardless of available capacity.
func (h *WorkerPool) eligibleWorkers(platforms []string, cost int, jobID string) []*WorkerConn {
	h.logger.Debug("[WorkerPool][eligibleWorkers] Finding eligible workers", "platforms", platforms, "cost", cost, "jobID", jobID)

	h.mu.RLock()
	defer h.mu.RUnlock()

	if cost < 0 {
		// Nevative cost must alwasy result in no workers
		return []*WorkerConn{}
	}

	var out []*WorkerConn
	if len(platforms) == 0 {
		// No platform constraint — iterate all workers.
		for _, w := range h.workers {
			if !w.IsInState(contract.WorkerStateOnline) {
				continue
			}

			if jobID != "" {
				if _, alreadyRejected := w.rejectedJobsCache.Get(jobID); alreadyRejected {
					continue
				}
			}

			if cost == 0 || w.AvailableCapacity() >= cost {
				out = append(out, w)
			}
		}

		return out
	}

	for _, key := range platforms {
		for _, w := range h.platform[key] {
			if !w.IsInState(contract.WorkerStateOnline) {
				continue
			}

			if jobID != "" {
				if _, alreadyRejected := w.rejectedJobsCache.Get(jobID); alreadyRejected {
					continue
				}
			}

			if cost == 0 || w.AvailableCapacity() >= cost {
				out = append(out, w)
			}
		}
	}

	return out
}

// subtractCapacity reduces a worker's available capacity and tracks the task as in-flight.
func (h *WorkerPool) subtractCapacity(ctx context.Context, w *WorkerConn, taskID string, cost int) error {
	return h.lanes.Do(ctx, seriallane.Namespace("worker", w.workerID), func(ctx context.Context) error {
		if err := w.AssignTask(taskID, cost); err != nil {
			return err
		}

		w.UpdateHeartbeat()

		return nil
	})
}

// restoreCapacity restores capacity after a task completes, fails, or is NACKed.
func (h *WorkerPool) restoreCapacity(ctx context.Context, w *WorkerConn, taskID string, cost int) error {
	return h.lanes.Do(ctx, seriallane.Namespace("worker", w.workerID), func(ctx context.Context) error {
		w.CompleteTask(taskID, cost)
		w.UpdateLastIdleTime()

		h.notifyDispatcher()

		return nil
	})
}

// overrideCapacity applies a TYPE_CAPACITY_UPDATE from the worker (worker-authoritative).
func (h *WorkerPool) overrideCapacity(ctx context.Context, workerID string, capacity int) error {
	h.mu.RLock()
	w, ok := h.workers[workerID]
	h.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown workerID: %s", workerID)
	}

	return h.lanes.Do(ctx, seriallane.Namespace("worker", w.workerID), func(ctx context.Context) error {
		w.UpdateLastIdleTime()
		w.OverrideCapacity(capacity)

		h.notifyDispatcher()

		return nil
	})
}

func (h *WorkerPool) FindWorkerByInFlightJobID(jobID string) *WorkerConn {
	for _, worker := range h.allWorkers() {
		if worker.IsInFlight(jobID) {
			return worker
		}
	}

	return nil
}

// notifyDispatcher sends a non-blocking signal to the dispatcher wake-up channel.
func (h *WorkerPool) notifyDispatcher() {
	select {
	case h.dispatchSignal <- struct{}{}:
	default:
	}
}

// totalCapacity returns the sum of all connected workers' declared total capacity.
func (h *WorkerPool) totalCapacity() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, w := range h.workers {
		total += w.Capacity()
	}

	return total
}

// allWorkers returns a snapshot of all connected workers for the GET /workers endpoint.
func (h *WorkerPool) allWorkers() []*WorkerConn {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make([]*WorkerConn, 0, len(h.workers))
	for _, w := range h.workers {
		out = append(out, w)
	}

	return out
}

func (h *WorkerPool) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.workers)
}

func (h *WorkerPool) HeartbeatMonitoringService(interval time.Duration, timeout time.Duration) rungroup.Service {
	return rungroup.NewIntervalService(interval, func(ctx context.Context) error {
		return h.checkHeartbeats(timeout)
	})
}

func (h *WorkerPool) checkHeartbeats(timeout time.Duration) error {
	h.mu.RLock()
	var stale []*WorkerConn
	for _, w := range h.workers {
		if w.HeartbeatTimedOut(timeout) {
			stale = append(stale, w)
		}
	}
	h.mu.RUnlock()

	for _, w := range stale {
		h.logger.Warn("worker heartbeat timeout — closing connection",
			"worker_id", w.workerID,
			"last_heartbeat", w.LastHeartbeat().String(),
		)

		if err := w.conn.Close(); err != nil {
			return err
		}
		h.unregister(w)
	}

	return nil
}

type StaleWorkerMonitoringServiceOptions struct {
	interval              time.Duration // On what interval should the stale workers be checked
	idleThreshold         time.Duration // How long should a worker be doing nothing to be considered idle
	scaleDownCooldown     time.Duration // How long should a worker be marked as idle untill its considered stale
	hasPendingForPlatform func(os, arch string) bool
	drainWorker           func(workerID string) error
	drainWorkerAndWait    func(ctx context.Context, workerID string) error
	disconnectWorker      func(workerID string) error
	onIdle                func(event IdleWorkerEvent)
}

func (h *WorkerPool) StaleWorkerMonitoringService(opts StaleWorkerMonitoringServiceOptions) rungroup.Service {
	return rungroup.NewIntervalService(opts.interval, func(ctx context.Context) error {
		if opts.onIdle == nil || opts.idleThreshold <= 0 {
			return fmt.Errorf("No onIdle callback or idleThreshold set: %w", rungroup.ErrDoNotRestart)
		}

		workers := h.allWorkers()

		now := time.Now()
		for _, w := range workers {
			// Only notify when the worker is fully idle (available == declared capacity).
			// Or its not currently online (draining or offline)
			if w.AvailableCapacity() < w.capacity || !w.IsInState(contract.WorkerStateOnline) {
				continue
			}

			if now.Sub(w.LastIdleTime()) < opts.idleThreshold {
				// Not idle for long enough
				continue
			}

			// Skip if we already fired during this idle window (reset on next task assignment).
			if w.lastIdleNotified.After(w.LastIdleTime()) {
				continue
			}

			// Enforce ScaleDownCooldown per workerID.
			h.scaleDownMu.Lock()
			if last, ok := h.scaleDownCooldowns[w.workerID]; ok && now.Sub(last) < opts.scaleDownCooldown {
				h.scaleDownMu.Unlock()
				continue
			}

			h.scaleDownCooldowns[w.workerID] = now
			h.scaleDownMu.Unlock()

			w.MarkIdleNotified()

			pendingWorkExists := opts.hasPendingForPlatform != nil && opts.hasPendingForPlatform(w.os, w.arch)

			opts.onIdle(IdleWorkerEvent{
				WorkerID:           w.workerID,
				IdleDuration:       time.Since(w.LastIdleTime()),
				OS:                 w.os,
				Arch:               w.arch,
				RemoteAddr:         w.conn.RemoteAddr().String(),
				Capacity:           w.capacity,
				ConnectedWorkers:   len(workers),
				PendingWorkExists:  pendingWorkExists,
				DrainWorker:        func() error { return opts.drainWorker(w.workerID) },
				DrainWorkerAndWait: func(ctx context.Context) error { return opts.drainWorkerAndWait(ctx, w.workerID) },
				DisconnectWorker:   func() error { return opts.disconnectWorker(w.workerID) },
			})
		}

		return nil
	})
}

// waitForWorkerDisconnect blocks until the worker with the given ID is no longer
// registered in the hub, or until ctx is cancelled.
func (h *WorkerPool) waitForWorkerDisconnect(ctx context.Context, workerID string) error {
	err := retry.Do(ctx, retry.Constant(200*time.Millisecond), func() error {
		h.mu.RLock()
		defer h.mu.RUnlock()

		if _, found := h.workers[workerID]; !found {
			// Its not present in the worker list, assume its disconnected
			return retry.ErrDoNotRetry
		}

		return errors.New("")
	})

	// When the try loop told us not to retry, lets absorb that error then
	if errors.Is(err, retry.ErrDoNotRetry) {
		return nil
	}

	return err
}

var (
	ErrMissingWorkerOS   error = errors.New("missing worker os")
	ErrMissingWorkerArch error = errors.New("missing worker arch")
	ErrCapacityZero      error = errors.New("capacity is zero")
)

func (p *WorkerPool) NewWorkerConnection(id, os, arch string, capacity int, conn net.Conn) (*WorkerConn, error) {
	if os == "" {
		return nil, ErrMissingWorkerOS
	}

	if arch == "" {
		return nil, ErrMissingWorkerArch
	}

	if capacity < 1 {
		return nil, ErrCapacityZero
	}

	if id == "" {
		id = fmt.Sprintf("worker-%s_%s-%s", os, arch, ulid.Make().String())
	}

	wc := NewWorkerConn(id, os, arch, capacity, conn)

	p.register(wc)

	return wc, nil
}

func platformKey(os, arch string) string {
	return os + "/" + arch
}
