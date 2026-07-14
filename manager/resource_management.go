package manager

import (
	"context"
	"time"
)

// ResourceShortageEvent is passed to Config.OnResourceShortage when pending task cost
// exceeds total cluster capacity.
type ResourceShortageEvent struct {
	// PendingCost is the total capacity cost of all currently Pending tasks.
	PendingCost int
	// ClusterCapacity is the total declared capacity across all connected workers.
	ClusterCapacity int
	// PendingCount is the number of Pending tasks.
	PendingCount int
	// ConnectedWorkers is the current number of connected workers.
	ConnectedWorkers int
	// UnsatisfiedPlatforms lists OS/arch combinations for which Pending tasks exist but
	// no connected worker has sufficient remaining capacity to accept them.
	// Each entry tells the caller what type of worker to provision.
	UnsatisfiedPlatforms []PlatformDemand
}

// PlatformDemand describes unmet demand for a specific OS/arch combination.
type PlatformDemand struct {
	OS           string
	Arch         string
	PendingCost  int // total cost of Pending tasks requiring this platform
	PendingCount int // number of Pending tasks requiring this platform
}

// IdleWorkerEvent is passed to Config.OnIdleWorker when a worker has been idle
// past Config.IdleWorkerThreshold.
type IdleWorkerEvent struct {
	WorkerID     string
	IdleDuration time.Duration
	OS           string
	Arch         string
	// RemoteAddr is the network address of the worker connection (e.g. "192.168.1.42:51234").
	RemoteAddr string
	// Capacity is the worker's total declared capacity.
	Capacity int
	// ConnectedWorkers is the total number of connected workers at the time of this event.
	// Useful for avoiding scale-down of the last remaining worker.
	ConnectedWorkers int
	// PendingWorkExists reports whether any Pending tasks exist that this worker's
	// OS/arch combination could serve. When false, scale-down is safe without risking starvation.
	PendingWorkExists bool
	// DrainWorker sends TYPE_SYSTEM_CONTROL{drain} to this worker, causing it to stop accepting
	// new work and disconnect once its current tasks complete. Non-blocking — returns as soon as
	// the packet is sent; the worker drains and disconnects asynchronously. Non-nil always.
	DrainWorker func() error
	// DrainWorkerAndWait sends TYPE_SYSTEM_CONTROL{drain} and blocks until the worker has
	// fully disconnected or the context is cancelled. Use this when you need to act
	// immediately after the worker is gone (e.g. terminate the cloud host). Non-nil always.
	DrainWorkerAndWait func(ctx context.Context) error
	// DisconnectWorker closes the worker connection immediately. In-flight tasks are re-queued
	// as Pending. Non-nil always.
	DisconnectWorker func() error
}
