package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"lowbit.dev/retry"
	"lowbit.dev/rungroup"
	"lowbit.dev/sets"
	"lowbit.dev/urlsign"
	"lowbit.dev/verreg"
	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/artifact"
	"lowbit.dev/workforce/manager/webhooks"
)

// Manager is the central orchestrator. It dispatches jobs to connected workers,
// manages the job lifecycle, and serves the REST API.
//
// The Manager implements http.Handler — it does not own an HTTP server.
// The developer passes it to their own http.Server and controls TLS, timeouts, etc.
//
// Usage:
//
//		m, err := manager.New(cfg)
//		if err != nil { ... }
//		ctx, cancel := context.WithCancel(context.Background())
//		if err := m.Run(ctx); err != nil {
//	     	log.fatal(err)
//	 	}
//
//		http.ListenAndServe(":8080", m)
type Manager struct {
	cfg           Config
	workers       *WorkerPool
	messageVerreg *verreg.Registry[contract.MessageFactory]

	queue           *PriorityQueue[*contract.Job]
	cancelledJobIDs sets.SimpleSet[string] // jobs removed from the heap on cancel

	scaleMu           sync.RWMutex
	lastScaleDownCall time.Time // guarded by scaleMu; enforces ScaleDownCooldown
	lastScaleUpCall   time.Time // guarded by scaleMu; enforces ScaleUpCooldown

	/* Legacy */
	// disp      *dispatcher
	// pipe      *pipeline
	webhooks  *webhooks.WebhookDispatcher
	mux       http.Handler
	urlSigner *urlsign.Signer

	/* New */

	lastDispatchRun atomic.Uint64
	dispatchSignal  chan struct{}
}

// New validates cfg, applies defaults, and returns a ready Manager.
func New(cfg Config) (*Manager, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	cfg.Logger.Debug("[NewManager] Setting up signal...")
	signal := make(chan struct{})

	cfg.Logger.Debug("[NewManager] Setting up message registry...")
	r := verreg.NewRegistry[contract.MessageFactory]()
	contract.RegisterMessages(r)

	cfg.Logger.Debug("[NewManager] Building flat message map...")
	r.Build()

	cfg.Logger.Debug("[NewManager] Setting up Manager structure...")
	m := &Manager{
		cfg:           cfg,
		messageVerreg: r,

		lastDispatchRun: atomic.Uint64{},
		dispatchSignal:  signal,
		// pipe:           pipe,

		cancelledJobIDs: *sets.NewSimpleSet[string](),
		queue: NewPriorityQueue(func(a, b *contract.Job) bool {
			// Does A have a higher priority number? Then it pops first.
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}

			// Are they the same priority? Then the older one pops first.
			return a.CreatedAt.Before(b.CreatedAt)
		}),
	}

	cfg.Logger.Debug("[NewManager] Setting up Worker Pool...")
	m.workers = NewWorkerPool(cfg.Logger, signal, m.OnJobsRequeued)

	cfg.Logger.Debug("[NewManager] Setting up Webhook Dispatcher...")
	m.webhooks = webhooks.NewWebhookDispatcher(cfg.WebhookStore, cfg.Webhook, cfg.Logger)

	// disp := newDispatcher(&cfg, h, cfg.Jobs, cfg.Tasks, cfg.Artifacts, webhooks, signer, cfg.Logger, signal)
	// pipe := newPipeline(cfg.Jobs, cfg.Tasks, cfg.Artifacts, disp, webhooks, cfg.Logger)

	if len(cfg.ArtifactSigningKey) > 0 {
		var err error

		cfg.Logger.Debug("[NewManager] Setting up UrlSigner for artifacts...")
		m.urlSigner, err = urlsign.NewSigner(cfg.ArtifactSigningKey, cfg.ArtifactSignedURLTTL)
		if err != nil {
			return nil, fmt.Errorf("failed to create new artifact url signer: %w", err)
		}
	}

	cfg.Logger.Debug("[NewManager] Setting up http routes...")
	m.mux = m.buildMux()
	return m, nil
}

func (m *Manager) Run(ctx context.Context) error {

	rg := rungroup.New(
		rungroup.WithShutdownBoundary(),
		rungroup.WithShutdownTimeout(time.Minute*15),
		rungroup.WithEventHandler(func(e rungroup.Event) {
			slog.Info("[Manager][RunGroup] Event Received", "event", e)
		}),
	)

	rg.Add(rungroup.ServiceFunc(m.DispatcherRoutine),
		rungroup.WithName("JobDispatcherRoutine"),
		rungroup.WithBackoff(retry.MaxAttempts(10, retry.Exponential(10*time.Second, 5*time.Minute))),
		rungroup.WithStabilityWindow(5*time.Minute),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
	)

	rg.Add(m.webhooks,
		rungroup.WithName("WebhooksDispatcherRoutine"),
		rungroup.WithBackoff(retry.MaxAttempts(10, retry.Exponential(10*time.Second, 5*time.Minute))),
		rungroup.WithStabilityWindow(5*time.Minute),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
	)

	rg.Add(m.workers.StaleWorkerMonitoringService(StaleWorkerMonitoringServiceOptions{
		interval:          time.Minute * 15,
		idleThreshold:     time.Minute * 30,
		scaleDownCooldown: time.Minute * 25,

		/* Callbacks for cluster mutations and introspection */
		hasPendingForPlatform: m.HasPendingForPlatform,
		drainWorker:           m.DrainWorker,
		drainWorkerAndWait:    m.DrainWorkerAndWait,
		disconnectWorker:      m.DisconnectWorker,
		onIdle:                m.cfg.OnIdleWorker,
	}))

	rg.Add(rungroup.NewIntervalService(60*time.Second, func(ctx context.Context) error {
		if m.queue.Size() < 1 {
			// Nothing in the queue so no need to trigger a process run for it
			return nil
		}

		delta := uint64(time.Now().Unix()) - m.lastDispatchRun.Load()
		if delta < 60 {
			// Dispatcher processed the queue less than 60 seconds ago
			return nil
		}

		m.Logger().Info("[Dispatcher][PeriodicTriggerRoutine] Triggering dispatcher queue work...", "delta", delta)
		m.NotifyDispatcher()

		return nil
	}))

	// rg.Add(m.webhooks,
	// 	rungroup.WithName("WebhooksDispatcherRoutine"),
	// 	rungroup.WithBackoff(retry.MaxAttempts(10, retry.Exponential(10*time.Second, 5*time.Minute))),
	// 	rungroup.WithStabilityWindow(5*time.Minute),
	// 	rungroup.WithRestartPolicy(rungroup.RestartAlways),
	// )

	return rg.Run(ctx)
}

// RegisterJob/RegisterTask methods removed — seed task definitions directly
// into the TaskStore (cfg.Tasks) before calling New.

// recoverJobs rebuilds the dispatch heap from persisted pending and recoverable jobs.
func (m *Manager) RecoverJobs(ctx context.Context) error {
	pending, err := m.JobStore().ListPendingJobs(ctx)
	if err != nil {
		return fmt.Errorf("recover: list pending jobs: %w", err)
	}

	recoverable, err := m.JobStore().ListRecoverableJobs(ctx)
	if err != nil {
		return fmt.Errorf("recover: list recoverable jobs: %w", err)
	}

	// Reset recoverable jobs to Pending and close any orphaned runs.
	now := time.Now()
	for _, j := range recoverable {
		if m.cfg.RunStore != nil {
			runs, _ := m.cfg.RunStore.ListJobRuns(ctx, j.ID)
			for _, r := range runs {
				if r.Status == contract.RunStatusProvisioning || r.Status == contract.RunStatusRunning {
					_ = m.cfg.RunStore.UpdateRun(ctx, r.ID, func(run *contract.JobRun) {
						run.Status = contract.RunStatusFailed
						run.FailureReason = "recovered: manager restarted"
						run.FinishedAt = now
					})
				}
			}
		}

		err = m.JobStore().UpdateJob(ctx, j.ID, func(jb *contract.Job) {
			jb.Status = contract.JobStatusPending
		})

		if err != nil {
			m.Logger().Error("recover: reset job", "job_id", j.ID, "error", err)
		}

		j.Status = contract.JobStatusPending
	}

	m.EnqueueJobs(append(pending, recoverable...))

	m.Logger().Info("[Workforce][Manager]: Recovered jobs", "pending", len(pending), "recovered", len(recoverable))

	return nil
}

func (m *Manager) HasPendingForPlatform(os, arch string) bool {
	taskNames := make(map[string]struct{}, m.queue.Len())
	for _, e := range m.queue.Snapshot() {
		taskNames[e.Value.TaskName] = struct{}{}
	}

	reg := m.ArtifactRegistry()
	if reg == nil {
		return len(taskNames) > 0
	}

	ctx := context.Background()
	target := platformKey(os, arch)
	for taskName := range taskNames {
		platforms, err := reg.ListPlatforms(ctx, taskName, "")
		if err != nil {
			continue
		}
		for _, p := range platforms {
			if platformKey(p.OS, p.Arch) == target {
				return true
			}
		}
	}
	return false
}

// Stop initiates a graceful shutdown. It sends TYPE_SYSTEM_CONTROL{drain} to all
// connected workers, waits for in-progress tasks to complete or ctx to be cancelled,
// and stops internal goroutines.
// Blocks until shutdown is complete or ctx is cancelled.
func (m *Manager) Stop(ctx context.Context) error {
	m.drainWorkers(ctx)

	// Poll until no in-flight tasks remain or ctx expires.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			inFlight := 0

			for _, w := range m.workers.allWorkers() {
				inFlight += w.inFlight.Size()
			}

			if inFlight == 0 {
				return nil
			}
		}
	}
}

// ForceStop immediately closes all worker connections and stops internal goroutines.
// In-progress tasks on workers are re-queued as Pending on next boot.
func (m *Manager) ForceStop(_ context.Context) error {
	m.closeAllWorkers()
	return nil
}

// ServeHTTP implements http.Handler.
func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mux.ServeHTTP(w, r)
}

// ---- internal helpers ----

// DrainWorker sends TYPE_SYSTEM_CONTROL{drain} to the worker with the given ID,
// causing it to stop accepting new work and disconnect once its current tasks finish.
// Non-blocking — returns as soon as the packet is sent.
// Returns an error if the worker is not connected or the packet cannot be sent.
func (m *Manager) DrainWorker(workerID string) error {
	w, ok := m.workers.GetWorker(workerID)
	if !ok {
		return fmt.Errorf("worker %q not connected", workerID)
	}

	return w.Send(contract.ForulateSystemV0Message(contract.SystemCommandDrain))
}

// DrainWorkerAndWait sends TYPE_SYSTEM_CONTROL{drain} to the worker with the given ID
// and blocks until the worker has fully disconnected or ctx is cancelled.
// Returns an error if the worker is not connected, the packet cannot be sent, or ctx expires.
func (m *Manager) DrainWorkerAndWait(ctx context.Context, workerID string) error {
	if err := m.DrainWorker(workerID); err != nil {
		return err
	}

	return m.workers.waitForWorkerDisconnect(ctx, workerID)
}

// DisconnectWorker closes the connection to the worker with the given ID immediately.
// Any in-flight tasks are re-queued as Pending.
// Returns an error if the worker is not connected.
func (m *Manager) DisconnectWorker(workerID string) error {
	w, ok := m.workers.GetWorker(workerID)
	if !ok {
		return fmt.Errorf("worker %q not connected", workerID)
	}

	return w.conn.Close()
}

// drainWorkers sends TYPE_SYSTEM_CONTROL{drain} to all workers.
func (m *Manager) drainWorkers(_ context.Context) {
	message := contract.ForulateSystemV0Message(contract.SystemCommandDrain)

	for _, w := range m.WorkerPool().workers {
		if err := w.Send(message); err != nil {
			m.Logger().Warn("drain: send to worker", "worker_id", w.workerID, "error", err)

		}
	}
}

// closeAllWorkers closes all worker connections immediately.
func (m *Manager) closeAllWorkers() {
	for _, w := range m.workers.allWorkers() {
		w.conn.Close()
	}
}

func (m *Manager) OnJobsRequeued(jobIDs []string) {
	// Reset in-flight jobs to Pending and re-queue them.
	ctx := context.Background()
	now := time.Now()
	for _, id := range jobIDs {
		// Close any active run for this job.
		if m.cfg.RunStore != nil {
			runs, _ := m.cfg.RunStore.ListJobRuns(ctx, id)
			for _, r := range runs {
				if r.Status == contract.RunStatusProvisioning || r.Status == contract.RunStatusRunning {
					_ = m.cfg.RunStore.UpdateRun(ctx, r.ID, func(run *contract.JobRun) {
						run.Status = contract.RunStatusFailed
						run.FailureReason = "worker disconnected"
						run.FinishedAt = now
					})
				}
			}
		}

		job, err := m.JobStore().GetJob(ctx, id)
		if err != nil {
			m.Logger().Error("[Manager] Re-queue job not found", "job_id", id, "error", err)
			continue
		}

		err = m.JobStore().UpdateJob(ctx, id, func(j *contract.Job) {
			j.Status = contract.JobStatusPending
		})

		if err != nil {
			m.Logger().Error("[Manager] Failed to update job", "job", job.ID, "error", err)
		}

		job.Status = contract.JobStatusPending
		m.EnqueueJob(job)
	}
}

func (m *Manager) failJobDirect(ctx context.Context, job *contract.Job, reason string) {
	err := m.JobStore().UpdateJob(ctx, job.ID, func(j *contract.Job) {
		j.Status = contract.JobStatusFailed
		j.FailureReason = reason
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		m.cfg.Logger.Error("fail job: update", "job_id", job.ID, "error", err)
	}

	if m.WebhookDispatcher() != nil {
		m.WebhookDispatcher().FireJobFailed(ctx, job, reason)
	}
}

// cancelJobByID cancels a job and all its child jobs.
func (m *Manager) cancelJobByID(ctx context.Context, jobID string) {
	job, err := m.JobStore().GetJob(ctx, jobID)
	if err != nil {
		return
	}

	// Cancel any child jobs still in progress.
	children, _ := m.JobStore().ListChildJobs(ctx, jobID)
	for _, child := range children {
		if child.Status.IsTerminal() {
			continue
		}

		if child.Status == contract.JobStatusRunning || child.Status == contract.JobStatusProvisioning || child.Status == contract.JobStatusProposing {
			m.SendCancelToWorker(child.ID)
		}

		if child.Status == contract.JobStatusPending {
			m.cancelledJobIDs.Add(child.ID)
		}

		_ = m.JobStore().UpdateJob(ctx, child.ID, func(j *contract.Job) {
			cancelNow := time.Now()
			cancelDuration := cancelNow.Sub(j.CreatedAt)
			j.Status = contract.JobStatusCancelled
			j.UpdatedAt = cancelNow
			j.CompletedAt = &cancelNow
			j.Duration = &cancelDuration
		})
	}

	// Cancel the job itself.
	if job.Status == contract.JobStatusRunning || job.Status == contract.JobStatusProvisioning || job.Status == contract.JobStatusProposing || job.Status == contract.JobStatusAccepted {
		m.SendCancelToWorker(jobID)
	}

	if job.Status == contract.JobStatusPending {
		m.cancelledJobIDs.Add(job.ID)
	}

	err = m.JobStore().UpdateJob(ctx, jobID, func(j *contract.Job) {
		cancelNow := time.Now()
		cancelDuration := cancelNow.Sub(j.CreatedAt)
		j.Status = contract.JobStatusCancelled
		j.UpdatedAt = cancelNow
		j.CompletedAt = &cancelNow
		j.Duration = &cancelDuration
	})

	if err != nil {
		m.cfg.Logger.Error("cancel job: update", "job_id", jobID, "error", err)
	}

	if m.WebhookDispatcher() != nil {
		m.WebhookDispatcher().FireJobCancelled(ctx, job)
	}
}

// SendCancelToWorker sends TYPE_CANCEL_JOB to the worker currently handling the job.
func (m *Manager) SendCancelToWorker(jobID string) {
	target := m.workers.FindWorkerByInFlightJobID(jobID)
	if target == nil {
		return
	}

	if err := target.Send(fmt.Sprintf("cancel --job-id=%s", jobID)); err != nil {
		m.Logger().Error("[SendCancelToWorker] Failed to send cancel message to worker", "worker", target.workerID)
	}
}

// #####################################################################
//
// #####################################################################

// buildConsolidatePayload marshals the ConsolidatePayload that will be written
// to stdin when the parent binary is re-invoked in consolidate phase.
// Children are sorted by creation time to give a stable, deterministic order.
func buildConsolidatePayload(parent *contract.Job, children []*contract.Job) (contract.JsonOrBytes, error) {
	basePayload := parent.Payload
	var previous contract.ConsolidatePayload
	if len(parent.Payload) > 0 && json.Unmarshal(parent.Payload, &previous) == nil && previous.Payload != nil {
		// If parent payload already is a consolidate payload, keep carrying forward
		// the original payload to avoid nesting payload.payload.payload... across rounds.
		basePayload = previous.Payload
	}

	sorted := make([]*contract.Job, len(children))
	copy(sorted, children)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})

	childResults := make([]contract.ChildJobResult, len(sorted))
	for i, c := range sorted {
		childResults[i] = contract.ChildJobResult{
			JobID:    c.ID,
			TaskName: c.TaskName,
			Result:   c.Result,
		}
	}

	jsonbytes, err := json.Marshal(contract.ConsolidatePayload{
		Payload:  basePayload,
		Children: childResults,
	})

	if err != nil {
		return nil, err
	}

	return contract.JsonOrBytes(jsonbytes), nil
}

// queuePlatformEntry summarises pending jobs for one OS/arch pair.
type queuePlatformEntry struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	Depth     int    `json:"depth"`
	TotalCost int    `json:"total_cost"`
}

// queuePriorityEntry summarises pending jobs at one priority level.
type queuePriorityEntry struct {
	Priority int `json:"priority"`
	Depth    int `json:"depth"`
}

// queueSnapshot is the value returned by queueInfo.
type queueSnapshot struct {
	Depth      int
	ByPlatform []queuePlatformEntry
	ByPriority []queuePriorityEntry
}

// queueInfo returns a consistent snapshot of the in-memory dispatch heap.
// Task definitions are resolved without holding the dispatcher lock.
func (m *Manager) QueueInfo(ctx context.Context) queueSnapshot {
	type heapItem struct {
		taskName string
		priority int
		cost     int
	}

	items := make([]heapItem, 0, m.queue.Size())
	for job := range m.queue.Values() {
		items = append(items, heapItem{job.TaskName, job.Priority, job.Cost})
	}

	byPriority := make(map[int]int)
	byPlatform := make(map[string]*queuePlatformEntry)

	for _, item := range items {
		byPriority[item.priority]++

		if m.ArtifactRegistry() == nil {
			continue
		}

		platforms, err := m.ArtifactRegistry().ListPlatforms(ctx, item.taskName, "")
		if err != nil || len(platforms) == 0 {
			continue
		}

		// Count under the first available platform (represents the task's primary platform).
		p := platforms[0]
		key := platformKey(p.OS, p.Arch)
		if byPlatform[key] == nil {
			byPlatform[key] = &queuePlatformEntry{OS: p.OS, Arch: p.Arch}
		}

		byPlatform[key].Depth++
		byPlatform[key].TotalCost += item.cost
	}

	snap := queueSnapshot{Depth: len(items)}
	for _, e := range byPlatform {
		snap.ByPlatform = append(snap.ByPlatform, *e)
	}

	for p, count := range byPriority {
		snap.ByPriority = append(snap.ByPriority, queuePriorityEntry{Priority: p, Depth: count})
	}

	return snap
}

// #####################################################################
//              Utilities
// #####################################################################

func (m *Manager) WorkerPool() *WorkerPool {
	return m.workers
}

func (m *Manager) ArtifactRegistry() artifact.ArtifactRegistry {
	return m.cfg.ArtifactsRegistry
}

func (m *Manager) WebhookDispatcher() *webhooks.WebhookDispatcher {
	return m.webhooks
}

func (m *Manager) Logger() *slog.Logger {
	return m.cfg.Logger
}

func (m *Manager) JobStore() JobStore {
	return m.cfg.JobStore
}

func (m *Manager) TaskStore() TaskStore {
	return m.cfg.TaskStore
}

func (m *Manager) LogStore() LogStore {
	return m.cfg.LogStore
}

func (m *Manager) RunStore() RunStore {
	return m.cfg.RunStore
}
