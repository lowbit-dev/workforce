package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"lowbit.dev/rungroup"
	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/artifact"
)

var (
	ErrUnknownTask        error = errors.New("unknown task")
	ErrUnknownJob         error = errors.New("unknown job")
	ErrNoEledgibleWorkers error = errors.New("no elegible workers")
	ErrNoArtifactPlatform error = errors.New("no artifact platform available")
	ErrProposalRejected   error = errors.New("proposal rejected by worker")
	ErrProposalTimedout   error = errors.New("proposal timedout")
)

func (m *Manager) DispatcherRoutine(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ctx.Err(), rungroup.ErrDoNotRestart)

		case <-m.dispatchSignal:
			m.drainDispatchSignals()

			if err := m.ProcessQueue(ctx); err != nil {
				return err
			}
		}
	}
}

// ProcessQueue runs a single dispatch pass over the queue. Jobs are popped in
// priority order; jobs that cannot be dispatched right now (no eligible worker
// for their platforms, missing artifact platform, transient errors) are deferred
// and pushed back at the end of the pass, so one blocked job never starves the
// jobs behind it (head-of-line blocking). Deferred jobs are retried on the next
// wake-up signal (worker connect, capacity restore, new job, artifact publish,
// or the periodic backstop).
func (m *Manager) ProcessQueue(ctx context.Context) error {
	m.lastDispatchRun.Store(uint64(time.Now().Unix()))

	if m.queue.Size() == 0 {
		return nil
	}

	m.Logger().Debug("[Dispatcher][ProcessQueue] Starting dispatch pass", "queued", m.queue.Size())

	// Boost starved jobs once per pass rather than per pop.
	m.applyStarvationAging()

	// Snapshot of platforms with at least one online worker, plus per-pass caches
	// for artifact platform keys and task existence (both stores are fs-backed).
	onlinePlatforms := m.workers.onlinePlatformKeys()
	platformCache := make(map[taskVersionKey][]string)
	taskCache := make(map[string]struct{})

	var deferred []*contract.Job
	dispatched, failed := 0, 0
	shortage := false

	for job := m.PopNextJobFromQueue(); job != nil; job = m.PopNextJobFromQueue() {
		platformKeys, d := m.jobPlatformKeysForDispatch(ctx, job, onlinePlatforms, platformCache, taskCache)
		if d != nil {
			if d.fail {
				m.Logger().Warn("[Dispatcher][ProcessQueue] Failing job — permanent dispatch error",
					"job_id", job.ID, "task", job.TaskName, "reason", d.reason)
				m.failJobDirect(ctx, job, d.reason)
				failed++
				continue
			}

			m.Logger().Debug("[Dispatcher][ProcessQueue] Deferring job",
				"job_id", job.ID, "task", job.TaskName, "reason", d.reason, "platforms", platformKeys)
			deferred = append(deferred, job)
			shortage = shortage || d.shortage
			continue
		}

		if err := m.tryProposeJob(ctx, job, platformKeys); err != nil {
			if errors.Is(err, ErrUnknownTask) {
				m.Logger().Warn("[Dispatcher][ProcessQueue] Failing job — unknown task",
					"job_id", job.ID, "task", job.TaskName)
				m.failJobDirect(ctx, job, fmt.Sprintf("unknown task: %s", job.TaskName))
				failed++
				continue
			}

			if errors.Is(err, ErrNoEledgibleWorkers) {
				shortage = true
			}

			m.Logger().Debug("[Dispatcher][ProcessQueue] Deferring job",
				"job_id", job.ID, "task", job.TaskName, "reason", err.Error(), "platforms", platformKeys)
			deferred = append(deferred, job)
			continue
		}

		dispatched++
	}

	// Re-queue deferred jobs so a later wake-up can retry them.
	for _, job := range deferred {
		m.queue.PushItem(job)
	}

	// Deferred jobs signal unmet demand — let the autoscaler hook decide.
	if shortage {
		m.checkResourceShortage(ctx)
	}

	summary := m.Logger().Debug
	if dispatched > 0 || failed > 0 {
		summary = m.Logger().Info
	}
	summary("[Dispatcher][ProcessQueue] Dispatch pass complete",
		"dispatched", dispatched, "deferred", len(deferred), "failed", failed, "queued", m.queue.Size())

	return nil
}

// taskVersionKey keys the per-pass artifact platform cache.
type taskVersionKey struct {
	task    string
	version string
}

// jobDeferral describes why a job cannot dispatch in this pass.
type jobDeferral struct {
	reason   string
	shortage bool // unmet worker demand — relevant for autoscaling
	fail     bool // permanent error — fail the job instead of re-queuing
}

// jobPlatformKeysForDispatch resolves the artifact platform keys a job can run on
// and decides whether the job is dispatchable in this pass. A nil deferral means
// the job may proceed to tryProposeJob. Platform and task lookups are cached per
// pass so a full queue scan stays cheap.
func (m *Manager) jobPlatformKeysForDispatch(ctx context.Context, job *contract.Job, online map[string]struct{}, platformCache map[taskVersionKey][]string, taskCache map[string]struct{}) ([]string, *jobDeferral) {
	// Unknown tasks can never dispatch — fail fast instead of re-queuing forever.
	if _, ok := taskCache[job.TaskName]; !ok {
		if _, err := m.cfg.TaskStore.GetTask(ctx, job.TaskName); err != nil {
			return nil, &jobDeferral{reason: fmt.Sprintf("unknown task: %s", job.TaskName), fail: true}
		}
		taskCache[job.TaskName] = struct{}{}
	}

	if m.ArtifactRegistry() == nil {
		return nil, nil // no platform constraints
	}

	key := taskVersionKey{task: job.TaskName, version: job.ArtifactVersion}
	keys, ok := platformCache[key]
	if !ok {
		platforms, err := m.cfg.ArtifactsRegistry.ListPlatforms(ctx, job.TaskName, job.ArtifactVersion)
		if err != nil {
			// Transient registry error — retry next pass.
			return nil, &jobDeferral{reason: fmt.Sprintf("list platforms: %s", err)}
		}

		keys = make([]string, 0, len(platforms))
		for _, p := range platforms {
			keys = append(keys, platformKey(p.OS, p.Arch))
		}
		platformCache[key] = keys
	}

	if len(keys) == 0 {
		return nil, &jobDeferral{reason: ErrNoArtifactPlatform.Error()}
	}

	// Cheap pre-filter: no online worker for any of the job's platforms.
	for _, k := range keys {
		if _, ok := online[k]; ok {
			return keys, nil
		}
	}

	return keys, &jobDeferral{reason: "no online workers for artifact platforms", shortage: true}
}

// applyStarvationAging boosts jobs pending longer than StarvationTimeout to
// max+1 priority. Called once per dispatch pass.
func (m *Manager) applyStarvationAging() {
	if m.cfg.StarvationTimeout <= 0 {
		return
	}

	maxP := 0
	for job := range m.queue.Values() {
		if job.Priority > maxP {
			maxP = job.Priority
		}
	}

	now := time.Now()
	dirty := false
	for job := range m.queue.Values() {
		if now.Sub(job.CreatedAt) > m.cfg.StarvationTimeout && job.Priority <= maxP {
			job.Priority = maxP + 1
			dirty = true
		}
	}

	if dirty {
		m.queue.Reheapify()
	}
}

// EnqueueJob adds a job to the in-memory dispatch heap and signals the loop.
func (m *Manager) EnqueueJob(job *contract.Job) {
	m.queue.PushItem(job)
	m.NotifyDispatcher()
}

// EnqueueJobs adds multiple jobs at once (used at boot for recovery).
func (m *Manager) EnqueueJobs(jobs []*contract.Job) {
	if len(jobs) == 0 {
		return
	}

	for _, j := range jobs {
		m.queue.PushItem(j)
	}

	m.NotifyDispatcher()
}

// PopNextJobFromQueue pops the highest-effective-priority job.
// Skips jobs that were cancelled while sitting in the heap.
func (m *Manager) PopNextJobFromQueue() *contract.Job {
	if m.queue.Size() < 1 {
		return nil
	}

	job, ok := m.queue.PopItem()
	if !ok {
		// Queue was empty, nothing to pop
		return nil
	}

	if m.cancelledJobIDs.Has(job.ID) {
		m.cancelledJobIDs.Remove(job.ID)
		return m.PopNextJobFromQueue() // discard silently from the queue and get the next
	}

	return job
}

// tryPropose selects a worker and sends TYPE_PROPOSE_JOB for the given job.
// platformKeys are the artifact platforms the job can run on, resolved (and
// cached) by the caller for this dispatch pass; empty means no platform
// constraint when no artifact registry is configured.
// On NACK the packet reader re-queues the job via EnqueueJob.
func (m *Manager) tryProposeJob(ctx context.Context, job *contract.Job, platformKeys []string) error {
	taskDef, err := m.cfg.TaskStore.GetTask(ctx, job.TaskName)
	if err != nil {
		m.Logger().Error("unknown task — discarding", "job_id", job.ID, "task_name", job.TaskName)

		return fmt.Errorf("%w: taks(%s)", ErrUnknownTask, job.TaskName)
	}

	// With a registry configured, an empty platform set means the artifact has
	// no builds for the requested version — the job can never dispatch.
	if m.ArtifactRegistry() != nil && len(platformKeys) == 0 {
		m.Logger().Warn("dispatcher: no artifact platforms for task/version", "job_id", job.ID, "task", job.TaskName, "version", job.ArtifactVersion)
		return ErrNoArtifactPlatform
	}

	workers := m.workers.eligibleWorkers(platformKeys, job.Cost, job.ID)
	if len(workers) == 0 {
		return ErrNoEledgibleWorkers
	}

	selected := m.cfg.WorkerSelector.Select(workers)

	// Resolve artifact info for the selected worker's OS/arch.
	var artInfo contract.ArtifactInfo
	if m.ArtifactRegistry() != nil {

		var platform artifact.ArtifactPlatform
		if job.ArtifactVersion != "" {
			platform, err = m.ArtifactRegistry().ResolveVersion(ctx, taskDef.Name, job.ArtifactVersion, selected.os, selected.arch)
		} else {
			platform, err = m.ArtifactRegistry().Resolve(ctx, taskDef.Name, selected.os, selected.arch)
		}

		// TODO: When the resolution failed because of the specific verion defined in the job, we should just stop
		// TODO: then let the client know its an unresolvable version. Retrying in a bit will most likely not solve this issue.

		if err != nil {
			m.Logger().Error("dispatcher: artifact resolution failed — re-queuing", "job_id", job.ID, "task", job.TaskName, "error", err)
			return err
		}

		artInfo = contract.ArtifactInfo{
			Hash:         platform.Hash,
			URL:          platform.URL,
			Dependencies: platform.Dependencies,
		}

		// Sign the download URL if a signing key is configured, so workers receive
		// a time-limited authenticated URL and the download route rejects unsigned requests.
		if m.urlSigner != nil && artInfo.URL != "" {
			if signed, err := m.urlSigner.Sign(artInfo.URL); err == nil {
				artInfo.URL = signed
			} else {
				m.Logger().Warn("dispatcher: failed to sign artifact URL", "url", artInfo.URL, "error", err)
			}
		}
	}

	if err := selected.Send(contract.FormulateProposeV0Message(job, taskDef, &artInfo)); err != nil {
		// TODO: handle the error
		m.Logger().Error("Failed to send job proposal to worker", "job", job.ID, "worker", selected.workerID, "error", err)

		return err
	}

	// In a bit of an easy toggle to switch this behaviour off if we need to
	if false {
		waitCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		response := selected.WaitForResponse(waitCtx, func(m contract.Message) bool {
			switch msg := m.(type) {
			case *contract.AcceptMessage:
				return job.ID == msg.JobID

			case *contract.RejectMessage:
				return job.ID == msg.JobID

			default:
				return false
			}
		})

		if response == nil {
			// the worker did not respond intime
			return ErrProposalTimedout
		}

		if rejectMsg, ok := response.(*contract.RejectMessage); ok {
			// Worker rejected the proposal
			return fmt.Errorf("%w: %s", ErrProposalRejected, rejectMsg.Reason)
		}

		// Here we asume the worker accepted the job
	}

	// Mark job as Proposing and track it as in-flight on the worker.
	err = m.JobStore().UpdateJob(ctx, job.ID, func(j *contract.Job) {
		j.Status = contract.JobStatusProposing
	})

	if err != nil {
		m.Logger().Error("dispatcher: update job to Proposing", "job_id", job.ID, "error", err)
		return err
	}

	// TODO: this should happen when the worker returns an accept message, not now
	// TODO: Or Should we reserve the cost in case it might accept, and then add it back if they reject?
	if err := m.workers.subtractCapacity(ctx, selected, job.ID, job.Cost); err != nil {
		// Freak incident. Error case here would be that the worker is either not known or already at capacity.
		// If this happens we are in serious trouble already, so corrupt data is the least of our problemns.
		m.Logger().Error("Failed to subtract from worker available capacity", "error", err)

		return err
	}

	m.Logger().Debug("[Dispatcher][tryProposeJob] Job proposal sent",
		"job_id", job.ID, "task", job.TaskName, "worker_id", selected.workerID,
		"worker_platform", platformKey(selected.os, selected.arch), "cost", job.Cost)

	// Fire job.proposing webhook.
	if m.WebhookDispatcher() != nil {
		m.WebhookDispatcher().FireJobProposing(ctx, job, selected.workerID)
	}

	return nil
}

func (m *Manager) acceptJobProposal(ctx context.Context, job *contract.Job, worker *WorkerConn) error {

	return nil
}

func (m *Manager) rejectJobProposal(ctx context.Context, job *contract.Job, worker *WorkerConn) error {

	return nil
}

// checkResourceShortage builds a ResourceShortageEvent and calls OnResourceShortage
// if pending cost exceeds cluster capacity and ScaleUpCooldown has elapsed since the last call.
func (m *Manager) checkResourceShortage(ctx context.Context) {
	if m.cfg.OnResourceShortage == nil {
		return
	}

	m.scaleMu.Lock()
	// Enforce ScaleUpCooldown.
	if !m.lastScaleUpCall.IsZero() && time.Since(m.lastScaleUpCall) < m.cfg.ScaleUpCooldown {
		m.scaleMu.Unlock()
		return
	}

	// Snapshot heap to compute aggregate demand per task name.
	// minCost tracks the cheapest pending job for each task name — used to determine
	// whether any worker can satisfy even the lowest-cost job of that type.
	type taskDemand struct {
		cost, count, minCost int
	}

	demandByTask := make(map[string]*taskDemand, m.queue.Size())
	pendingCost, pendingCount := 0, 0

	for job := range m.queue.Values() {
		pendingCost += job.Cost
		pendingCount++
		if demandByTask[job.TaskName] == nil {
			demandByTask[job.TaskName] = &taskDemand{minCost: job.Cost}
		}

		demandByTask[job.TaskName].cost += job.Cost
		demandByTask[job.TaskName].count++
		if job.Cost < demandByTask[job.TaskName].minCost {
			demandByTask[job.TaskName].minCost = job.Cost
		}
	}

	m.scaleMu.Unlock()

	clusterCapacity := m.workers.totalCapacity()
	if pendingCost <= clusterCapacity {
		return
	}

	// Build UnsatisfiedPlatforms: resolve each task's artifact platforms and check whether
	// any connected worker on those platforms has remaining capacity.
	platformDemand := make(map[string]*PlatformDemand)
	for taskName, demand := range demandByTask {
		var platformKeys []string
		var platformsByKey map[string]artifact.ArtifactPlatform

		if m.ArtifactRegistry() != nil {
			platforms, err := m.ArtifactRegistry().ListPlatforms(ctx, taskName, "")
			if err == nil {
				platformsByKey = make(map[string]artifact.ArtifactPlatform, len(platforms))
				for _, p := range platforms {
					key := platformKey(p.OS, p.Arch)
					platformKeys = append(platformKeys, key)
					platformsByKey[key] = p
				}
			}
		}

		// Use minCost so that workers with some available capacity but not enough to
		// satisfy even the cheapest pending job are correctly flagged as unsatisfied.
		if len(m.workers.eligibleWorkers(platformKeys, demand.minCost, "")) == 0 {
			for key, p := range platformsByKey {
				if _, ok := platformDemand[key]; !ok {
					platformDemand[key] = &PlatformDemand{OS: p.OS, Arch: p.Arch}
				}
				platformDemand[key].PendingCost += demand.cost
				platformDemand[key].PendingCount += demand.count
			}
		}
	}

	unsatisfied := make([]PlatformDemand, 0, len(platformDemand))
	for _, pd := range platformDemand {
		unsatisfied = append(unsatisfied, *pd)
	}

	m.scaleMu.Lock()
	m.lastScaleUpCall = time.Now()
	m.scaleMu.Unlock()

	m.cfg.OnResourceShortage(ResourceShortageEvent{
		PendingCost:          pendingCost,
		ClusterCapacity:      clusterCapacity,
		PendingCount:         pendingCount,
		ConnectedWorkers:     m.WorkerPool().Size(),
		UnsatisfiedPlatforms: unsatisfied,
	})
}

// NotifyDispatcheer sends a non-blocking wake-up signal to the dispatch loop.
func (m *Manager) NotifyDispatcher() {
	select {
	case m.dispatchSignal <- struct{}{}:
	default:
	}
}

func (m *Manager) drainDispatchSignals() {
	for {
		select {
		case <-m.dispatchSignal:
			// keep draining

		default:
			return
		}
	}
}
