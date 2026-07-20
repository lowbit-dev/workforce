package manager

import (
	"container/heap"
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

// ProcessQueue pops and dispatches jobs until either the heap is empty or
// no eligible worker can be found.
func (m *Manager) ProcessQueue(ctx context.Context) error {
	m.lastDispatchRun.Store(uint64(time.Now().Unix()))

	for job := m.PopNextJobFromQueue(); job != nil; job = m.PopNextJobFromQueue() {

		// TODO: Make TryPropose return an error,
		// TODO: switch on error sentinal if we need to re-queue it or mark it failed

		if err := m.tryProposeJob(ctx, job); err != nil {
			m.Logger().Warn("Failed to propose job", "job", job.ID, "task", job.TaskName, "error", err)

			// TODO: based on the type of error, we should either requeue or mark as failed with a good reason

			m.queue.PushItem(job)
			m.checkResourceShortage(ctx)

			// TODO: Should this return or keep on trying to dispatch jobs?
			return nil
		}
	}

	return nil
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

// popNext pops the highest-effective-priority job, applying starvation aging when configured.
// Skips jobs that were cancelled while sitting in the heap.
func (m *Manager) PopNextJobFromQueue() *contract.Job {
	if m.queue.Size() < 1 {
		return nil
	}

	// Apply priority aging: boost starved jobs to max+1.
	if m.cfg.StarvationTimeout > 0 {
		maxP := 0
		for job := range m.queue.Values() {
			if job.Priority > maxP {
				maxP = job.Priority
			}
		}

		now := time.Now()
		heapDirty := false

		for job := range m.queue.Values() {
			if now.Sub(job.CreatedAt) > m.cfg.StarvationTimeout && job.Priority <= maxP {
				job.Priority = maxP + 1
				heapDirty = true
			}
		}

		if heapDirty {
			heap.Init(m.queue)
		}
	}

	job, ok := m.queue.PopItem()
	if !ok {
		// Queue was empty, noting to pop
		return nil
	}

	if m.cancelledJobIDs.Has(job.ID) {
		m.cancelledJobIDs.Remove(job.ID)
		return m.PopNextJobFromQueue() // discard silently from the queue and get the next
	}

	return job
}

// tryPropose selects a worker and sends TYPE_PROPOSE_JOB for the given job.
// Returns true if the proposal was sent successfully (job is now Proposing).
// On NACK the packet reader calls requeueJob; on no eligible workers returns false.
func (m *Manager) tryProposeJob(ctx context.Context, job *contract.Job) error {
	taskDef, err := m.cfg.TaskStore.GetTask(ctx, job.TaskName)
	if err != nil {
		m.Logger().Error("unknown task — discarding", "job_id", job.ID, "task_name", job.TaskName)

		return fmt.Errorf("%w: taks(%s)", ErrUnknownTask, job.TaskName)
	}

	platformKeys := []string{}

	// Determine eligible workers from the artifact's available platforms.
	if m.ArtifactRegistry() != nil {
		platforms, err := m.cfg.ArtifactsRegistry.ListPlatforms(ctx, job.TaskName, job.ArtifactVersion)
		if err != nil {
			m.Logger().Error("dispatcher: list platforms failed — re-queuing", "job_id", job.ID, "task", job.TaskName, "error", err)

			return err
		}

		if len(platforms) == 0 {
			m.Logger().Warn("dispatcher: no artifact platforms for task/version", "job_id", job.ID, "task", job.TaskName, "version", job.ArtifactVersion)
			return ErrNoArtifactPlatform
		}

		for _, p := range platforms {
			platformKeys = append(platformKeys, platformKey(p.OS, p.Arch))
		}
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

	if err := selected.Send(contract.FormulateProposeV0Message(job, &artInfo)); err != nil {
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
