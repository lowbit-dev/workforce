package manager

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/artifact"
	"lowbit.dev/workforce/manager/store"
	"lowbit.dev/workforce/manager/webhooks"
)

// ---- test doubles ----

// fakeArtifactRegistry is an in-memory ArtifactRegistry keyed by task name.
type fakeArtifactRegistry struct {
	mu        sync.Mutex
	platforms map[string][]artifact.ArtifactPlatform
}

var _ artifact.ArtifactRegistry = (*fakeArtifactRegistry)(nil)

func newFakeRegistry() *fakeArtifactRegistry {
	return &fakeArtifactRegistry{platforms: make(map[string][]artifact.ArtifactPlatform)}
}

func (f *fakeArtifactRegistry) setPlatforms(task string, platforms ...artifact.ArtifactPlatform) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.platforms[task] = platforms
}

func (f *fakeArtifactRegistry) ListPlatforms(_ context.Context, name, _ string) ([]artifact.ArtifactPlatform, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.platforms[name], nil
}

func (f *fakeArtifactRegistry) Resolve(_ context.Context, name, os, arch string) (artifact.ArtifactPlatform, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.platforms[name] {
		if p.OS == os && p.Arch == arch {
			return p, nil
		}
	}
	return artifact.ArtifactPlatform{}, artifact.ErrNotFound
}

func (f *fakeArtifactRegistry) ResolveVersion(ctx context.Context, name, _, os, arch string) (artifact.ArtifactPlatform, error) {
	return f.Resolve(ctx, name, os, arch)
}

func (f *fakeArtifactRegistry) Publish(context.Context, artifact.ArtifactVersion, map[string]io.Reader) error {
	return nil
}

func (f *fakeArtifactRegistry) ListVersions(context.Context, string) ([]artifact.ArtifactVersion, error) {
	return nil, nil
}

func (f *fakeArtifactRegistry) DeleteArtifact(context.Context, string) error { return nil }

// ---- helpers ----

func newTestManager(t *testing.T, reg artifact.ArtifactRegistry, mutateCfg func(*Config)) (*Manager, *store.MemStore) {
	t.Helper()

	ms := store.NewMemStore()
	cfg := Config{
		JobStore:          ms,
		TaskStore:         ms,
		LogStore:          ms,
		RunStore:          ms,
		WebhookStore:      ms,
		ArtifactsRegistry: reg,
		Webhook:           &webhooks.WebhookDispatcherConfig{},
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		NoWorkerAuth:      true,
		NoClientAuth:      true,
	}
	if mutateCfg != nil {
		mutateCfg(&cfg)
	}

	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New manager: %v", err)
	}

	return m, ms
}

// addTestWorker registers a worker over a net.Pipe and drains the worker side
// of the pipe so Send never blocks.
func addTestWorker(t *testing.T, m *Manager, id, os, arch string, capacity int) *WorkerConn {
	t.Helper()

	managerEnd, workerEnd := net.Pipe()
	t.Cleanup(func() {
		managerEnd.Close()
		workerEnd.Close()
	})
	go io.Copy(io.Discard, workerEnd)

	w := NewWorkerConn(id, os, arch, capacity, managerEnd)
	m.workers.register(w)
	return w
}

func newTestJob(id, task string, priority, cost int) *contract.Job {
	return &contract.Job{
		ID:        id,
		TaskName:  task,
		Status:    contract.JobStatusPending,
		Priority:  priority,
		Cost:      cost,
		CreatedAt: time.Now(),
	}
}

func saveTask(t *testing.T, ms *store.MemStore, name string) {
	t.Helper()
	if err := ms.SaveTask(context.Background(), &contract.Task{Name: name}); err != nil {
		t.Fatalf("save task %s: %v", name, err)
	}
}

func enqueueTestJob(t *testing.T, m *Manager, ms *store.MemStore, job *contract.Job) {
	t.Helper()
	if err := ms.SaveJob(context.Background(), job); err != nil {
		t.Fatalf("save job %s: %v", job.ID, err)
	}
	m.EnqueueJob(job)
}

func jobStatus(t *testing.T, ms *store.MemStore, id string) contract.JobStatus {
	t.Helper()
	job, err := ms.GetJob(context.Background(), id)
	if err != nil {
		t.Fatalf("get job %s: %v", id, err)
	}
	return job.Status
}

func linuxPlatform(task string) artifact.ArtifactPlatform {
	return artifact.ArtifactPlatform{Artifact: task, Version: "1.0.0", OS: "linux", Arch: "amd64", Hash: "abc", URL: "http://example/bin"}
}

func windowsPlatform(task string) artifact.ArtifactPlatform {
	return artifact.ArtifactPlatform{Artifact: task, Version: "1.0.0", OS: "windows", Arch: "amd64", Hash: "def", URL: "http://example/bin"}
}

// ---- tests ----

// A Windows-only job at the top of the queue must not block Linux jobs behind it
// when no Windows workers are connected.
func TestProcessQueue_SkipsJobWithoutOnlinePlatform(t *testing.T) {
	reg := newFakeRegistry()
	reg.setPlatforms("win-task", windowsPlatform("win-task"))
	reg.setPlatforms("linux-task", linuxPlatform("linux-task"))

	m, ms := newTestManager(t, reg, nil)
	saveTask(t, ms, "win-task")
	saveTask(t, ms, "linux-task")

	addTestWorker(t, m, "linux-1", "linux", "amd64", 10)

	// Windows job sits at the top of the queue (higher priority).
	enqueueTestJob(t, m, ms, newTestJob("job-win", "win-task", 100, 1))
	enqueueTestJob(t, m, ms, newTestJob("job-linux", "linux-task", 1, 1))

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-linux"); got != contract.JobStatusProposing {
		t.Fatalf("expected linux job to be proposing, got %s", got)
	}
	if got := jobStatus(t, ms, "job-win"); got != contract.JobStatusPending {
		t.Fatalf("expected windows job to stay pending, got %s", got)
	}
	if got := m.queue.Size(); got != 1 {
		t.Fatalf("expected 1 job left in queue, got %d", got)
	}

	// A Windows worker connects — the deferred job dispatches on the next pass.
	addTestWorker(t, m, "win-1", "windows", "amd64", 10)

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-win"); got != contract.JobStatusProposing {
		t.Fatalf("expected windows job to be proposing after worker connect, got %s", got)
	}
	if got := m.queue.Size(); got != 0 {
		t.Fatalf("expected empty queue, got %d", got)
	}
}

// Among jobs competing for the same capacity, the highest priority job wins;
// the lower priority job is deferred (not dropped) and dispatches once capacity frees up.
func TestProcessQueue_PriorityOrderAndBackfillAfterCapacityRestore(t *testing.T) {
	reg := newFakeRegistry()
	reg.setPlatforms("linux-task", linuxPlatform("linux-task"))

	m, ms := newTestManager(t, reg, nil)
	saveTask(t, ms, "linux-task")

	w := addTestWorker(t, m, "linux-1", "linux", "amd64", 1)

	enqueueTestJob(t, m, ms, newTestJob("job-low", "linux-task", 1, 1))
	enqueueTestJob(t, m, ms, newTestJob("job-high", "linux-task", 10, 1))

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-high"); got != contract.JobStatusProposing {
		t.Fatalf("expected high priority job to be proposing, got %s", got)
	}
	if got := jobStatus(t, ms, "job-low"); got != contract.JobStatusPending {
		t.Fatalf("expected low priority job to stay pending, got %s", got)
	}
	if got := m.queue.Size(); got != 1 {
		t.Fatalf("expected 1 job left in queue, got %d", got)
	}

	// Capacity frees up — the deferred job dispatches on the next pass.
	if err := m.workers.restoreCapacity(context.Background(), w, "job-high", 1); err != nil {
		t.Fatalf("restore capacity: %v", err)
	}

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-low"); got != contract.JobStatusProposing {
		t.Fatalf("expected low priority job to be proposing after capacity restore, got %s", got)
	}
}

// A job whose platform demand exceeds any worker's remaining capacity is deferred
// while smaller jobs behind it still dispatch, and the shortage hook fires.
func TestProcessQueue_BackfillsAroundTooCostlyJobAndFiresShortage(t *testing.T) {
	reg := newFakeRegistry()
	reg.setPlatforms("linux-task", linuxPlatform("linux-task"))

	var events []ResourceShortageEvent
	m, ms := newTestManager(t, reg, func(c *Config) {
		c.OnResourceShortage = func(e ResourceShortageEvent) { events = append(events, e) }
	})
	saveTask(t, ms, "linux-task")

	addTestWorker(t, m, "linux-1", "linux", "amd64", 5)

	enqueueTestJob(t, m, ms, newTestJob("job-big", "linux-task", 100, 10))
	enqueueTestJob(t, m, ms, newTestJob("job-small", "linux-task", 1, 1))

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-small"); got != contract.JobStatusProposing {
		t.Fatalf("expected small job to be proposing, got %s", got)
	}
	if got := jobStatus(t, ms, "job-big"); got != contract.JobStatusPending {
		t.Fatalf("expected big job to stay pending, got %s", got)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 shortage event, got %d", len(events))
	}
	e := events[0]
	if e.PendingCost != 10 {
		t.Fatalf("expected pending cost 10, got %d", e.PendingCost)
	}
	if len(e.UnsatisfiedPlatforms) != 1 || e.UnsatisfiedPlatforms[0].OS != "linux" || e.UnsatisfiedPlatforms[0].Arch != "amd64" {
		t.Fatalf("expected unsatisfied linux/amd64 platform, got %+v", e.UnsatisfiedPlatforms)
	}
	if e.UnsatisfiedPlatforms[0].PendingCost != 10 {
		t.Fatalf("expected unsatisfied pending cost 10, got %d", e.UnsatisfiedPlatforms[0].PendingCost)
	}
}

// A job referencing an unknown task is failed permanently instead of being
// re-queued forever.
func TestProcessQueue_UnknownTaskFailsJob(t *testing.T) {
	reg := newFakeRegistry()
	m, ms := newTestManager(t, reg, nil)

	addTestWorker(t, m, "linux-1", "linux", "amd64", 10)
	enqueueTestJob(t, m, ms, newTestJob("job-ghost", "ghost-task", 1, 1))

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-ghost"); got != contract.JobStatusFailed {
		t.Fatalf("expected ghost job to be failed, got %s", got)
	}
	if got := m.queue.Size(); got != 0 {
		t.Fatalf("expected empty queue, got %d", got)
	}

	job, err := ms.GetJob(context.Background(), "job-ghost")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.FailureReason == "" {
		t.Fatal("expected a failure reason to be recorded")
	}
}

// A job cancelled while sitting in the queue is silently discarded, never proposed.
func TestProcessQueue_CancelledJobIsDiscarded(t *testing.T) {
	reg := newFakeRegistry()
	reg.setPlatforms("linux-task", linuxPlatform("linux-task"))

	m, ms := newTestManager(t, reg, nil)
	saveTask(t, ms, "linux-task")

	w := addTestWorker(t, m, "linux-1", "linux", "amd64", 10)
	enqueueTestJob(t, m, ms, newTestJob("job-x", "linux-task", 1, 1))

	m.cancelledJobIDs.Add("job-x")

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := m.queue.Size(); got != 0 {
		t.Fatalf("expected empty queue, got %d", got)
	}
	if got := w.AvailableCapacity(); got != 10 {
		t.Fatalf("expected worker capacity untouched, got %d", got)
	}
	if got := jobStatus(t, ms, "job-x"); got != contract.JobStatusPending {
		t.Fatalf("expected cancelled job to remain pending in store (cancel handled elsewhere), got %s", got)
	}
}

// A job whose task has no published artifact platforms is deferred, and
// dispatches once a build for an online platform is published.
func TestProcessQueue_NoArtifactPlatformDefersUntilPublished(t *testing.T) {
	reg := newFakeRegistry()
	m, ms := newTestManager(t, reg, nil)
	saveTask(t, ms, "late-task")

	addTestWorker(t, m, "linux-1", "linux", "amd64", 10)
	enqueueTestJob(t, m, ms, newTestJob("job-late", "late-task", 1, 1))

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-late"); got != contract.JobStatusPending {
		t.Fatalf("expected job to stay pending without artifact platforms, got %s", got)
	}
	if got := m.queue.Size(); got != 1 {
		t.Fatalf("expected 1 job left in queue, got %d", got)
	}

	// Publish a linux build — the next pass dispatches the job.
	reg.setPlatforms("late-task", linuxPlatform("late-task"))

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-late"); got != contract.JobStatusProposing {
		t.Fatalf("expected job to be proposing after publish, got %s", got)
	}
}

// Starvation aging is applied once per pass: a starved low-priority job is
// boosted above a fresher high-priority job.
func TestProcessQueue_StarvationAgingBoostsOldJob(t *testing.T) {
	reg := newFakeRegistry()
	reg.setPlatforms("linux-task", linuxPlatform("linux-task"))

	m, ms := newTestManager(t, reg, func(c *Config) {
		c.StarvationTimeout = time.Minute
	})
	saveTask(t, ms, "linux-task")

	addTestWorker(t, m, "linux-1", "linux", "amd64", 1)

	oldJob := newTestJob("job-old", "linux-task", 0, 1)
	oldJob.CreatedAt = time.Now().Add(-2 * time.Minute)
	newJob := newTestJob("job-new", "linux-task", 5, 1)

	enqueueTestJob(t, m, ms, oldJob)
	enqueueTestJob(t, m, ms, newJob)

	if err := m.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("ProcessQueue: %v", err)
	}

	if got := jobStatus(t, ms, "job-old"); got != contract.JobStatusProposing {
		t.Fatalf("expected starved job to be proposing, got %s", got)
	}
	if got := jobStatus(t, ms, "job-new"); got != contract.JobStatusPending {
		t.Fatalf("expected fresh job to stay pending, got %s", got)
	}
}
