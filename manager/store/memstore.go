package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/webhooks"
)

// MemStore is an in-memory implementation of JobStore, TaskStore, LogStore, and WebhookStore.
// It provides best-effort delivery — state is lost on process exit.
// Safe for concurrent use from multiple goroutines.
type MemStore struct {
	mu sync.RWMutex

	jobs     map[string]*contract.Job
	taskDefs map[string]*contract.Task // task definitions, keyed by Task.Name

	// logMu guards log buffers and subscribers independently from the main mu
	// to avoid holding the write lock during potentially slow subscriber sends.
	logMu       sync.Mutex
	logs        map[string]*bytes.Buffer // jobID → accumulated log
	subscribers map[string][]chan []byte // jobID → broadcast channels

	webhooks map[string]*webhooks.WebhookEntry
}

// NewMemStore allocates and returns a ready-to-use MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		jobs:        make(map[string]*contract.Job),
		taskDefs:    make(map[string]*contract.Task),
		logs:        make(map[string]*bytes.Buffer),
		subscribers: make(map[string][]chan []byte),
		webhooks:    make(map[string]*webhooks.WebhookEntry),
	}
}

// ---- JobStore ----

func (m *MemStore) SaveJob(_ context.Context, job *contract.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *job
	m.jobs[job.ID] = &cp
	return nil
}

func (m *MemStore) GetJob(_ context.Context, id string) (*contract.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %q not found", id)
	}
	cp := *job
	return &cp, nil
}

func (m *MemStore) UpdateJob(_ context.Context, id string, mutate func(*contract.Job)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	mutate(job)
	return nil
}

// ListJobs returns a page of root jobs (ParentJobID == "") ordered by CreatedAt ascending.
// cursor is the ID of the last job seen; empty string starts from the beginning.
func (m *MemStore) ListJobs(_ context.Context, cursor string, limit int) ([]*contract.Job, string, error) {
	m.mu.RLock()
	all := make([]*contract.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if j.ParentJobID != "" {
			continue // skip child jobs
		}
		cp := *j
		all = append(all, &cp)
	}
	m.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool {
		return all[i].CreatedAt.Before(all[j].CreatedAt)
	})

	start := 0
	if cursor != "" {
		for i, j := range all {
			if j.ID == cursor {
				start = i + 1
				break
			}
		}
	}

	if start >= len(all) {
		return nil, "", nil
	}

	end := start + limit
	if end > len(all) {
		end = len(all)
	}

	page := all[start:end]
	nextCursor := ""
	if end < len(all) {
		nextCursor = page[len(page)-1].ID
	}
	return page, nextCursor, nil
}

func (m *MemStore) ListPendingJobs(_ context.Context) ([]*contract.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*contract.Job
	for _, j := range m.jobs {
		if j.Status == contract.JobStatusPending {
			cp := *j
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Priority > out[j].Priority
	})
	return out, nil
}

func (m *MemStore) ListRecoverableJobs(_ context.Context) ([]*contract.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*contract.Job
	for _, j := range m.jobs {
		switch j.Status {
		case contract.JobStatusProposing, contract.JobStatusProvisioning, contract.JobStatusRunning:
			cp := *j
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (m *MemStore) ListChildJobs(_ context.Context, parentJobID string) ([]*contract.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*contract.Job
	for _, j := range m.jobs {
		if j.ParentJobID == parentJobID {
			cp := *j
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (m *MemStore) CountJobsByStatus(_ context.Context) (map[contract.JobStatus]int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	counts := make(map[contract.JobStatus]int)
	for _, j := range m.jobs {
		counts[j.Status]++
	}
	return counts, nil
}

// ---- TaskStore ----

func (m *MemStore) SaveTask(_ context.Context, task *contract.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *task
	m.taskDefs[task.Name] = &cp
	return nil
}

func (m *MemStore) GetTask(_ context.Context, name string) (*contract.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.taskDefs[name]
	if !ok {
		return nil, fmt.Errorf("task %q not found", name)
	}
	cp := *task
	return &cp, nil
}

func (m *MemStore) ListTasks(_ context.Context) ([]*contract.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*contract.Task, 0, len(m.taskDefs))
	for _, t := range m.taskDefs {
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (m *MemStore) UpdateTask(_ context.Context, name string, mutate func(*contract.Task)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.taskDefs[name]
	if !ok {
		return fmt.Errorf("task %q not found", name)
	}
	mutate(task)
	return nil
}

func (m *MemStore) DeleteTask(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.taskDefs[name]; !ok {
		return fmt.Errorf("task %q not found", name)
	}
	delete(m.taskDefs, name)
	return nil
}

// ---- LogStore ----

func (m *MemStore) AppendJobLog(_ context.Context, jobID string, data []byte) error {
	m.logMu.Lock()
	buf, ok := m.logs[jobID]
	if !ok {
		buf = &bytes.Buffer{}
		m.logs[jobID] = buf
	}
	buf.Write(data)
	subs := m.subscribers[jobID]
	m.logMu.Unlock()

	// Broadcast to subscribers outside the lock to avoid blocking appenders.
	chunk := make([]byte, len(data))
	copy(chunk, data)
	for _, ch := range subs {
		select {
		case ch <- chunk:
		default:
			// Slow subscriber; drop chunk to avoid blocking the writer.
		}
	}
	return nil
}

func (m *MemStore) GetJobLogReader(_ context.Context, jobID string) (io.ReadCloser, error) {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	buf, ok := m.logs[jobID]
	if !ok {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	snapshot := make([]byte, buf.Len())
	copy(snapshot, buf.Bytes())
	return io.NopCloser(bytes.NewReader(snapshot)), nil
}

func (m *MemStore) GetRootJobLogReader(ctx context.Context, rootJobID string) (io.ReadCloser, error) {
	// Collect all jobs in the tree: root + all descendants ordered by CreatedAt.
	children, err := m.ListChildJobs(ctx, rootJobID)
	if err != nil {
		return nil, err
	}

	// Root job log first, then child logs in creation order.
	jobIDs := []string{rootJobID}
	for _, c := range children {
		jobIDs = append(jobIDs, c.ID)
	}

	var combined bytes.Buffer
	m.logMu.Lock()
	for _, id := range jobIDs {
		if buf, ok := m.logs[id]; ok {
			combined.Write(buf.Bytes())
		}
	}
	m.logMu.Unlock()

	snapshot := make([]byte, combined.Len())
	copy(snapshot, combined.Bytes())
	return io.NopCloser(bytes.NewReader(snapshot)), nil
}

func (m *MemStore) SubscribeJobLogs(ctx context.Context, jobID string) (<-chan []byte, error) {
	ch := make(chan []byte, 64)

	m.logMu.Lock()
	// Drain any existing buffered log first.
	if buf, ok := m.logs[jobID]; ok && buf.Len() > 0 {
		snapshot := make([]byte, buf.Len())
		copy(snapshot, buf.Bytes())
		ch <- snapshot
	}
	m.subscribers[jobID] = append(m.subscribers[jobID], ch)
	m.logMu.Unlock()

	go func() {
		<-ctx.Done()
		m.logMu.Lock()
		subs := m.subscribers[jobID]
		for i, sub := range subs {
			if sub == ch {
				m.subscribers[jobID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		m.logMu.Unlock()
		close(ch)
	}()

	return ch, nil
}

// CloseJobLogSubscribers closes all subscriber channels for a job.
// Call this when a job reaches a terminal state so SSE handlers exit cleanly.
func (m *MemStore) CloseJobLogSubscribers(jobID string) {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	for _, ch := range m.subscribers[jobID] {
		close(ch)
	}
	delete(m.subscribers, jobID)
}

// ---- WebhookStore ----

func (m *MemStore) EnqueueWebhook(_ context.Context, entry *webhooks.WebhookEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *entry
	m.webhooks[entry.ID] = &cp
	return nil
}

func (m *MemStore) DequeueWebhooks(_ context.Context) ([]*webhooks.WebhookEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	var out []*webhooks.WebhookEntry
	for _, e := range m.webhooks {
		if !e.NextAttemptAt.After(now) {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (m *MemStore) AckWebhook(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.webhooks, id)
	return nil
}

func (m *MemStore) NackWebhook(_ context.Context, id string, attempts int, nextAttemptAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.webhooks[id]
	if !ok {
		return fmt.Errorf("webhook entry %q not found", id)
	}
	entry.Attempts = attempts
	entry.NextAttemptAt = nextAttemptAt
	return nil
}
