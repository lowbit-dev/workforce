package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/webhooks"
)

const (
	defaultSyncInterval   = 200 * time.Millisecond
	defaultCompactAfter   = 500
	defaultJournalBufSize = 256
)

// FSStoreConfig controls FSStore behaviour.
type FSStoreConfig struct {
	// Dir is the directory where all store files are written.
	// Created on OpenFSStore if it does not exist.
	Dir string

	// SyncInterval is how often the background writer fsyncs the journal.
	// Higher values reduce write latency at the cost of a larger durability window.
	// Zero means fsync after every entry (maximum durability).
	// Default: 200ms.
	SyncInterval time.Duration

	// CompactAfter is the number of journal entries written before a compaction
	// is triggered. Compaction writes a full snapshot and truncates the journal,
	// bounding replay time on the next boot.
	// Default: 500.
	CompactAfter int

	// JournalBufSize is the capacity of the channel between callers and the
	// background writer goroutine. Default: 256.
	JournalBufSize int
}

func (c *FSStoreConfig) setDefaults() {
	if c.SyncInterval < 0 {
		c.SyncInterval = 0
	}
	if c.CompactAfter <= 0 {
		c.CompactAfter = defaultCompactAfter
	}
	if c.JournalBufSize <= 0 {
		c.JournalBufSize = defaultJournalBufSize
	}
}

// FSStore implements JobStore, TaskStore, LogStore, and WebhookStore with filesystem persistence.
//
// Reads are served from an in-memory hot state; read latency is identical to MemStore.
// Writes update memory immediately and are journaled to disk asynchronously by a single
// background goroutine that fsyncs at most once per SyncInterval.
//
// The store survives process restarts. Within the SyncInterval durability window it
// survives unclean shutdowns — at most SyncInterval of mutations may be lost.
//
// Call Close after all store operations have stopped. It is a programmer error to call
// any store method concurrently with or after Close.
type FSStore struct {
	cfg FSStoreConfig

	// In-memory hot state — identical structure to MemStore.
	mu       sync.RWMutex
	jobs     map[string]*contract.Job
	taskDefs map[string]*contract.Task // task definitions keyed by Task.Name
	webhooks map[string]*webhooks.WebhookEntry

	// Log state: per-job append handles and broadcast channels.
	logMu       sync.Mutex
	logHandles  map[string]*os.File // jobID → open O_APPEND handle
	logBufs     map[string][]byte   // jobID → accumulated bytes (for SubscribeJobLogs)
	subscribers map[string][]chan []byte

	// Journal channel: callers send entries; background writer drains.
	journalCh chan journalEntry
	done      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// OpenFSStore opens (or creates) an FSStore rooted at cfg.Dir.
//
// On first open it starts from an empty state. On subsequent opens it loads
// the latest snapshot, replays the journal, writes a fresh compact snapshot,
// and starts the background writer goroutine.
func OpenFSStore(cfg FSStoreConfig) (*FSStore, error) {
	cfg.setDefaults()

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("fsstore: mkdir %s: %w", cfg.Dir, err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, "logs"), 0o755); err != nil {
		return nil, fmt.Errorf("fsstore: mkdir logs: %w", err)
	}

	fs := &FSStore{
		cfg:         cfg,
		jobs:        make(map[string]*contract.Job),
		taskDefs:    make(map[string]*contract.Task),
		webhooks:    make(map[string]*webhooks.WebhookEntry),
		logHandles:  make(map[string]*os.File),
		logBufs:     make(map[string][]byte),
		subscribers: make(map[string][]chan []byte),
		journalCh:   make(chan journalEntry, cfg.JournalBufSize),
		done:        make(chan struct{}),
	}

	if err := fs.loadSnapshot(); err != nil {
		return nil, fmt.Errorf("fsstore: load snapshot: %w", err)
	}
	if err := fs.replayJournal(); err != nil {
		return nil, fmt.Errorf("fsstore: replay journal: %w", err)
	}
	// Compact immediately: turns the replayed state into a clean snapshot and
	// starts with an empty journal, bounding replay time on the next open.
	if err := fs.compact(); err != nil {
		return nil, fmt.Errorf("fsstore: initial compact: %w", err)
	}

	fs.wg.Add(1)
	go fs.runWriter()

	return fs, nil
}

// Close flushes and fsyncs the journal, then closes all open file handles.
// Must be called after all store operations have completed.
func (fs *FSStore) Close() error {
	fs.closeOnce.Do(func() {
		close(fs.done)
		fs.wg.Wait()

		fs.logMu.Lock()
		for _, f := range fs.logHandles {
			f.Close()
		}
		fs.logMu.Unlock()
	})
	return nil
}

// ---- JobStore ----

func (fs *FSStore) SaveJob(_ context.Context, job *contract.Job) error {
	fs.mu.Lock()
	cp := *job
	fs.jobs[job.ID] = &cp
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opJob, Data: mustMarshal(job)})
	return nil
}

func (fs *FSStore) GetJob(_ context.Context, id string) (*contract.Job, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	job, ok := fs.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %q not found", id)
	}
	cp := *job
	return &cp, nil
}

func (fs *FSStore) UpdateJob(_ context.Context, id string, mutate func(*contract.Job)) error {
	fs.mu.Lock()
	job, ok := fs.jobs[id]
	if !ok {
		fs.mu.Unlock()
		return fmt.Errorf("job %q not found", id)
	}
	mutate(job)
	cp := *job
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opJob, Data: mustMarshal(&cp)})
	return nil
}

func (fs *FSStore) ListJobs(_ context.Context, cursor string, limit int) ([]*contract.Job, string, error) {
	fs.mu.RLock()
	all := make([]*contract.Job, 0, len(fs.jobs))
	for _, j := range fs.jobs {
		if j.ParentJobID != "" {
			continue // skip child jobs
		}
		cp := *j
		all = append(all, &cp)
	}
	fs.mu.RUnlock()

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

func (fs *FSStore) ListPendingJobs(_ context.Context) ([]*contract.Job, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	var out []*contract.Job
	for _, j := range fs.jobs {
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

func (fs *FSStore) ListRecoverableJobs(_ context.Context) ([]*contract.Job, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	var out []*contract.Job
	for _, j := range fs.jobs {
		switch j.Status {
		case contract.JobStatusProposing, contract.JobStatusProvisioning, contract.JobStatusRunning:
			cp := *j
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (fs *FSStore) ListChildJobs(_ context.Context, parentJobID string) ([]*contract.Job, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	var out []*contract.Job
	for _, j := range fs.jobs {
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

func (fs *FSStore) CountJobsByStatus(_ context.Context) (map[contract.JobStatus]int, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	counts := make(map[contract.JobStatus]int)
	for _, j := range fs.jobs {
		counts[j.Status]++
	}
	return counts, nil
}

// ---- TaskStore ----

func (fs *FSStore) SaveTask(_ context.Context, task *contract.Task) error {
	fs.mu.Lock()
	cp := *task
	fs.taskDefs[task.Name] = &cp
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opTaskDef, Data: mustMarshal(task)})
	return nil
}

func (fs *FSStore) GetTask(_ context.Context, name string) (*contract.Task, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	task, ok := fs.taskDefs[name]
	if !ok {
		return nil, fmt.Errorf("task %q not found", name)
	}
	cp := *task
	return &cp, nil
}

func (fs *FSStore) ListTasks(_ context.Context) ([]*contract.Task, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	out := make([]*contract.Task, 0, len(fs.taskDefs))
	for _, t := range fs.taskDefs {
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (fs *FSStore) UpdateTask(_ context.Context, name string, mutate func(*contract.Task)) error {
	fs.mu.Lock()
	task, ok := fs.taskDefs[name]
	if !ok {
		fs.mu.Unlock()
		return fmt.Errorf("task %q not found", name)
	}
	mutate(task)
	cp := *task
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opTaskDef, Data: mustMarshal(&cp)})
	return nil
}

func (fs *FSStore) DeleteTask(_ context.Context, name string) error {
	fs.mu.Lock()
	if _, ok := fs.taskDefs[name]; !ok {
		fs.mu.Unlock()
		return fmt.Errorf("task %q not found", name)
	}
	delete(fs.taskDefs, name)
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opTaskDefDelete, ID: name})
	return nil
}

// ---- LogStore ----

func (fs *FSStore) AppendJobLog(_ context.Context, jobID string, data []byte) error {
	fs.logMu.Lock()
	f, err := fs.getOrOpenLogHandle(jobID)
	if err != nil {
		fs.logMu.Unlock()
		return fmt.Errorf("fsstore: open log handle for %s: %w", jobID, err)
	}

	data = append(data, byte('\n'))

	_, writeErr := f.Write(data)
	if writeErr == nil {
		fs.logBufs[jobID] = append(fs.logBufs[jobID], data...)
	}
	subs := fs.subscribers[jobID]
	fs.logMu.Unlock()

	if writeErr != nil {
		return fmt.Errorf("fsstore: write log for %s: %w", jobID, writeErr)
	}

	// Broadcast to live subscribers outside the lock.
	chunk := make([]byte, len(data))
	copy(chunk, data)
	for _, ch := range subs {
		select {
		case ch <- chunk:
		default:
			// Slow subscriber; drop chunk rather than blocking the writer.
		}
	}
	return nil
}

func (fs *FSStore) GetJobLogReader(_ context.Context, jobID string) (io.ReadCloser, error) {
	logPath := filepath.Join(fs.cfg.Dir, "logs", jobID+".log")
	f, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		return nil, fmt.Errorf("fsstore: open log %s: %w", jobID, err)
	}
	return f, nil
}

func (fs *FSStore) GetRootJobLogReader(ctx context.Context, rootJobID string) (io.ReadCloser, error) {
	children, err := fs.ListChildJobs(ctx, rootJobID)
	if err != nil {
		return nil, err
	}
	jobIDs := []string{rootJobID}
	for _, c := range children {
		jobIDs = append(jobIDs, c.ID)
	}
	readers := make([]io.Reader, 0, len(jobIDs))
	closers := make([]io.Closer, 0, len(jobIDs))
	for _, id := range jobIDs {
		r, err := fs.GetJobLogReader(ctx, id)
		if err != nil {
			for _, c := range closers {
				c.Close()
			}
			return nil, err
		}
		readers = append(readers, r)
		closers = append(closers, r)
	}
	return &multiReadCloser{Reader: io.MultiReader(readers...), closers: closers}, nil
}

func (fs *FSStore) SubscribeJobLogs(ctx context.Context, jobID string) (<-chan []byte, error) {
	ch := make(chan []byte, 64)

	fs.logMu.Lock()
	// Pre-fill with any buffered log data accumulated since the job started.
	if buf, ok := fs.logBufs[jobID]; ok && len(buf) > 0 {
		snapshot := make([]byte, len(buf))
		copy(snapshot, buf)
		ch <- snapshot
	}
	fs.subscribers[jobID] = append(fs.subscribers[jobID], ch)
	fs.logMu.Unlock()

	go func() {
		<-ctx.Done()
		fs.logMu.Lock()
		subs := fs.subscribers[jobID]
		for i, sub := range subs {
			if sub == ch {
				fs.subscribers[jobID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		fs.logMu.Unlock()
		close(ch)
	}()

	return ch, nil
}

// CloseJobLogSubscribers closes all subscriber channels for a job.
// Call this when a job reaches a terminal state so SSE handlers exit cleanly.
func (fs *FSStore) CloseJobLogSubscribers(jobID string) {
	fs.logMu.Lock()
	defer fs.logMu.Unlock()
	for _, ch := range fs.subscribers[jobID] {
		close(ch)
	}
	delete(fs.subscribers, jobID)
}

// ---- WebhookStore ----

func (fs *FSStore) EnqueueWebhook(_ context.Context, entry *webhooks.WebhookEntry) error {
	fs.mu.Lock()
	cp := *entry
	fs.webhooks[entry.ID] = &cp
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opWebhookEnqueue, Data: mustMarshal(entry)})
	return nil
}

func (fs *FSStore) DequeueWebhooks(_ context.Context) ([]*webhooks.WebhookEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	now := time.Now()
	var out []*webhooks.WebhookEntry
	for _, e := range fs.webhooks {
		if !e.NextAttemptAt.After(now) {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (fs *FSStore) AckWebhook(_ context.Context, id string) error {
	fs.mu.Lock()
	delete(fs.webhooks, id)
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opWebhookAck, ID: id})
	return nil
}

func (fs *FSStore) NackWebhook(_ context.Context, id string, attempts int, nextAttemptAt time.Time) error {
	fs.mu.Lock()
	entry, ok := fs.webhooks[id]
	if !ok {
		fs.mu.Unlock()
		return fmt.Errorf("webhook entry %q not found", id)
	}
	entry.Attempts = attempts
	entry.NextAttemptAt = nextAttemptAt
	fs.mu.Unlock()
	fs.journal(journalEntry{Op: opWebhookNack, ID: id, Attempts: attempts, NextAttemptAt: nextAttemptAt})
	return nil
}

// ---- internal helpers ----

// journal enqueues an entry for the background writer. Non-blocking: if done is
// closed (store is shutting down), the entry is silently dropped.
func (fs *FSStore) journal(e journalEntry) {
	e.Ts = time.Now()
	select {
	case fs.journalCh <- e:
	case <-fs.done:
	}
}

// getOrOpenLogHandle returns the open *os.File for the given job's log, opening
// it in append mode on first access. Must be called with fs.logMu held.
func (fs *FSStore) getOrOpenLogHandle(jobID string) (*os.File, error) {
	if f, ok := fs.logHandles[jobID]; ok {
		return f, nil
	}
	logPath := filepath.Join(fs.cfg.Dir, "logs", jobID+".log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	fs.logHandles[jobID] = f
	return f, nil
}

// multiReadCloser wraps io.MultiReader and closes all underlying readers on Close.
type multiReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (m *multiReadCloser) Close() error {
	var first error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
