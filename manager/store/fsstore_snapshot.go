package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/webhooks"
)

type snapshot struct {
	Version   int                      `json:"version"`
	WrittenAt time.Time                `json:"written_at"`
	Jobs      []*contract.Job          `json:"jobs"`
	TaskDefs  []*contract.Task         `json:"task_defs"`
	Webhooks  []*webhooks.WebhookEntry `json:"webhooks"`
	Runs      []*contract.JobRun       `json:"runs"`
}

func (fs *FSStore) snapshotPath() string {
	return filepath.Join(fs.cfg.Dir, "snapshot.json")
}

// loadSnapshot reads snapshot.json into in-memory state.
// A missing file is not an error — it simply means a first boot from empty state.
// Called at boot only — no concurrency concerns.
func (fs *FSStore) loadSnapshot() error {
	f, err := os.Open(fs.snapshotPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	var snap snapshot
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	for _, j := range snap.Jobs {
		cp := *j
		fs.jobs[j.ID] = &cp
	}
	for _, t := range snap.TaskDefs {
		cp := *t
		fs.taskDefs[t.Name] = &cp
	}
	for _, e := range snap.Webhooks {
		cp := *e
		fs.webhooks[e.ID] = &cp
	}
	for _, r := range snap.Runs {
		cp := *r
		fs.runs[r.ID] = &cp
	}
	return nil
}

// compact serialises the current in-memory state to snapshot.json and truncates
// journal.jsonl. The snapshot is written atomically via a tmp-then-rename pattern;
// on POSIX, rename is atomic so the previous snapshot survives any mid-write crash.
//
// compact is called from two places:
//   - OpenFSStore (single-goroutine, before the background writer starts)
//   - runWriter (the background goroutine, after flushing the journal buffer)
//
// In both cases no other goroutine modifies the maps concurrently, so RLock is
// sufficient for the snapshot read.
func (fs *FSStore) compact() error {
	fs.mu.RLock()
	snap := snapshot{
		Version:   1,
		WrittenAt: time.Now(),
		Jobs:      make([]*contract.Job, 0, len(fs.jobs)),
		TaskDefs:  make([]*contract.Task, 0, len(fs.taskDefs)),
		Webhooks:  make([]*webhooks.WebhookEntry, 0, len(fs.webhooks)),
		Runs:      make([]*contract.JobRun, 0, len(fs.runs)),
	}
	for _, j := range fs.jobs {
		cp := *j
		snap.Jobs = append(snap.Jobs, &cp)
	}
	for _, t := range fs.taskDefs {
		cp := *t
		snap.TaskDefs = append(snap.TaskDefs, &cp)
	}
	for _, e := range fs.webhooks {
		cp := *e
		snap.Webhooks = append(snap.Webhooks, &cp)
	}
	for _, r := range fs.runs {
		cp := *r
		snap.Runs = append(snap.Runs, &cp)
	}
	fs.mu.RUnlock()

	tmpPath := fs.snapshotPath() + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("compact: create tmp snapshot: %w", err)
	}

	if err := json.NewEncoder(f).Encode(&snap); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("compact: encode snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("compact: sync snapshot: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, fs.snapshotPath()); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("compact: rename snapshot: %w", err)
	}

	// Truncate the journal. The background writer closes and reopens its file
	// handle after compact() returns, so the truncation does not corrupt its
	// write position.
	jf, err := os.OpenFile(fs.journalPath(), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("compact: truncate journal: %w", err)
	}
	jf.Close()
	return nil
}
