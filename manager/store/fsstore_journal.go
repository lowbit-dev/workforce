package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/webhooks"
)

type journalOp string

const (
	opJob            journalOp = "job"
	opTaskDef        journalOp = "task_def"
	opTaskDefDelete  journalOp = "task_def_delete"
	opWebhookEnqueue journalOp = "webhook_enqueue"
	opWebhookAck     journalOp = "webhook_ack"
	opWebhookNack    journalOp = "webhook_nack"
	opRunCreate      journalOp = "run_create"
	opRunUpdate      journalOp = "run_update"
)

// journalEntry is one line in journal.jsonl.
// All fields that do not apply to a given op are left at their zero value.
type journalEntry struct {
	Op            journalOp       `json:"op"`
	Ts            time.Time       `json:"ts"`
	Data          json.RawMessage `json:"data,omitempty"`     // job, task, webhook entity
	ID            string          `json:"id,omitempty"`       // webhook_ack / webhook_nack
	Attempts      int             `json:"attempts,omitempty"` // webhook_nack
	NextAttemptAt time.Time       `json:"next_attempt_at"`    // webhook_nack (zero is fine for other ops)
}

func (fs *FSStore) journalPath() string {
	return filepath.Join(fs.cfg.Dir, "journal.jsonl")
}

// replayJournal reads journal.jsonl and applies every valid entry to in-memory state.
// A line that fails json.Valid is treated as a truncated final write and stops replay.
// Called at boot only — no concurrency concerns.
func (fs *FSStore) replayJournal() error {
	f, err := os.Open(fs.journalPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20) // 1 MB per line max
	for scanner.Scan() {
		line := scanner.Bytes()
		if !json.Valid(line) {
			// Truncated write at the end of a previous run — stop here.
			break
		}
		var e journalEntry
		if err := json.Unmarshal(line, &e); err != nil {
			break
		}
		if err := fs.applyJournalEntry(e); err != nil {
			return fmt.Errorf("replay op=%s: %w", e.Op, err)
		}
	}
	return scanner.Err()
}

// applyJournalEntry applies one entry to the in-memory maps.
// Called only from replayJournal (single-goroutine boot phase) — no locks needed.
func (fs *FSStore) applyJournalEntry(e journalEntry) error {
	switch e.Op {
	case opJob:
		var job contract.Job
		if err := json.Unmarshal(e.Data, &job); err != nil {
			return err
		}
		fs.jobs[job.ID] = &job

	case opTaskDef:
		var task contract.Task
		if err := json.Unmarshal(e.Data, &task); err != nil {
			return err
		}
		fs.taskDefs[task.Name] = &task

	case opTaskDefDelete:
		delete(fs.taskDefs, e.ID)

	case opWebhookEnqueue:
		var entry webhooks.WebhookEntry
		if err := json.Unmarshal(e.Data, &entry); err != nil {
			return err
		}
		fs.webhooks[entry.ID] = &entry

	case opWebhookAck:
		delete(fs.webhooks, e.ID)

	case opWebhookNack:
		if wh, ok := fs.webhooks[e.ID]; ok {
			wh.Attempts = e.Attempts
			wh.NextAttemptAt = e.NextAttemptAt
		}

	case opRunCreate, opRunUpdate:
		var run contract.JobRun
		if err := json.Unmarshal(e.Data, &run); err != nil {
			return err
		}
		fs.runs[run.ID] = &run
	}
	return nil
}

// runWriter is the single background goroutine that drains journalCh and writes
// entries to journal.jsonl. It fsyncs on the configured interval and triggers
// compaction when the entry count reaches CompactAfter.
func (fs *FSStore) runWriter() {
	defer fs.wg.Done()

	f, err := os.OpenFile(fs.journalPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		// The journal file was created/truncated during OpenFSStore, so this
		// should never fail. Panic with a descriptive message so the problem
		// is immediately visible rather than silently losing data.
		panic("fsstore: open journal for writing: " + err.Error())
	}

	w := bufio.NewWriterSize(f, 64*1024)
	enc := json.NewEncoder(w)

	var syncC <-chan time.Time
	if fs.cfg.SyncInterval > 0 {
		t := time.NewTicker(fs.cfg.SyncInterval)
		defer t.Stop()
		syncC = t.C
	}

	entryCount := 0

	flushSync := func() {
		_ = w.Flush()
		_ = f.Sync()
	}

	// compact triggers a snapshot write and reopens the journal file so the
	// goroutine's file position is correct after truncation.
	doCompact := func() {
		_ = w.Flush() // ensure all buffered data is on disk before snapshotting
		if err := fs.compact(); err != nil {
			return // non-fatal: journal grows until next attempt
		}
		f.Close()
		f, err = os.OpenFile(fs.journalPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			panic("fsstore: reopen journal after compact: " + err.Error())
		}
		w = bufio.NewWriterSize(f, 64*1024)
		enc = json.NewEncoder(w)
		entryCount = 0
	}

	writeEntry := func(e journalEntry) {
		_ = enc.Encode(e) // Encode appends a newline — one entry per line
		entryCount++
		if fs.cfg.SyncInterval == 0 {
			flushSync()
		}
		if entryCount >= fs.cfg.CompactAfter {
			doCompact()
		}
	}

	for {
		select {
		case <-fs.done:
			// Drain any entries queued before done was closed.
			for {
				select {
				case e := <-fs.journalCh:
					writeEntry(e)
				default:
					flushSync()
					f.Close()
					return
				}
			}
		case e := <-fs.journalCh:
			writeEntry(e)
		case <-syncC:
			flushSync()
		}
	}
}

// mustMarshal JSON-encodes v or panics. Used only for contract types that are
// always serialisable; a panic here indicates a programming error.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("fsstore: mustMarshal: " + err.Error())
	}
	return b
}
