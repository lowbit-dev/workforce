package storetest

import (
	"context"
	"os"
	"testing"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager"
	"lowbit.dev/workforce/manager/store"
	"lowbit.dev/workforce/manager/webhooks"
)

func openFSStore(t *testing.T) (*store.FSStore, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "fsstore-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	// SyncInterval=0 ensures every write is fsynced immediately, making the
	// persistence test deterministic without artificial sleeps.
	fs, err := store.OpenFSStore(store.FSStoreConfig{Dir: dir, SyncInterval: 0})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("OpenFSStore: %v", err)
	}
	t.Cleanup(func() {
		fs.Close()
		os.RemoveAll(dir)
	})
	return fs, dir
}

func TestFSStoreJobStore(t *testing.T) {
	RunJobStoreTests(t, func() manager.JobStore {
		fs, _ := openFSStore(t)
		return fs
	})
}

func TestFSStoreTaskStore(t *testing.T) {
	RunTaskStoreTests(t, func() manager.TaskStore {
		fs, _ := openFSStore(t)
		return fs
	})
}

func TestFSStoreLogStore(t *testing.T) {
	RunLogStoreTests(t, func() logRunStore {
		fs, _ := openFSStore(t)
		return fs
	})
}

func TestFSStoreWebhookStore(t *testing.T) {
	RunWebhookStoreTests(t, func() webhooks.WebhookStore {
		fs, _ := openFSStore(t)
		return fs
	})
}

// TestFSStorePersistence verifies that job, task definition, and webhook state
// survives a close-and-reopen cycle (simulating a clean process restart).
func TestFSStorePersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "fsstore-persist-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	now := time.Now().Truncate(time.Second) // truncate for JSON round-trip fidelity

	// --- Write data ---
	fs1, err := store.OpenFSStore(store.FSStoreConfig{Dir: dir, SyncInterval: 0})
	if err != nil {
		t.Fatalf("open1: %v", err)
	}

	job := &contract.Job{
		ID:        "persist-job",
		TaskName:  "echo",
		Status:    contract.JobStatusRunning,
		Phase:     "run",
		CreatedAt: now,
		UpdatedAt: now,
	}
	taskDef := &contract.Task{
		Name: "echo",
		Cost: 5,
	}
	webhook := &webhooks.WebhookEntry{
		ID:            "persist-wh",
		JobID:         "persist-job",
		URL:           "https://example.com/hook",
		Payload:       []byte(`{"event":"job.completed"}`),
		NextAttemptAt: now.Add(-time.Second), // due immediately
		CreatedAt:     now,
	}

	if err := fs1.SaveJob(ctx, job); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	if err := fs1.SaveTask(ctx, taskDef); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}
	if err := fs1.EnqueueWebhook(ctx, webhook); err != nil {
		t.Fatalf("EnqueueWebhook: %v", err)
	}

	// Update the job to simulate a mid-flight mutation.
	if err := fs1.UpdateJob(ctx, "persist-job", func(j *contract.Job) {
		j.Status = contract.JobStatusCompleted
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	if err := fs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// --- Reopen and verify ---
	fs2, err := store.OpenFSStore(store.FSStoreConfig{Dir: dir, SyncInterval: 0})
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer fs2.Close()

	gotJob, err := fs2.GetJob(ctx, "persist-job")
	if err != nil {
		t.Fatalf("GetJob after reopen: %v", err)
	}
	if gotJob.Status != contract.JobStatusCompleted {
		t.Errorf("job status: got %q, want %q", gotJob.Status, contract.JobStatusCompleted)
	}

	gotTask, err := fs2.GetTask(ctx, "echo")
	if err != nil {
		t.Fatalf("GetTask after reopen: %v", err)
	}
	if gotTask.Cost != 5 {
		t.Errorf("task cost: got %d, want 5", gotTask.Cost)
	}

	due, err := fs2.DequeueWebhooks(ctx)
	if err != nil {
		t.Fatalf("DequeueWebhooks after reopen: %v", err)
	}
	if len(due) != 1 || due[0].ID != "persist-wh" {
		t.Errorf("webhooks: got %d entries, want 1 with id persist-wh", len(due))
	}
}

// TestFSStoreJournalReplay verifies that mutations written only to the journal
// (not yet compacted into a snapshot) are correctly replayed on reopen.
func TestFSStoreJournalReplay(t *testing.T) {
	dir, err := os.MkdirTemp("", "fsstore-replay-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Use a high CompactAfter so compaction never fires during the test, ensuring
	// everything goes through the journal path.
	fs1, err := store.OpenFSStore(store.FSStoreConfig{
		Dir:          dir,
		SyncInterval: 0,
		CompactAfter: 10000,
	})
	if err != nil {
		t.Fatalf("open1: %v", err)
	}

	now := time.Now()
	for i := range 5 {
		j := &contract.Job{
			ID:        "replay-job-" + string(rune('a'+i)),
			TaskName:  "echo",
			Status:    contract.JobStatusPending,
			Phase:     "run",
			CreatedAt: now.Add(time.Duration(i) * time.Millisecond),
			UpdatedAt: now,
		}
		if err := fs1.SaveJob(ctx, j); err != nil {
			t.Fatalf("SaveJob %d: %v", i, err)
		}
	}
	if err := fs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fs2, err := store.OpenFSStore(store.FSStoreConfig{Dir: dir, SyncInterval: 0})
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer fs2.Close()

	jobs, _, err := fs2.ListJobs(ctx, "", 100)
	if err != nil {
		t.Fatalf("ListJobs after replay: %v", err)
	}
	if len(jobs) != 5 {
		t.Errorf("job count after replay: got %d, want 5", len(jobs))
	}
}
