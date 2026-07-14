// Package storetest provides a reusable compliance suite for testing implementations
// of the contract.JobStore, contract.TaskStore, contract.LogStore, and contract.WebhookStore interfaces.
//
// Usage:
//
//	func TestMyStore(t *testing.T) {
//	    storetest.RunJobStoreTests(t, func() contract.JobStore { return mystore.New() })
//	    storetest.RunTaskStoreTests(t, func() contract.TaskStore { return mystore.New() })
//	    storetest.RunLogStoreTests(t, func() contract.LogStore { return mystore.New() })
//	    storetest.RunWebhookStoreTests(t, func() contract.WebhookStore { return mystore.New() })
//	}
package storetest

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager"
	"lowbit.dev/workforce/manager/webhooks"
)

// RunJobStoreTests runs compliance tests against a JobStore factory.
func RunJobStoreTests(t *testing.T, factory func() manager.JobStore) {
	t.Helper()

	t.Run("SaveAndGetJob", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		job := newTestJob("job-1")
		if err := s.SaveJob(ctx, job); err != nil {
			t.Fatalf("SaveJob: %v", err)
		}
		got, err := s.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if got.ID != job.ID {
			t.Errorf("ID: got %q, want %q", got.ID, job.ID)
		}
		if got.TaskName != job.TaskName {
			t.Errorf("TaskName: got %q, want %q", got.TaskName, job.TaskName)
		}
	})

	t.Run("GetJobNotFound", func(t *testing.T) {
		t.Helper()
		s := factory()
		_, err := s.GetJob(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for nonexistent job, got nil")
		}
	})

	t.Run("UpdateJob", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		job := newTestJob("job-2")
		_ = s.SaveJob(ctx, job)

		if err := s.UpdateJob(ctx, job.ID, func(j *contract.Job) {
			j.Status = contract.JobStatusRunning
		}); err != nil {
			t.Fatalf("UpdateJob: %v", err)
		}

		got, _ := s.GetJob(ctx, job.ID)
		if got.Status != contract.JobStatusRunning {
			t.Errorf("Status: got %q, want %q", got.Status, contract.JobStatusRunning)
		}
	})

	t.Run("ListJobs_Pagination", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		for i := range 5 {
			j := newTestJob("pagejob-" + string(rune('a'+i)))
			j.CreatedAt = time.Now().Add(time.Duration(i) * time.Millisecond)
			_ = s.SaveJob(ctx, j)
		}

		page1, cursor, err := s.ListJobs(ctx, "", 3)
		if err != nil {
			t.Fatalf("ListJobs page1: %v", err)
		}
		if len(page1) != 3 {
			t.Fatalf("page1 len: got %d, want 3", len(page1))
		}
		if cursor == "" {
			t.Fatal("expected non-empty cursor after first page")
		}

		page2, cursor2, err := s.ListJobs(ctx, cursor, 3)
		if err != nil {
			t.Fatalf("ListJobs page2: %v", err)
		}
		if len(page2) != 2 {
			t.Fatalf("page2 len: got %d, want 2", len(page2))
		}
		if cursor2 != "" {
			t.Fatalf("expected empty cursor at last page, got %q", cursor2)
		}
	})

	t.Run("ListPendingJobs", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		pending := newTestJob("pending-1")
		pending.Status = contract.JobStatusPending
		running := newTestJob("running-1")
		running.Status = contract.JobStatusRunning

		_ = s.SaveJob(ctx, pending)
		_ = s.SaveJob(ctx, running)

		jobs, err := s.ListPendingJobs(ctx)
		if err != nil {
			t.Fatalf("ListPendingJobs: %v", err)
		}
		if len(jobs) != 1 || jobs[0].ID != "pending-1" {
			t.Errorf("expected 1 pending job, got %d", len(jobs))
		}
	})

	t.Run("ListRecoverableJobs", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		for _, status := range []contract.JobStatus{
			contract.JobStatusProposing,
			contract.JobStatusProvisioning,
			contract.JobStatusRunning,
		} {
			j := newTestJob("recoverable-" + string(status))
			j.Status = status
			_ = s.SaveJob(ctx, j)
		}
		pending := newTestJob("should-not-appear")
		pending.Status = contract.JobStatusPending
		_ = s.SaveJob(ctx, pending)

		jobs, err := s.ListRecoverableJobs(ctx)
		if err != nil {
			t.Fatalf("ListRecoverableJobs: %v", err)
		}
		if len(jobs) != 3 {
			t.Errorf("expected 3 recoverable jobs, got %d", len(jobs))
		}
	})

	t.Run("ListChildJobs", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		parent := newTestJob("parent-1")
		_ = s.SaveJob(ctx, parent)

		child1 := newTestJob("child-1")
		child1.ParentJobID = "parent-1"
		child2 := newTestJob("child-2")
		child2.ParentJobID = "parent-1"
		unrelated := newTestJob("unrelated-1")

		_ = s.SaveJob(ctx, child1)
		_ = s.SaveJob(ctx, child2)
		_ = s.SaveJob(ctx, unrelated)

		children, err := s.ListChildJobs(ctx, "parent-1")
		if err != nil {
			t.Fatalf("ListChildJobs: %v", err)
		}
		if len(children) != 2 {
			t.Errorf("expected 2 children, got %d", len(children))
		}
	})
}

// RunTaskStoreTests runs compliance tests against a TaskStore factory.
func RunTaskStoreTests(t *testing.T, factory func() manager.TaskStore) {
	t.Helper()

	t.Run("SaveAndGetTask", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		task := newTestTaskDef("echo")
		if err := s.SaveTask(ctx, task); err != nil {
			t.Fatalf("SaveTask: %v", err)
		}
		got, err := s.GetTask(ctx, task.Name)
		if err != nil {
			t.Fatalf("GetTask: %v", err)
		}
		if got.Name != task.Name {
			t.Errorf("Name: got %q, want %q", got.Name, task.Name)
		}
	})

	t.Run("ListTasks", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		_ = s.SaveTask(ctx, newTestTaskDef("task-a"))
		_ = s.SaveTask(ctx, newTestTaskDef("task-b"))

		tasks, err := s.ListTasks(ctx)
		if err != nil {
			t.Fatalf("ListTasks: %v", err)
		}
		if len(tasks) != 2 {
			t.Errorf("expected 2 tasks, got %d", len(tasks))
		}
	})

	t.Run("UpdateTask", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		task := newTestTaskDef("task-upd")
		_ = s.SaveTask(ctx, task)

		if err := s.UpdateTask(ctx, task.Name, func(tk *contract.Task) {
			tk.Cost = 5
		}); err != nil {
			t.Fatalf("UpdateTask: %v", err)
		}

		got, _ := s.GetTask(ctx, task.Name)
		if got.Cost != 5 {
			t.Errorf("cost: got %d, want 5", got.Cost)
		}
	})

	t.Run("DeleteTask", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		task := newTestTaskDef("task-del")
		_ = s.SaveTask(ctx, task)
		_ = s.DeleteTask(ctx, task.Name)

		_, err := s.GetTask(ctx, task.Name)
		if err == nil {
			t.Fatal("expected error after delete, got nil")
		}
	})
}

// RunLogStoreTests runs compliance tests against a LogStore factory.
func RunLogStoreTests(t *testing.T, factory func() manager.LogStore) {
	t.Helper()

	t.Run("AppendAndRead", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		_ = s.AppendJobLog(ctx, "job-log-1", []byte("hello "))
		_ = s.AppendJobLog(ctx, "job-log-1", []byte("world"))

		r, err := s.GetJobLogReader(ctx, "job-log-1")
		if err != nil {
			t.Fatalf("GetJobLogReader: %v", err)
		}
		defer r.Close()
		got, _ := io.ReadAll(r)
		if !bytes.Equal(got, []byte("hello world")) {
			t.Errorf("log content: got %q, want %q", got, "hello world")
		}
	})

	t.Run("EmptyLog", func(t *testing.T) {
		t.Helper()
		s := factory()
		r, err := s.GetJobLogReader(context.Background(), "nonexistent-job")
		if err != nil {
			t.Fatalf("GetJobLogReader for nonexistent job: %v", err)
		}
		defer r.Close()
		got, _ := io.ReadAll(r)
		if len(got) != 0 {
			t.Errorf("expected empty reader, got %d bytes", len(got))
		}
	})

	t.Run("Subscribe", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		ch, err := s.SubscribeJobLogs(ctx, "job-sub-1")
		if err != nil {
			t.Fatalf("SubscribeJobLogs: %v", err)
		}

		_ = s.AppendJobLog(ctx, "job-sub-1", []byte("chunk"))

		select {
		case data, ok := <-ch:
			if !ok {
				t.Fatal("channel closed unexpectedly")
			}
			if !bytes.Contains(data, []byte("chunk")) {
				t.Errorf("expected 'chunk' in received data, got %q", data)
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for log chunk")
		}
	})
}

// RunWebhookStoreTests runs compliance tests against a WebhookStore factory.
func RunWebhookStoreTests(t *testing.T, factory func() webhooks.WebhookStore) {
	t.Helper()

	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		entry := &webhooks.WebhookEntry{
			ID:            "wh-1",
			JobID:         "job-1",
			URL:           "https://example.com/hook",
			Payload:       []byte(`{}`),
			NextAttemptAt: time.Now().Add(-time.Second), // already due
		}
		if err := s.EnqueueWebhook(ctx, entry); err != nil {
			t.Fatalf("EnqueueWebhook: %v", err)
		}

		entries, err := s.DequeueWebhooks(ctx)
		if err != nil {
			t.Fatalf("DequeueWebhooks: %v", err)
		}
		if len(entries) != 1 || entries[0].ID != "wh-1" {
			t.Fatalf("DequeueWebhooks: expected 1 entry, got %d", len(entries))
		}
	})

	t.Run("FutureEntryNotDequeued", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		entry := &webhooks.WebhookEntry{
			ID:            "wh-future",
			NextAttemptAt: time.Now().Add(time.Hour),
		}
		_ = s.EnqueueWebhook(ctx, entry)

		entries, err := s.DequeueWebhooks(ctx)
		if err != nil {
			t.Fatalf("DequeueWebhooks: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("expected 0 entries, got %d", len(entries))
		}
	})

	t.Run("AckRemovesEntry", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		entry := &webhooks.WebhookEntry{ID: "wh-ack", NextAttemptAt: time.Now().Add(-time.Second)}
		_ = s.EnqueueWebhook(ctx, entry)
		_ = s.AckWebhook(ctx, "wh-ack")

		entries, _ := s.DequeueWebhooks(ctx)
		if len(entries) != 0 {
			t.Fatalf("expected 0 after ack, got %d", len(entries))
		}
	})

	t.Run("NackUpdatesRetry", func(t *testing.T) {
		t.Helper()
		s := factory()
		ctx := context.Background()

		entry := &webhooks.WebhookEntry{ID: "wh-nack", NextAttemptAt: time.Now().Add(-time.Second)}
		_ = s.EnqueueWebhook(ctx, entry)

		nextTime := time.Now().Add(5 * time.Second)
		if err := s.NackWebhook(ctx, "wh-nack", 2, nextTime); err != nil {
			t.Fatalf("NackWebhook: %v", err)
		}

		// Should not be dequeued now (next attempt is in future).
		entries, _ := s.DequeueWebhooks(ctx)
		if len(entries) != 0 {
			t.Fatalf("expected 0 after nack, got %d", len(entries))
		}
	})
}

// ---- helpers ----

func newTestJob(id string) *contract.Job {
	return &contract.Job{
		ID:        id,
		TaskName:  "test-task",
		Status:    contract.JobStatusPending,
		Phase:     "run",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func newTestTaskDef(name string) *contract.Task {
	return &contract.Task{
		Name: name,
	}
}
