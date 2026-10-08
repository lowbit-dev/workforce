package manager

import (
	"context"
	"encoding/json"
	"testing"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/store"
	"lowbit.dev/workforce/manager/webhooks"
)

func queuedWebhooks(t *testing.T, ms *store.MemStore) []*webhooks.WebhookEntry {
	t.Helper()
	entries, err := ms.DequeueWebhooks(context.Background())
	if err != nil {
		t.Fatalf("dequeue webhooks: %v", err)
	}
	return entries
}

func TestHandleJobCompleted_WebhookCarriesResult(t *testing.T) {
	ctx := context.Background()
	m, ms := newTestManager(t, newFakeRegistry(), nil)
	w := addTestWorker(t, m, "linux-1", "linux", "amd64", 10)

	job := newTestJob("job-1", "linux-task", 1, 1)
	job.Status = contract.JobStatusRunning
	job.WebhookURL = "http://example/hook"
	if err := ms.SaveJob(ctx, job); err != nil {
		t.Fatalf("save job: %v", err)
	}

	s := NewWorkerConnServer(m)
	s.handleJobCompleted(ctx, m.Logger(), w, job, &contract.ResultMessage{
		JobID:   "job-1",
		Type:    contract.ResultSuccess,
		Payload: contract.JsonOrBytes(`{"Documents":["d:bank_mutations"]}`),
	})

	if got := jobStatus(t, ms, "job-1"); got != contract.JobStatusCompleted {
		t.Fatalf("expected job to be completed, got %s", got)
	}

	entries := queuedWebhooks(t, ms)
	if len(entries) != 1 || entries[0].Event != "job.completed" {
		t.Fatalf("expected one job.completed webhook, got %+v", entries)
	}

	var envelope struct {
		Data struct {
			Result struct {
				Documents []string
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(entries[0].Payload, &envelope); err != nil {
		t.Fatalf("decode webhook payload: %v", err)
	}
	if docs := envelope.Data.Result.Documents; len(docs) != 1 || docs[0] != "d:bank_mutations" {
		t.Fatalf("expected the job result in the webhook, got payload %s", entries[0].Payload)
	}
}

func TestHandleFailJob_ChildFailureFiresParentWebhook(t *testing.T) {
	ctx := context.Background()
	m, ms := newTestManager(t, newFakeRegistry(), nil)

	parent := newTestJob("parent-1", "linux-task", 1, 1)
	parent.Status = contract.JobStatusAwaitingChildren
	parent.WebhookURL = "http://example/hook"

	child := newTestJob("child-1", "linux-task", 1, 1)
	child.ParentJobID = "parent-1"
	child.Status = contract.JobStatusRunning

	for _, j := range []*contract.Job{parent, child} {
		if err := ms.SaveJob(ctx, j); err != nil {
			t.Fatalf("save job %s: %v", j.ID, err)
		}
	}

	s := NewWorkerConnServer(m)
	s.handleFailJob(ctx, m.Logger(), child, &contract.ResultMessage{
		JobID:  "child-1",
		Type:   contract.ResultError,
		Reason: "boom",
	})

	if got := jobStatus(t, ms, "parent-1"); got != contract.JobStatusFailed {
		t.Fatalf("expected parent to be failed, got %s", got)
	}

	entries := queuedWebhooks(t, ms)
	if len(entries) != 1 || entries[0].JobID != "parent-1" || entries[0].Event != "job.failed" {
		t.Fatalf("expected one job.failed webhook for the parent, got %+v", entries)
	}
}
