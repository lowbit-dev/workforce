package manager

import (
	"encoding/json"
	"testing"
	"time"

	"lowbit.dev/workforce/contract"
)

func TestBuildConsolidatePayload_UsesOriginalPayloadFromPreviousConsolidate(t *testing.T) {
	originalPayload := contract.JsonOrBytes([]byte(`{"request":"x"}`))
	previousPayload := contract.ConsolidatePayload{
		Payload: originalPayload,
		Children: []contract.ChildJobResult{
			{JobID: "old-1", TaskName: "child", Result: contract.JsonOrBytes([]byte(`{"ok":true}`))},
		},
	}

	prevJSON, err := json.Marshal(previousPayload)
	if err != nil {
		t.Fatalf("marshal previous payload: %v", err)
	}

	parent := &contract.Job{Payload: contract.JsonOrBytes(prevJSON)}
	now := time.Now()
	children := []*contract.Job{
		{ID: "new-1", TaskName: "child", Result: contract.JsonOrBytes([]byte(`{"ok":true}`)), CreatedAt: now},
	}

	outBytes, err := buildConsolidatePayload(parent, children)
	if err != nil {
		t.Fatalf("buildConsolidatePayload: %v", err)
	}

	var out contract.ConsolidatePayload
	if err := json.Unmarshal(outBytes, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}

	if string(out.Payload) != string(originalPayload) {
		t.Fatalf("expected original payload %s, got %s", string(originalPayload), string(out.Payload))
	}

	if len(out.Children) != 1 {
		t.Fatalf("expected 1 child result, got %d", len(out.Children))
	}
}

func TestBuildConsolidatePayload_SortsChildrenByCreatedAt(t *testing.T) {
	parent := &contract.Job{Payload: contract.JsonOrBytes([]byte(`{"request":"x"}`))}
	now := time.Now()
	children := []*contract.Job{
		{ID: "late", TaskName: "child", Result: contract.JsonOrBytes([]byte(`{"n":2}`)), CreatedAt: now.Add(2 * time.Second)},
		{ID: "early", TaskName: "child", Result: contract.JsonOrBytes([]byte(`{"n":1}`)), CreatedAt: now},
	}

	outBytes, err := buildConsolidatePayload(parent, children)
	if err != nil {
		t.Fatalf("buildConsolidatePayload: %v", err)
	}

	var out contract.ConsolidatePayload
	if err := json.Unmarshal(outBytes, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}

	if len(out.Children) != 2 {
		t.Fatalf("expected 2 child results, got %d", len(out.Children))
	}

	if out.Children[0].JobID != "early" || out.Children[1].JobID != "late" {
		t.Fatalf("unexpected order: %q then %q", out.Children[0].JobID, out.Children[1].JobID)
	}
}
