package taskkit

import (
	"encoding/json"
	"os"
	"testing"
)

// ---- ReadInput ----

// TestReadInput_ValidJSON verifies that ReadInput correctly decodes JSON from stdin.
// NOTE: replaces os.Stdin temporarily; do not run in parallel.
func TestReadInput_ValidJSON(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
		r.Close()
	}()

	go func() {
		_, _ = w.Write([]byte(`{"name":"alice","score":42}`))
		w.Close()
	}()

	var result map[string]any
	if err := ReadInput(&result); err != nil {
		t.Fatalf("ReadInput: unexpected error: %v", err)
	}
	if result["name"] != "alice" {
		t.Errorf("name: expected 'alice', got %v", result["name"])
	}
	if result["score"] != float64(42) {
		t.Errorf("score: expected 42, got %v", result["score"])
	}
}

// TestReadInput_InvalidJSON verifies that ReadInput returns an error for malformed input.
// NOTE: replaces os.Stdin temporarily; do not run in parallel.
func TestReadInput_InvalidJSON(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
		r.Close()
	}()

	go func() {
		_, _ = w.Write([]byte(`not valid json`))
		w.Close()
	}()

	var result any
	if err := ReadInput(&result); err == nil {
		t.Fatal("expected error for invalid JSON input")
	}
}

// TestMustReadInput_PanicsOnInvalidJSON verifies that MustReadInput panics when
// stdin contains invalid JSON.
// NOTE: replaces os.Stdin temporarily; do not run in parallel.
func TestMustReadInput_PanicsOnInvalidJSON(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
		r.Close()
	}()

	go func() {
		_, _ = w.Write([]byte(`{bad`))
		w.Close()
	}()

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("expected MustReadInput to panic on invalid JSON")
		}
	}()

	var result any
	MustReadInput(&result)
}

// ---- CurrentPhase ----

func TestCurrentPhase_DefaultsToRun(t *testing.T) {
	t.Setenv(EnvPhase, "")
	if got := CurrentPhase(); got != PhaseRun {
		t.Fatalf("expected PhaseRun when %s is unset, got %s", EnvPhase, got)
	}
}

func TestCurrentPhase_ConsolidateWhenEnvSet(t *testing.T) {
	t.Setenv(EnvPhase, string(PhaseConsolidate))
	if got := CurrentPhase(); got != PhaseConsolidate {
		t.Fatalf("expected PhaseConsolidate, got %s", got)
	}
}

func TestCurrentPhase_UnknownValueFallsBackToRun(t *testing.T) {
	t.Setenv(EnvPhase, "unknown-phase")
	if got := CurrentPhase(); got != PhaseRun {
		t.Fatalf("expected PhaseRun for unknown phase value, got %s", got)
	}
}

// ---- GetEnv ----

func TestGetEnv_ReadsAllWorkerInjectedVariables(t *testing.T) {
	t.Setenv(EnvJobID, "job-abc")
	t.Setenv(EnvTaskType, "my-task")
	t.Setenv(EnvParentJobID, "parent-xyz")
	t.Setenv(EnvAttempt, "3")

	env := GetEnv()

	if env.JobID != "job-abc" {
		t.Errorf("JobID: expected 'job-abc', got %q", env.JobID)
	}
	if env.TaskType != "my-task" {
		t.Errorf("TaskType: expected 'my-task', got %q", env.TaskType)
	}
	if env.ParentJobID != "parent-xyz" {
		t.Errorf("ParentJobID: expected 'parent-xyz', got %q", env.ParentJobID)
	}
	if env.Attempt != 3 {
		t.Errorf("Attempt: expected 3, got %d", env.Attempt)
	}
}

func TestGetEnv_ZeroValuesOutsideWorker(t *testing.T) {
	t.Setenv(EnvJobID, "")
	t.Setenv(EnvTaskType, "")
	t.Setenv(EnvParentJobID, "")
	t.Setenv(EnvAttempt, "")

	env := GetEnv()

	if env.JobID != "" {
		t.Errorf("JobID: expected empty, got %q", env.JobID)
	}
	if env.TaskType != "" {
		t.Errorf("TaskType: expected empty, got %q", env.TaskType)
	}
	if env.ParentJobID != "" {
		t.Errorf("ParentJobID: expected empty, got %q", env.ParentJobID)
	}
	if env.Attempt != 0 {
		t.Errorf("Attempt: expected 0, got %d", env.Attempt)
	}
}

func TestGetEnv_InvalidAttemptDefaultsToZero(t *testing.T) {
	t.Setenv(EnvAttempt, "not-a-number")
	env := GetEnv()
	if env.Attempt != 0 {
		t.Errorf("Attempt: expected 0 for non-numeric value, got %d", env.Attempt)
	}
}

// ---- SubmitResult ----

// withResultPipe redirects FDs.ResultFD to the write end of an os.Pipe for the
// duration of the test. Returns the read end; SubmitResult closes the write end.
func withResultPipe(t *testing.T) *os.File {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := FDs.ResultFD
	FDs.ResultFD = int(w.Fd())
	t.Cleanup(func() {
		FDs.ResultFD = orig
		w.Close() // no-op if SubmitResult already closed it
		r.Close()
	})
	return r
}

func TestSubmitResult_WritesToResultFD(t *testing.T) {
	r := withResultPipe(t)

	type payload struct {
		Status string `json:"status"`
	}
	if err := SubmitResult(payload{Status: "ok"}); err != nil {
		t.Fatalf("SubmitResult: %v", err)
	}

	var got payload
	if err := json.NewDecoder(r).Decode(&got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if got.Status != "ok" {
		t.Fatalf("expected status 'ok', got %q", got.Status)
	}
}

func TestSubmitResult_FallbackToStdoutWhenFDClosed(t *testing.T) {
	orig := FDs.ResultFD
	FDs.ResultFD = 99
	t.Cleanup(func() { FDs.ResultFD = orig })

	// FD 99 is not open; SubmitResult should fall back to stdout without error.
	if err := SubmitResult(map[string]string{"x": "fallback"}); err != nil {
		t.Fatalf("SubmitResult fallback: %v", err)
	}
}

// ---- EmitJobs ----

// withSubjobsPipe redirects FDs.SubjobsFD to the write end of an os.Pipe for the
// duration of the test. Returns the read end; EmitJobs closes the write end.
func withSubjobsPipe(t *testing.T) *os.File {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := FDs.SubjobsFD
	FDs.SubjobsFD = int(w.Fd())
	t.Cleanup(func() {
		FDs.SubjobsFD = orig
		w.Close() // no-op if EmitJobs already closed it
		r.Close()
	})
	return r
}

func TestEmitJobs_WritesToSubjobsFD(t *testing.T) {
	r := withSubjobsPipe(t)

	jobs := []ChildJob{
		{Task: "child-a", Payload: map[string]string{"n": "1"}, Priority: 5},
		{Task: "child-b", Payload: nil},
	}
	if err := EmitJobs(jobs); err != nil {
		t.Fatalf("EmitJobs: %v", err)
	}

	var got []ChildJob
	if err := json.NewDecoder(r).Decode(&got); err != nil {
		t.Fatalf("decode child jobs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 child jobs, got %d", len(got))
	}
	if got[0].Task != "child-a" {
		t.Errorf("job[0].Task: expected 'child-a', got %q", got[0].Task)
	}
	if got[0].Priority != 5 {
		t.Errorf("job[0].Priority: expected 5, got %d", got[0].Priority)
	}
	if got[1].Task != "child-b" {
		t.Errorf("job[1].Task: expected 'child-b', got %q", got[1].Task)
	}
}

func TestEmitJobs_FallbackToStdoutWhenFDClosed(t *testing.T) {
	orig := FDs.SubjobsFD
	FDs.SubjobsFD = 99
	t.Cleanup(func() { FDs.SubjobsFD = orig })

	// FD 99 is not open; EmitJobs should fall back to stdout without error.
	if err := EmitJobs([]ChildJob{{Task: "my-task"}}); err != nil {
		t.Fatalf("EmitJobs fallback: %v", err)
	}
}
