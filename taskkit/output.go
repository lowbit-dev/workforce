package taskkit

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
)

var (
	once   sync.Once
	logger *slog.Logger
)

// Log returns the package-level *slog.Logger wired to stdout via slog.NewTextHandler.
// Task binaries should use this logger for all output; plain-text lines are streamed
// back to the Manager as TYPE_LOG_STREAM packets.
func Log() *slog.Logger {
	once.Do(func() {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	})
	return logger
}

// SubmitResult JSON-encodes v and writes it to FD3 (the result pipe).
// It closes FD3 after writing so the Worker knows the result is complete.
// Must be called exactly once before the binary exits.
//
// When FD3 is not open — e.g. when running the binary directly in a terminal
// during development — SubmitResult falls back to stdout with a clear visual
// boundary so the result is still visible:
//
//	=== TASK RESULT ===
//	{"status":"ok"}
//	==================
func SubmitResult(v any) error {
	// Open the result fd by number, write, and close.
	resultFD := os.NewFile(uintptr(FDs.ResultFD), "result")
	if resultFD != nil {
		if _, err := resultFD.Stat(); err == nil {
			defer resultFD.Close()
			if err := json.NewEncoder(resultFD).Encode(v); err != nil {
				return fmt.Errorf("write result to FD%d: %w", FDs.ResultFD, err)
			}
			return nil
		}
		resultFD.Close()
	}

	// FD3 is not open — running outside a Worker (e.g. directly in a terminal).
	encoded, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal result for stdout fallback: %w", err)
	}

	fmt.Fprint(os.Stdout, "\n\n=== RESULT =============================================\n")
	fmt.Fprintln(os.Stdout, string(encoded))
	return nil
}

// ChildJob is a single child job request to be submitted for decomposition.
type ChildJob struct {
	// Task is the registered task name to run for this child job.
	Task string `json:"task"`
	// Payload is the input for the child job. Will be JSON-encoded.
	Payload any `json:"payload"`
	// Priority controls dispatch order; higher = dispatched first. Optional.
	Priority int `json:"priority,omitempty"`
	// Version pins a specific artifact version. Optional; defaults to latest.
	Version string `json:"version,omitempty"`
}

// EmitJobs writes the given child job requests to FD4 as a JSON array.
// The Worker reads FD4 after the binary exits and asks the Manager to create
// the corresponding child Job records.
//
// When FD4 is not open — e.g. when running the binary directly in a terminal
// during development — EmitJobs falls back to stdout with a clear visual
// boundary so the requests are still visible:
//
//	=== CHILD JOBS ===
//	[{"task":"process","payload":{"id":1}}]
//	==================
func EmitJobs(jobs []ChildJob) error {
	// Open the subjobs fd by number, write, and close.
	subjobsFD := os.NewFile(uintptr(FDs.SubjobsFD), "subjobs")
	if subjobsFD != nil {
		if _, err := subjobsFD.Stat(); err == nil {
			defer subjobsFD.Close()
			if err := json.NewEncoder(subjobsFD).Encode(jobs); err != nil {
				return fmt.Errorf("write child jobs to FD%d: %w", FDs.SubjobsFD, err)
			}
			return nil
		}
		subjobsFD.Close()
	}

	// FD4 is not open — running outside a Worker (e.g. directly in a terminal).
	encoded, err := json.MarshalIndent(jobs, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal child jobs for stdout fallback: %w", err)
	}

	fmt.Fprint(os.Stdout, "\n\n=== CHILD JOBS ==========================================\n")
	fmt.Fprintln(os.Stdout, string(encoded))
	return nil
}
