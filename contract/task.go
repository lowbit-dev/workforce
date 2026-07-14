package contract

import "time"

// RetryPolicy controls how many times a job is retried after a TYPE_JOB_ERROR.
// Per-field zero values fall back to Manager.Config.DefaultRetryPolicy.
type RetryPolicy struct {
	// MaxAttempts is the total number of execution attempts allowed (1 = no retry; 0 = use default).
	MaxAttempts int
	// RetryDelay is the base delay before the first retry.
	// Subsequent delays grow geometrically: RetryDelay * 2^attempt, capped at MaxRetryDelay.
	RetryDelay time.Duration
	// MaxRetryDelay caps the computed geometric backoff delay. Zero means no cap.
	MaxRetryDelay time.Duration
}

// Limitations defines the resource and execution constraints the Worker enforces on the task binary.
// Zero values mean no limit is applied for that field.
type Limitations struct {
	// MaxExecutionTime is the maximum wall-clock time the binary may run.
	// Zero means no time limit.
	MaxExecutionTime time.Duration
	// MaxMemoryBytes is the maximum resident memory the process may use (bytes).
	// Enforced via RLIMIT_AS on Linux. Zero means no limit.
	MaxMemoryBytes int64
	// MaxCPUCores is the maximum number of CPU cores the process may use.
	// Enforced via cgroup cpu.max on Linux. Zero means no limit.
	MaxCPUCores float64
}

// Task is a registered task type stored in the TaskStore.
// It defines resource limits and retry behaviour for a named kind of work.
// The artifact (binary) for a task is always stored under the task's own name.
// One Task definition can produce many Job execution instances.
type Task struct {
	Name        string
	Limitations Limitations // execution constraints applied by the worker
	RetryPolicy RetryPolicy // retry behaviour on TYPE_JOB_ERROR
	// NoResult indicates the task binary does not write a result to FD3.
	// When true, the Worker skips the empty-FD3 error check and sends TYPE_RESULT with nil payload.
	NoResult bool
	// Cost is the worker capacity units consumed by each job execution of this task type.
	Cost int
}

// ArtifactInfo is the minimal artifact descriptor sent to a Worker inside a TYPE_PROPOSE_JOB packet.
type ArtifactInfo struct {
	Hash         string   // SHA-256 hex of the binary; used for cache lookup and integrity verification
	URL          string   // download URL served by the Manager's own HTTP server
	Dependencies []string // host binaries required in $PATH for this platform binary
}
