package contract

import "time"

// RunStatus is the lifecycle state of a single JobRun execution attempt.
type RunStatus string

const (
	// RunStatusProvisioning means the manager sent DispatchMessage but the binary has not started yet.
	RunStatusProvisioning RunStatus = "provisioning"
	// RunStatusRunning means the task binary is currently executing.
	RunStatusRunning RunStatus = "running"
	// RunStatusCompleted means the task binary exited cleanly and a result was produced.
	RunStatusCompleted RunStatus = "completed"
	// RunStatusFailed means the run ended with an error (non-zero exit, timeout, worker disconnect, etc).
	RunStatusFailed RunStatus = "failed"
)

// JobRun records one execution attempt of a Job.
// A new JobRun is created each time the manager dispatches a job to a worker.
// On retry, the job gets a fresh JobRun while the previous run's record is preserved.
type JobRun struct {
	ID       string
	JobID    string
	Phase    JobPhase // phase of the job at the time this run was dispatched (run or consolidate)
	Attempt  int      // 1-based; mirrors Job.Attempts at the time of creation
	WorkerID string
	Status   RunStatus
	// InputPayload is the exact stdin payload sent to the worker for this run.
	InputPayload JsonOrBytes
	// OutputPayload is the payload returned by the worker for this run.
	// For ResultSuccess this is the task result bytes; for ResultSubjobs this is the
	// raw child-jobs array; for ResultError this stores the reason text bytes.
	OutputPayload JsonOrBytes

	// FailureReason is the human-readable error description, populated when Status == RunStatusFailed.
	FailureReason string
	// ExitCode is the process exit code when the task binary exited non-zero. Zero for non-exit failures.
	ExitCode int

	// ResultType indicated what the type of result was that this run returned, error, result or subjobs
	ResultType ResultType

	// StartedAt is set when the manager receives StartingMessage (binary has actually started).
	// Zero while Status == RunStatusProvisioning.
	StartedAt time.Time
	// FinishedAt is set when the run reaches a terminal state. Zero while still in progress.
	FinishedAt time.Time
}
