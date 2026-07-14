package taskkit

import (
	"os"
	"strconv"
)

// Environment variable names injected by the Worker before executing a task binary.
// These can be accessed directly via os.Getenv or through the typed GetEnv helper.
const (
	// EnvJobID is the unique ID of the job being executed.
	// Useful as an idempotency key when the job produces external side effects.
	EnvJobID = "WORKFORCE_JOB_ID"

	// EnvTaskType is the registered name of the task type (e.g. "segment-transcode").
	// Useful for logging and diagnostics inside shared binary entrypoints.
	EnvTaskType = "WORKFORCE_TASK_TYPE"

	// EnvParentJobID is the ID of the root job that submitted this execution.
	// Useful for correlating logs across all jobs in a tree.
	EnvParentJobID = "WORKFORCE_PARENT_JOB_ID"

	// EnvAttempt is the 1-based attempt number for this job execution.
	// 1 means first attempt; 2 means first retry; and so on.
	// Useful for conditional retry behaviour (e.g. backing off on attempt > 1).
	EnvAttempt = "WORKFORCE_ATTEMPT"

	// EnvPhase is the execution phase: "run" or "consolidate".
	// Use CurrentPhase() for typed access.
	EnvPhase = "WORKFORCE_PHASE"
)

// Env holds the execution context injected by the Worker into every task binary.
// All fields are empty/zero when the binary is run outside a Worker (e.g. in a terminal).
type Env struct {
	// JobID is the unique ID of this job execution.
	JobID string

	// TaskType is the registered task type name.
	TaskType string

	// ParentJobID is the ID of the root job this execution belongs to.
	// Empty for root jobs.
	ParentJobID string

	// Attempt is the 1-based attempt number. 1 = first attempt, 2 = first retry, etc.
	Attempt int

	Phase Phase
}

// GetEnv reads the WORKFORCE_* environment variables set by the Worker and returns
// them as a typed Env value. Call it once at the start of main and store the result.
//
//	env := taskkit.GetEnv()
//	taskkit.Log().Info("starting", "job_id", env.JobID, "attempt", env.Attempt)
//
// When running outside a Worker all fields are zero/empty — no error is returned.
// Binaries that do not call GetEnv are entirely unaffected.
func GetEnv() Env {
	attempt, _ := strconv.Atoi(os.Getenv(EnvAttempt))
	return Env{
		JobID:       os.Getenv(EnvJobID),
		TaskType:    os.Getenv(EnvTaskType),
		ParentJobID: os.Getenv(EnvParentJobID),
		Attempt:     attempt,
		Phase:       CurrentPhase(),
	}
}
