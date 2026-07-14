package contract

import "time"

// JobStatus is the lifecycle state of a Job execution instance.
type JobStatus string

const (
	JobStatusPending          JobStatus = "pending"
	JobStatusProposing        JobStatus = "proposing"
	JobStatusAccepted         JobStatus = "accepted"
	JobStatusProvisioning     JobStatus = "provisioning"
	JobStatusRunning          JobStatus = "running"
	JobStatusAwaitingChildren JobStatus = "awaiting_children"
	JobStatusCompleted        JobStatus = "completed"
	JobStatusFailed           JobStatus = "failed"
	JobStatusCancelled        JobStatus = "cancelled"
)

// IsTerminal reports whether s is a terminal job state.
func (s JobStatus) IsTerminal() bool {
	return s == JobStatusCompleted || s == JobStatusFailed || s == JobStatusCancelled
}

type JobPhase string

const (
	JobPhaseRun         JobPhase = "run"
	JobPhaseConsolidate JobPhase = "consolidate"
)

// Job is a single execution instance of a Task.
// Root jobs are submitted by clients via POST /jobs.
// Child jobs are created by the Manager when a binary writes to FD4 during decomposition.
// All jobs — root and children alike — reference a Task by name.
type Job struct {
	ID              string
	ParentJobID     string // empty for root jobs; set for child jobs created via decomposition
	TaskName        string // references Task.Name
	ArtifactVersion string // artifact version pinned at dispatch time
	Status          JobStatus
	Phase           JobPhase // "run" (default) or "consolidate"
	Priority        int      // higher value = dispatched first; default 0
	Cost            int
	Attempts        int // incremented on each TYPE_JOB_ERROR
	Payload         JsonOrBytes
	Result          JsonOrBytes
	FailureReason   string
	// Webhook fields — only meaningful on root jobs; child jobs inherit none.
	WebhookURL     string
	WebhookEvents  []string
	WebhookHeaders map[string]string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ChildJobResult holds the task name and result of a single completed child job,
// used to build the ConsolidatePayload sent to a binary in consolidate phase.
type ChildJobResult struct {
	JobID    string      `json:"job_id"`
	TaskName string      `json:"task_name"`
	Result   JsonOrBytes `json:"result"`
}

// ConsolidatePayload is the stdin payload delivered to a task binary when it is
// re-invoked in the "consolidate" phase. It contains the original job payload
// and the ordered results of all completed child jobs.
//
// Result is typed as RawResult: valid JSON is inlined verbatim; non-JSON bytes
// fall back to a base64-encoded string.
type ConsolidatePayload struct {
	// Payload is the original job payload, verbatim.
	Payload JsonOrBytes `json:"payload"`
	// Children holds one entry per completed child job, in creation order.
	Children []ChildJobResult `json:"children"`
}
