package manager

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/artifact"
	"lowbit.dev/workforce/manager/webhooks"
)

// JobStore persists Job execution instances.
type JobStore interface {
	SaveJob(ctx context.Context, job *contract.Job) error
	GetJob(ctx context.Context, id string) (*contract.Job, error)
	UpdateJob(ctx context.Context, id string, mutate func(*contract.Job)) error
	// ListJobs returns a page of root jobs (ParentJobID == "").
	// cursor is opaque; empty string = first page.
	// Returns the next cursor (empty string when no more pages).
	ListJobs(ctx context.Context, cursor string, limit int) ([]*contract.Job, string, error)
	// ListPendingJobs returns all jobs in Pending status, ordered by Priority descending.
	// Called once at Manager boot to rebuild the in-memory dispatch heap.
	ListPendingJobs(ctx context.Context) ([]*contract.Job, error)
	// ListRecoverableJobs returns all jobs in Proposing, Provisioning, or Running status.
	// Called at Manager boot; the Manager resets these to Pending and adds them to the heap.
	ListRecoverableJobs(ctx context.Context) ([]*contract.Job, error)
	// ListChildJobs returns all jobs with the given ParentJobID, ordered by CreatedAt ascending.
	ListChildJobs(ctx context.Context, parentJobID string) ([]*contract.Job, error)
	// CountJobsByStatus returns a count of all jobs (root and child) grouped by status.
	CountJobsByStatus(ctx context.Context) (map[contract.JobStatus]int, error)
}

// TaskStore persists Task definitions.
type TaskStore interface {
	SaveTask(ctx context.Context, task *contract.Task) error
	GetTask(ctx context.Context, name string) (*contract.Task, error)
	ListTasks(ctx context.Context) ([]*contract.Task, error)
	UpdateTask(ctx context.Context, name string, mutate func(*contract.Task)) error
	DeleteTask(ctx context.Context, name string) error
}

// RunStore persists JobRun execution attempt records.
type RunStore interface {
	CreateRun(ctx context.Context, run *contract.JobRun) error
	UpdateRun(ctx context.Context, id string, mutate func(*contract.JobRun)) error
	GetRun(ctx context.Context, id string) (*contract.JobRun, error)
	ListJobRuns(ctx context.Context, jobID string) ([]*contract.JobRun, error)
}

// LogStore is an append-only log store for job output.
// Logs are never loaded into memory as a whole; readers stream bytes directly to HTTP responses.
type LogStore interface {
	// AppendRunLog appends a chunk of log output to the given run's append-only log stream.
	// It also forwards the chunk to any job-level subscribers so the existing SSE endpoint
	// continues to work without change.
	AppendRunLog(ctx context.Context, runID string, data []byte) error
	// GetRunLogReader returns a reader over the full accumulated log for a single run.
	GetRunLogReader(ctx context.Context, runID string) (io.ReadCloser, error)
	// GetJobLogReader composes all run logs for jobID in StartedAt order.
	// Falls back to the legacy per-job log file if no runs exist (migration path).
	GetJobLogReader(ctx context.Context, jobID string) (io.ReadCloser, error)
	// GetRootJobLogReader returns a reader over the concatenated logs for all jobs in a job tree,
	// ordered by job creation time. Used for the root-level log endpoint.
	GetRootJobLogReader(ctx context.Context, rootJobID string) (io.ReadCloser, error)
	// SubscribeJobLogs returns a channel that receives log chunks in real time as they arrive.
	// The channel multiplexes across all runs for the job: new runs automatically forward
	// their chunks here. Closed when ctx is cancelled.
	SubscribeJobLogs(ctx context.Context, jobID string) (<-chan []byte, error)
	// SubscribeRunLogs returns a channel that receives log chunks for a specific run only.
	// Closed when ctx is cancelled or when the run reaches a terminal state.
	SubscribeRunLogs(ctx context.Context, runID string) (<-chan []byte, error)
	// CloseRunLogSubscribers closes all subscriber channels for a run and flushes any
	// buffered log data to durable storage. Call when a run reaches a terminal state.
	CloseRunLogSubscribers(runID string) error
	// CloseJobLogSubscribers closes all job-level subscriber channels.
	// Call when a job reaches a terminal state so SSE handlers exit cleanly.
	CloseJobLogSubscribers(jobID string) error
}

type ExitCodeRange struct {
	From uint8
	To   uint8
}

// Config holds all configuration for the Manager.
// The Manager implements http.Handler — it does not own an HTTP server.
// The developer wraps it in their own http.Server and controls TLS, timeouts, etc.
type Config struct {
	JobStore          JobStore
	TaskStore         TaskStore
	LogStore          LogStore
	RunStore          RunStore
	WebhookStore      webhooks.WebhookStore
	ArtifactsRegistry artifact.ArtifactRegistry

	Domain string

	// Logger is used for all internal Manager log output. Defaults to slog.Default() if nil.
	Logger *slog.Logger

	// WorkerSelector picks the target worker from the set of eligible workers for a task.
	// Defaults to LeastLoadedSelector if nil.
	WorkerSelector WorkerSelector

	// HeartbeatTimeout closes a worker connection if no heartbeat is received within this window.
	// Default: 30s.
	HeartbeatTimeout time.Duration

	WorkerConnectPath string

	// WorkerAuthFunc is called on POST /workers/connect before the connection is hijacked.
	// Receives the Bearer token from the Authorization header. Return non-nil to reject.
	// If nil, the Manager refuses to start unless NoWorkerAuth is explicitly true.
	WorkerAuthFunc func(token string) error

	// ClientAuthFunc is called on each HTTP request (excluding /workers/connect and /health).
	// Receives the full *http.Request. Return non-nil to reject.
	// If nil, the Manager refuses to start unless NoClientAuth is explicitly true.
	ClientAuthFunc func(r *http.Request) error

	// NoWorkerAuth must be explicitly true to allow unauthenticated worker connections.
	NoWorkerAuth bool

	// NoClientAuth must be explicitly true to allow unauthenticated HTTP API access.
	NoClientAuth bool

	Webhook *webhooks.WebhookDispatcherConfig

	// MaxArtifactUploadSize is the maximum multipart body size for POST /artifacts/{name}.
	// Default: 500 MB.
	MaxArtifactUploadSize int64

	// ArtifactSigningKey is a secret key used to generate HMAC-SHA256 signed download URLs.
	// When set, every artifact URL given to workers carries signed ?exp and ?sig query
	// parameters, and the Manager rejects download requests with missing, expired, or invalid
	// signatures. If nil, artifact download routes remain unauthenticated (backward-compatible).
	ArtifactSigningKey []byte

	// ArtifactSignedURLTTL controls how long a signed artifact URL remains valid.
	// Defaults to 1 hour. Has no effect when ArtifactSigningKey is nil.
	ArtifactSignedURLTTL time.Duration

	// DefaultRetryPolicy provides field-level fallback values for tasks whose
	// TaskDefinition.RetryPolicy has one or more zero fields.
	// Defaults: MaxAttempts=3, RetryDelay=1s, MaxRetryDelay=1m.
	DefaultRetryPolicy contract.RetryPolicy

	// StarvationTimeout is the duration after which a Pending task gets its effective priority
	// boosted. Zero disables anti-starvation aging. Default: 5m.
	StarvationTimeout time.Duration

	// OnResourceShortage is called when total pending task cost exceeds total cluster capacity.
	// Receives a ResourceShortageEvent with platform-level demand breakdown to guide worker
	// provisioning decisions. Rate-limited by ScaleUpCooldown. Optional.
	OnResourceShortage func(event ResourceShortageEvent)

	// ScaleUpCooldown is the minimum interval between consecutive OnResourceShortage calls.
	// Prevents scale-up thrashing when demand fluctuates rapidly. Default: 30s.
	ScaleUpCooldown time.Duration

	// IdleWorkerThreshold is how long a worker must be idle before OnIdleWorker is called for that worker.
	// Default: 5m.
	IdleWorkerThreshold time.Duration

	// OnIdleWorker is called when a worker has been idle past IdleWorkerThreshold.
	// Receives an IdleWorkerEvent with platform and cluster context to guide scale-down decisions.
	// Rate-limited per workerID by ScaleDownCooldown. Optional.
	OnIdleWorker func(event IdleWorkerEvent)

	// ScaleDownCooldown is the minimum interval between OnIdleWorker calls for the same workerID.
	// Default: 60s.
	ScaleDownCooldown time.Duration

	// IdleWorkerEvaluationInterval defines the interval on which the idleness of workers is evaluated (Default: 15 min)
	IdleWorkerEvaluationInterval time.Duration

	DefaultPanicExitCode           uint8
	DefaultDoNotRetryExitCodeRange ExitCodeRange
}

func (c *Config) applyDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.WorkerSelector == nil {
		c.WorkerSelector = LeastLoadedSelector{}
	}
	if c.IdleWorkerThreshold == 0 {
		c.IdleWorkerThreshold = 5 * time.Minute
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 30 * time.Second
	}
	if c.Webhook.WebhookTimeout == 0 {
		c.Webhook.WebhookTimeout = time.Second
	}
	if c.Webhook.WebhookMaxBackoff == 0 {
		c.Webhook.WebhookMaxBackoff = 5 * time.Minute
	}
	if c.MaxArtifactUploadSize == 0 {
		c.MaxArtifactUploadSize = 500 * 1024 * 1024
	}
	if c.ArtifactSignedURLTTL == 0 {
		c.ArtifactSignedURLTTL = time.Hour
	}
	if c.DefaultRetryPolicy.MaxAttempts == 0 {
		c.DefaultRetryPolicy.MaxAttempts = 3
	}
	if c.DefaultRetryPolicy.RetryDelay == 0 {
		c.DefaultRetryPolicy.RetryDelay = time.Second
	}

	if c.DefaultRetryPolicy.MaxRetryDelay == 0 {
		c.DefaultRetryPolicy.MaxRetryDelay = time.Minute
	}

	if c.StarvationTimeout == 0 {
		c.StarvationTimeout = 5 * time.Minute
	}

	if c.ScaleUpCooldown == 0 {
		c.ScaleUpCooldown = 30 * time.Second
	}

	if c.ScaleDownCooldown == 0 {
		c.ScaleDownCooldown = 60 * time.Second
	}

	if c.IdleWorkerEvaluationInterval == 0 {
		c.IdleWorkerEvaluationInterval = 15 * time.Minute
	}

	if c.WorkerConnectPath == "" {
		c.WorkerConnectPath = "/workers/connect"
	}

	if c.DefaultPanicExitCode == 0 {
		c.DefaultPanicExitCode = 2 // Default for Golang panics
	}

	if c.DefaultDoNotRetryExitCodeRange.From == 0 {
		c.DefaultDoNotRetryExitCodeRange.From = 200
	}

	if c.DefaultDoNotRetryExitCodeRange.To == 0 {
		c.DefaultDoNotRetryExitCodeRange.From = 254
	}
}

func (c *Config) validate() error {
	if c.JobStore == nil {
		return errors.New("Jobs store is required")
	}

	if c.TaskStore == nil {
		return errors.New("Tasks store is required")
	}

	if c.LogStore == nil {
		return errors.New("Logs store is required")
	}

	if c.RunStore == nil {
		return errors.New("Run store is required")
	}

	if c.WebhookStore == nil {
		return errors.New("Webhooks store is required")
	}

	if c.ArtifactsRegistry == nil {
		return errors.New("Artifact Registry is required")
	}

	if c.WorkerAuthFunc == nil && !c.NoWorkerAuth {
		return errors.New("WorkerAuthFunc is required; set NoWorkerAuth=true to allow unauthenticated workers")
	}

	if c.ClientAuthFunc == nil && !c.NoClientAuth {
		return errors.New("ClientAuthFunc is required; set NoClientAuth=true to allow unauthenticated API access")
	}

	return nil
}
