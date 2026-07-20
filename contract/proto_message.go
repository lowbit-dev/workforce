package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"lowbit.dev/netargv"
	"lowbit.dev/verreg"
)

// MessageFactory constructs and validates a typed Message from a raw netargv frame.
type MessageFactory func(netargv.Message) (Message, error)

// Message is the common interface for all Manager↔Worker messages.
type Message interface{}

// RegisterMessages registers all known message factories into reg.
func RegisterMessages(reg *verreg.Registry[MessageFactory]) {
	reg.Register(0, "propose", proposeV0Factory)
	reg.Register(0, "accept", acceptV0Factory)
	reg.Register(0, "reject", rejectV0Factory)
	reg.Register(0, "dispatch", dispatchV0Factory)
	reg.Register(0, "staged", stagedV0Factory)
	reg.Register(0, "starting", startingV0Factory)
	reg.Register(0, "log", logV0Factory)
	reg.Register(0, "result", resultV0Factory)
	reg.Register(0, "heartbeat", heartbeatV0Factory)
	reg.Register(0, "system", systemV0Factory)
	reg.Register(0, "capacity", capacityV0Factory)
	reg.Register(0, "cancel", cancelV0Factory)
}

// ---- helpers ----

func requireFlag(flags netargv.FlagSet, name string) (string, error) {
	v, ok := flags.Lookup(name)
	if !ok || v == "" {
		return "", fmt.Errorf("missing required flag --%s", name)
	}

	return v, nil
}

func requireInt(flags netargv.FlagSet, name string) (int, error) {
	s, err := requireFlag(flags, name)
	if err != nil {
		return 0, err
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("flag --%s: %w", name, err)
	}

	return n, nil
}

func requireFloat(flags netargv.FlagSet, name string) (float64, error) {
	s, err := requireFlag(flags, name)
	if err != nil {
		return 0, err
	}

	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("flag --%s: %w", name, err)
	}

	return n, nil
}

func optionalInt(flags netargv.FlagSet, name string) (int, bool, error) {
	s, err := requireFlag(flags, name)
	if err != nil {
		return 0, false, nil
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, true, fmt.Errorf("flag --%s: %w", name, err)
	}

	return n, true, nil
}

func optionalFloat(flags netargv.FlagSet, name string) (float64, bool, error) {
	s, err := requireFlag(flags, name)
	if err != nil {
		return 0, false, nil
	}

	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, true, fmt.Errorf("flag --%s: %w", name, err)
	}

	return n, true, nil
}

// ---- ProposeMessage ----

// ProposeMessage is sent Manager → Worker to propose a job.
// It carries only the information the worker needs to decide whether it can
// accept: identity, routing, cost, and artifact coordinates. Runtime
// constraints and the stdin payload are deferred to DispatchMessage.
type ProposeMessage struct {
	JobID        string
	ParentJobID  string
	Task         string
	Cost         int
	NoResult     bool
	ArtifactHash string
	ArtifactURL  string
	ArtifactDeps []string
}

func proposeV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()

	jobID, err := requireFlag(flags, "job-id")
	if err != nil {
		return nil, err
	}
	task, err := requireFlag(flags, "task")
	if err != nil {
		return nil, err
	}
	cost, err := requireInt(flags, "cost")
	if err != nil {
		return nil, err
	}
	artifactHash, err := requireFlag(flags, "artifact-hash")
	if err != nil {
		return nil, err
	}
	artifactURL, err := requireFlag(flags, "artifact-url")
	if err != nil {
		return nil, err
	}

	return &ProposeMessage{
		JobID:        jobID,
		ParentJobID:  flags.Get("parent-job-id"),
		Task:         task,
		Cost:         cost,
		NoResult:     flags.Has("no-result"),
		ArtifactHash: artifactHash,
		ArtifactURL:  artifactURL,
		ArtifactDeps: flags.GetRepeated("dep"),
	}, nil
}

func FormulateProposeV0Message(job *Job, artifactInfo *ArtifactInfo) string {
	msg := fmt.Sprintf("propose --job-id=%s --task=%s --cost=%d --artifact-hash=%s --artifact-url=%s",
		job.ID, job.TaskName, job.Cost, artifactInfo.Hash, artifactInfo.URL,
	)

	for _, dep := range artifactInfo.Dependencies {
		msg += " --dep=" + dep
	}

	return msg
}

// ---- DispatchMessage ----

// DispatchMessage is sent Manager → Worker after the worker has accepted a proposal.
// It carries the runtime constraints, execution context, and the actual stdin payload.
type DispatchMessage struct {
	JobID          string
	RunID          string // manager-generated run identifier; echoed back on all subsequent frames
	Phase          string
	Attempt        int
	MaxExecTime    time.Duration
	MaxMemoryBytes int64
	MaxCPUCores    float64
	// Payload is the job's stdin input, delivered as the binary payload.
	Payload JsonOrBytes
}

func dispatchV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()

	jobID, err := requireFlag(flags, "job-id")
	if err != nil {
		return nil, err
	}
	phase, err := requireFlag(flags, "phase")
	if err != nil {
		return nil, err
	}
	if phase != "run" && phase != "consolidate" {
		return nil, fmt.Errorf("flag --phase: must be \"run\" or \"consolidate\", got %q", phase)
	}
	attempt, err := requireInt(flags, "attempt")
	if err != nil {
		return nil, err
	}

	runID, err := requireFlag(flags, "run-id")
	if err != nil {
		return nil, err
	}

	msg := &DispatchMessage{
		JobID:   jobID,
		RunID:   runID,
		Phase:   phase,
		Attempt: attempt,
		Payload: m.Payload(),
	}

	if s := flags.Get("max-exec-time"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("flag --max-exec-time: %w", err)
		}
		msg.MaxExecTime = d
	}
	if s := flags.Get("max-memory"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("flag --max-memory: %w", err)
		}
		msg.MaxMemoryBytes = n
	}
	if s := flags.Get("max-cpu-cores"); s != "" {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("flag --max-cpu-cores: %w", err)
		}
		msg.MaxCPUCores = f
	}

	return msg, nil
}

func FormulateDispatchV0Message(job *Job, runID string) string {
	return fmt.Sprintf("dispatch --job-id=%s --run-id=%s --phase=%s --attempt=%d",
		job.ID, runID, string(job.Phase), job.Attempts,
	)
}

// ---- StagedMessage ----

// StagedMessage is sent Worker → Manager after a dispatch is received.
// It signals that the worker has reserved its slot and is staging the run
// (e.g. fetching the artifact binary). A reject sent after staged releases the slot.
type StagedMessage struct {
	JobID string
	RunID string // echoed from DispatchMessage
}

func stagedV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()
	jobID, err := requireFlag(flags, "job-id")
	if err != nil {
		return nil, err
	}
	runID, err := requireFlag(flags, "run-id")
	if err != nil {
		return nil, err
	}
	return &StagedMessage{JobID: jobID, RunID: runID}, nil
}

// ---- AcceptMessage ----

// AcceptMessage is sent Worker → Manager when a worker accepts a proposed job.
type AcceptMessage struct {
	JobID string
}

func acceptV0Factory(m netargv.Message) (Message, error) {
	jobID, err := requireFlag(m.Flags(), "job-id")
	if err != nil {
		return nil, err
	}
	return &AcceptMessage{JobID: jobID}, nil
}

// ---- RejectMessage ----

// RejectMessage is sent Worker → Manager when a worker declines a proposed job.
// Reason should be one of the known prefixes: "capacity", "draining", "pressure".
type RejectMessage struct {
	JobID  string
	Reason string
}

func rejectV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()
	jobID, err := requireFlag(flags, "job-id")
	if err != nil {
		return nil, err
	}
	reason, err := requireFlag(flags, "reason")
	if err != nil {
		return nil, err
	}
	return &RejectMessage{JobID: jobID, Reason: reason}, nil
}

// ---- StartingMessage ----

// StartingMessage is sent Worker → Manager once the task binary has been fetched and started.
type StartingMessage struct {
	JobID string
	RunID string // echoed from DispatchMessage
}

func startingV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()
	jobID, err := requireFlag(flags, "job-id")
	if err != nil {
		return nil, err
	}
	runID, err := requireFlag(flags, "run-id")
	if err != nil {
		return nil, err
	}
	return &StartingMessage{JobID: jobID, RunID: runID}, nil
}

// ---- LogMessage ----

// LogMessage is sent Worker → Manager for each stdout line from the running binary.
// The line content is carried as the message payload.
type LogMessage struct {
	JobID string
	RunID string // echoed from DispatchMessage
	Line  []byte
}

func logV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()
	jobID, err := requireFlag(flags, "job-id")
	if err != nil {
		return nil, err
	}
	runID, err := requireFlag(flags, "run-id")
	if err != nil {
		return nil, err
	}
	return &LogMessage{JobID: jobID, RunID: runID, Line: m.Payload()}, nil
}

// ---- ResultMessage ----

var ErrUnknownResultType error = errors.New("unknown result type")

// ResultType identifies which kind of result the worker is reporting.
type ResultType string

const (
	ResultSuccess ResultType = "result"  // clean exit; result bytes in Payload
	ResultError   ResultType = "error"   // non-zero exit or runner failure
	ResultSubjobs ResultType = "subjobs" // binary emitted child jobs via FD4
)

// ChildJobRequest is one child job request written to FD4 by a decomposing binary.
type ChildJobRequest struct {
	Task     string      `json:"task"`
	Payload  JsonOrBytes `json:"payload"`
	Priority int         `json:"priority,omitempty"`
	Version  string      `json:"version,omitempty"`
}

// ResultMessage is sent Worker → Manager to report the terminal result of a job.
//
//	--type=result   Payload holds the raw result bytes from FD3; Warnings may be set.
//	--type=error    Reason and ExitCode describe the failure.
//	--type=subjobs  Payload holds a JSON array of ChildJobRequest values (from FD4).
type ResultMessage struct {
	ParentJobID string
	JobID       string
	RunID       string // echoed from DispatchMessage
	Type        ResultType
	Duration    time.Duration     // wall-clock time the task binary ran (excludes artifact fetch)
	Warnings    string            // result: non-empty stderr on a clean exit
	Reason      string            // error: human-readable failure description
	ExitCode    int               // error: process exit code
	Jobs        []ChildJobRequest // subjobs: decoded child job requests
	// Payload holds result bytes (type=result) or raw FD4 bytes (type=subjobs).
	Payload JsonOrBytes
}

func resultV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()

	jobID, err := requireFlag(flags, "job-id")
	if err != nil {
		return nil, err
	}

	outcomeType, err := requireFlag(flags, "type")
	if err != nil {
		return nil, err
	}

	runID, err := requireFlag(flags, "run-id")
	if err != nil {
		return nil, err
	}

	parentJobID, _ := flags.Lookup("parent-job-id")
	msg := &ResultMessage{
		JobID:       jobID,
		RunID:       runID,
		ParentJobID: parentJobID,
		Type:        ResultType(outcomeType),
		Duration:    -1,
	}

	if s := flags.Get("duration"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("flag --duration: %w", err)
		}
		msg.Duration = d
	}

	switch msg.Type {
	case ResultSuccess:
		msg.Warnings = flags.Get("warnings")
		msg.Payload = m.Payload()

	case ResultError:
		msg.Reason = string(m.Payload())

		if s := flags.Get("exit-code"); s != "" {
			code, err := strconv.Atoi(s)
			if err != nil {
				return nil, fmt.Errorf("flag --exit-code: %w", err)
			}
			msg.ExitCode = code
		}

	case ResultSubjobs:
		if len(m.Payload()) == 0 {
			return nil, errors.New("outcome subjobs: payload is required")
		}
		if err := json.Unmarshal(m.Payload(), &msg.Jobs); err != nil {
			return nil, fmt.Errorf("outcome subjobs: invalid payload: %w", err)
		}
		if len(msg.Jobs) == 0 {
			return nil, errors.New("outcome subjobs: payload must contain at least one job")
		}
		msg.Payload = m.Payload()

	default:
		return nil, fmt.Errorf("flag --type: unknown outcome type %q", outcomeType)
	}

	return msg, nil
}

// ---- HeartbeatMessage ----

// HeartbeatMessage is sent Worker → Manager as a keepalive ping; echoed back by the Manager as pong.
type HeartbeatMessage struct {
	CPUPercent    float64
	MemoryPercent float64
	State         WorkerState
}

func heartbeatV0Factory(m netargv.Message) (Message, error) {
	flags := m.Flags()

	cpu, found, err := optionalFloat(flags, "cpu")
	if found && err != nil {
		return nil, err
	}

	mem, err := requireFloat(flags, "mem")
	if found && err != nil {
		return nil, err
	}

	state, found, err := optionalInt(flags, "state")
	if found && err != nil {
		return nil, err
	}

	if found && !IsValidWorkerState(state) {
		return nil, errors.New("flag --state is not a valid state")
	}

	return &HeartbeatMessage{
		CPUPercent:    cpu,
		MemoryPercent: mem,
		State:         WorkerState(state),
	}, nil
}

// ---- SystemMessage ----

type SystemCommand string

const (
	SystemCommandDrain    SystemCommand = "drain"
	SystemCommandShutdown SystemCommand = "shutdown"
)

// SystemMessage is sent Manager → Worker to change the worker's operating mode.
type SystemMessage struct {
	// Command is either "drain" or "shutdown".
	Command SystemCommand
}

var errInvalidSystemCommand = errors.New(`flag --command: must be "drain" or "shutdown"`)

func systemV0Factory(m netargv.Message) (Message, error) {
	cmd, err := requireFlag(m.Flags(), "command")
	if err != nil {
		return nil, err
	}

	if cmd != "drain" && cmd != "shutdown" {
		return nil, errInvalidSystemCommand
	}

	return &SystemMessage{Command: SystemCommand(cmd)}, nil
}

func ForulateSystemV0Message(command SystemCommand) string {
	return fmt.Sprintf("system --command=%s", string(command))
}

// ---- CapacityMessage ----

// CapacityMessage is sent Worker → Manager to report a corrected view of available capacity.
type CapacityMessage struct {
	Available int
}

func capacityV0Factory(m netargv.Message) (Message, error) {
	n, err := requireInt(m.Flags(), "available")
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, errors.New("flag --available: must be >= 0")
	}
	return &CapacityMessage{Available: n}, nil
}

// ---- CancelMessage ----

// CancelMessage is sent Manager → Worker to cancel a specific running job.
type CancelMessage struct {
	JobID string
}

func cancelV0Factory(m netargv.Message) (Message, error) {
	jobID, err := requireFlag(m.Flags(), "job-id")
	if err != nil {
		return nil, err
	}
	return &CancelMessage{JobID: jobID}, nil
}
