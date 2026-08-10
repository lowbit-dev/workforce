package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"lowbit.dev/workforce/contract"
)

func (w *Worker) handleMessage(ctx context.Context, msg contract.Message) {
	switch m := msg.(type) {
	case *contract.ProposeMessage:
		w.handlePropose(m)
	case *contract.DispatchMessage:
		w.handleDispatch(ctx, m)
	case *contract.HeartbeatMessage:
		w.lastHeartbeatAck.Store(time.Now().UnixNano())
		slog.Debug("[Worker][handleMessage] Heartbeat received from manager")
	case *contract.SystemMessage:
		w.handleSystem(m)
	case *contract.CancelMessage:
		w.handleCancel(m)
	default:
		w.sendError("unhandled_message", fmt.Sprintf("message of type %T is not handled", msg))
		w.cfg.Logger.Warn("[Worker][handleMessage] Received unexpected message type", "type", fmt.Sprintf("%T", msg))
	}
}

// sendError sends a structured error message to the manager.
// code is a machine-readable token (e.g. "unknown_verb", "invalid_message").
// message is a human-readable description; single quotes are stripped to keep
// the netargv framing valid.
func (w *Worker) sendError(code, message string) {
	safe := strings.ReplaceAll(message, "'", "")
	w.send(fmt.Sprintf("error --code=%s --message='%s'", code, safe))
}

func (w *Worker) handlePropose(msg *contract.ProposeMessage) {
	if !w.state.Is(contract.WorkerStateOnline) {
		w.send(fmt.Sprintf("reject --job-id=%s --reason=draining", msg.JobID))
		return
	}

	if w.cfg.ResourceLimits.MaxCPUPercent > 0 || w.cfg.ResourceLimits.MaxMemPercent > 0 {
		snap := w.metricsSampler.snapshot()
		if snap.IsOverLimit(w.cfg.ResourceLimits.MaxCPUPercent, w.cfg.ResourceLimits.MaxMemPercent) {
			w.send(fmt.Sprintf("reject --job-id=%s --reason=pressure", msg.JobID))
			return
		}
	}

	if missing := w.checkDeps(msg.ArtifactDeps); len(missing) > 0 {
		w.send(fmt.Sprintf("reject --job-id=%s --reason=missing_deps --detail='%s'", msg.JobID, strings.Join(missing, ",")))
		return
	}

	w.tasksMu.Lock()
	canAccept := (w.usedCap + msg.Cost) <= w.cfg.Capacity
	if canAccept {
		w.usedCap += msg.Cost
		w.pendingJobs[msg.JobID] = msg
	}
	w.tasksMu.Unlock()

	if !canAccept {
		w.send(fmt.Sprintf("reject --job-id=%s --reason=capacity", msg.JobID))
		return
	}

	w.send(fmt.Sprintf("accept --job-id=%s", msg.JobID))
}

func (w *Worker) handleDispatch(ctx context.Context, msg *contract.DispatchMessage) {
	w.tasksMu.Lock()
	proposal, ok := w.pendingJobs[msg.JobID]
	if !ok {
		w.tasksMu.Unlock()

		w.sendError("unknown_job", "dispatched job was not proposed or accepted")
		return
	}

	delete(w.pendingJobs, msg.JobID)
	taskCtx, cancel := context.WithCancel(ctx)
	w.tasks[msg.JobID] = &activeTask{cancel: cancel, cost: proposal.Cost, runID: msg.RunID}
	w.tasksMu.Unlock()

	w.send(fmt.Sprintf("staged --job-id=%s --run-id=%s", msg.JobID, msg.RunID))

	go w.runJob(taskCtx, proposal, msg)
}

func (w *Worker) handleSystem(msg *contract.SystemMessage) {
	slog.Debug("[Worker][handleMessage] Reveived System Message", "command", msg.Command)

	switch msg.Command {
	case "drain":
		w.state.Store(contract.WorkerStateDraining)
		w.tasksMu.Lock()
		remaining := len(w.tasks)

		if remaining == 0 {
			w.state.Store(contract.WorkerStateShuttingDown)
			w.cfg.Logger.Info("[Worker][handleSystem] Drain completed (no active tasks. Closing connection...")
			w.conn.Close()
		}
	case "shutdown":
		if w.state.Transition(contract.WorkerStateOnline, contract.WorkerStateShuttingDown) || w.state.Transition(contract.WorkerStateDraining, contract.WorkerStateShuttingDown) {
			close(w.done)
		}
	}
}

func (w *Worker) handleCancel(msg *contract.CancelMessage) {
	w.tasksMu.Lock()
	t, ok := w.tasks[msg.JobID]
	w.tasksMu.Unlock()
	if ok {
		t.cancel()
	}
}

// runJob fetches the artifact, enforces resource limits, runs the task binary,
// streams logs, and sends the final result to the manager.
func (w *Worker) runJob(ctx context.Context, proposal *contract.ProposeMessage, dispatch *contract.DispatchMessage) {
	defer w.releaseTask(dispatch.JobID)

	w.cfg.Logger.Info("[Worker][runJob] Job started", "job_id", dispatch.JobID, "task", proposal.Task, "phase", dispatch.Phase)

	binaryPath, err := w.artifactCache.Fetch(ctx, proposal.ArtifactHash, proposal.ArtifactURL)
	if err != nil {
		w.cfg.Logger.Error("[Worker][runJob] Failed to fetch artifact", "job_id", dispatch.JobID, "error", err)
		w.send(fmt.Sprintf("result --job-id=%s --type=error --reason='%s'",
			dispatch.JobID, sanitize(err.Error())))
		return
	}

	w.send(fmt.Sprintf("starting --job-id=%s --run-id=%s", dispatch.JobID, dispatch.RunID))

	t := taskExec{
		JobID:       dispatch.JobID,
		TaskName:    proposal.Task,
		ParentJobID: proposal.ParentJobID,
		Phase:       dispatch.Phase,
		Attempt:     dispatch.Attempt,
		BinaryPath:  binaryPath,
		Payload:     []byte(dispatch.Payload),
		NoResult:    proposal.NoResult,
		Limits: Limitations{
			MaxExecutionTime: dispatch.MaxExecTime,
			MaxMemoryBytes:   dispatch.MaxMemoryBytes,
			MaxCPUCores:      dispatch.MaxCPUCores,
		},
		Proc: ProcConfig{
			Env: append(SystemEnv(), EnvWithPrefix(w.cfg.InheritableENVPrefix)...),
		},
	}

	logWriter := &logLineWriter{jobID: dispatch.JobID, runID: dispatch.RunID, w: w}

	logHeader := fmt.Sprintf("log --job-id=%s --run-id=%s", dispatch.JobID, dispatch.RunID)
	headertmpl := `
┌────────────────────────────────────────────────────────
│  %s
│  phase:   %s
│  attempt: %d
│  job:     %s
│  run:     %s
│  time:    %s
│  worker:  %s
└────────────────────────────────────────────────────────
`
	w.sendWithPayload(logHeader, []byte(fmt.Sprintf(headertmpl, proposal.Task, dispatch.Phase, dispatch.Attempt, dispatch.JobID, dispatch.RunID, time.Now().Format(time.RFC3339), w.cfg.WorkerID)))

	start := time.Now()
	resultData, childJobsData, warnings, err := w.RunTask(ctx, t, logWriter)
	duration := time.Since(start)

	logFooterHeader := fmt.Sprintf("log --job-id=%s --run-id=%s", dispatch.JobID, dispatch.RunID)
	footertmpl := `


┌────────────────────────────────────────────────────────
│  status:   %s
│  time:     %s
│  duration: %s
└────────────────────────────────────────────────────────

%s
`

	if err != nil {
		code, reason, isExit := IsExitError(err)
		if !isExit {
			reason = err.Error()
		}

		w.sendWithPayload(logFooterHeader, []byte(fmt.Sprintf(footertmpl, "failed", time.Now().Format(time.RFC3339), duration.String(), reason)))

		header := fmt.Sprintf("result --job-id=%s --run-id=%s --type=error --exit-code=%d --duration=%s", dispatch.JobID, dispatch.RunID, code, duration.String())
		slog.Debug("[Worker][runJob] Sending job error message", "header", header)
		w.sendWithPayload(header, []byte(reason))
		return
	}

	if len(childJobsData) > 0 {
		w.sendWithPayload(logFooterHeader, []byte(fmt.Sprintf(footertmpl, "success", time.Now().Format(time.RFC3339), duration.String(), string(childJobsData))))

		header := fmt.Sprintf("result --job-id=%s --run-id=%s --type=subjobs --duration=%s", dispatch.JobID, dispatch.RunID, duration.String())
		slog.Debug("[Worker][runJob] Sending child job result message", "header", header, "payload", string(childJobsData))
		w.sendWithPayload(header, childJobsData)
		return
	}

	w.sendWithPayload(logFooterHeader, []byte(fmt.Sprintf(footertmpl, "success", time.Now().Format(time.RFC3339), duration.String(), string(resultData))))

	header := fmt.Sprintf("result --job-id=%s --run-id=%s --type=result --duration=%s", dispatch.JobID, dispatch.RunID, duration.String())
	if warnings != "" {
		header += fmt.Sprintf(" --warnings='%s'", sanitize(warnings))
	}

	slog.Debug("[Worker][runJob] Sending job complete message", "header", header, "payload", string(resultData))

	w.sendWithPayload(header, resultData)
}

// sanitize strips single quotes so values are safe to embed in a netargv single-quoted flag.
func sanitize(s string) string {
	return strings.Trim(strings.ReplaceAll(s, "'", ""), "\n\r\t")
}

// =====================================================================
// LogLineWriter
// =====================================================================

// logLineWriter is an io.Writer that forwards each completed line from the task
// binary's stdout to the manager as a "log" message.
type logLineWriter struct {
	jobID string
	runID string
	w     *Worker
	buf   []byte
}

func (l *logLineWriter) Write(p []byte) (int, error) {
	l.buf = append(l.buf, p...)
	for {
		idx := bytes.IndexByte(l.buf, '\n')
		if idx < 0 {
			break
		}
		line := l.buf[:idx]
		l.buf = l.buf[idx+1:]
		l.w.sendWithPayload(fmt.Sprintf("log --job-id=%s --run-id=%s", l.jobID, l.runID), line)
	}
	return len(p), nil
}

func (w *Worker) releaseTask(jobID string) {
	w.tasksMu.Lock()
	if t, ok := w.tasks[jobID]; ok {
		w.usedCap -= t.cost
		delete(w.tasks, jobID)
	}
	remaining := len(w.tasks)
	w.tasksMu.Unlock()

	if (w.state.Is(contract.WorkerStateDraining) || w.state.Is(contract.WorkerStateShuttingDown)) && remaining == 0 {
		w.state.Store(contract.WorkerStateShuttingDown)

		w.cfg.Logger.Info("[Worker][releaseTask] Drain completed. Closing connection...")
		w.conn.Close()
	}
}
