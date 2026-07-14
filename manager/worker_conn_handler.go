package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"lowbit.dev/cooper"
	"lowbit.dev/netargv"
	"lowbit.dev/problemjson"
	"lowbit.dev/ulid"
	"lowbit.dev/verreg"
	"lowbit.dev/workforce/contract"
)

// #####################################################################
// Serve Worker connection
// #####################################################################

var ErrManagerNil = errors.New("the manager instance is nil")

type WorkerConnServer struct {
	m *Manager
}

func NewWorkerConnServer(m *Manager) *WorkerConnServer {
	return &WorkerConnServer{m: m}
}

func (s *WorkerConnServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.m.Logger().Debug("[WorkerConnServer][ServeHTTP] Client initiated handshake", "remote", r.RemoteAddr)

	if s.m.cfg.WorkerAuthFunc != nil {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if err := s.m.cfg.WorkerAuthFunc(token); err != nil {
			s.m.Logger().Warn("[WorkerConnServer][ServeHTTP] Client failed authentication", "remote", r.RemoteAddr)
			problemjson.Unauthorized(problemjson.Error(err)).ServeHTTP(w, r)
			return
		}
	}

	// =====================================================================
	// Extract worker details from the requets
	// =====================================================================
	id := r.Header.Get("X-Worker-ID")
	os := r.Header.Get("X-Worker-OS")
	arch := r.Header.Get("X-Worker-Arch")
	capacityStr := r.Header.Get("X-Worker-Capacity")
	capacity, err := strconv.ParseInt(capacityStr, 10, 64)
	if err != nil {
		s.m.Logger().Warn("[WorkerConnServer][ServeHTTP] Client send unparsable capacity integer. Dropping connection...", "error", err)
		return
	}

	s.m.Logger().Debug("[WorkerConnServer][ServeHTTP] Starting Hijacking...", "worker_id", id, "remote", r.RemoteAddr)

	cooper.Hijack(
		// =====================================================================
		// Handler
		// =====================================================================
		func(conn net.Conn, proto string) {
			defer conn.Close()

			s.m.Logger().Debug("[WorkerConnServer][Hijacked] Client completed handshake", "worker_id", id, "remote", conn.RemoteAddr())

			workerConn, err := s.m.workers.NewWorkerConnection(id, os, arch, int(capacity), conn)
			if err != nil {
				s.m.Logger().Error("[WorkerConnServer][Hijacked] Failed to create a new worker connection. Dropping connection...", "error", err)
				return
			}

			s.m.NotifyDispatcher()

			workerConnLogger := s.m.Logger().With("worker_id", id, "remote", workerConn.conn.RemoteAddr())

			workerConnLogger.Debug("[WorkerConnServer][Hijacked] Starting to serve worker connection...")
			s.ServeWorkerConnection(context.Background(), workerConnLogger, workerConn, proto)
			workerConnLogger.Debug("[WorkerConnServer][Hijacked] Worker connection serve stopped...")

			s.m.workers.unregister(workerConn)

			workerConnLogger.Debug("[WorkerConnServer][Hijacked] Unregisterd worker")
		},

		// =====================================================================
		// Accepted Proto's
		// =====================================================================
		cooper.Protocols("workforce/0"),

		// =====================================================================
		// Workforce Accept negotiation
		// =====================================================================
		cooper.ResponseHeaders(func(r *http.Request, proto string) http.Header {
			header := make(http.Header)

			acceptKey := contract.GenerateWorkforceAccept(r.Header.Get(contract.ProtoMagicKeyHeaderName))
			header.Add(contract.ProtoMagicAcceptHeaderName, acceptKey)

			slog.Debug("[Worker][ConnectAndWork] Generated Accept key for handshake", "key", acceptKey)

			return header
		}),

		// =====================================================================
		// On handshake error
		// =====================================================================
		cooper.OnError(func(err error) {
			s.m.Logger().Error("[WorkerConnServer][ServeHTTP] Failed to complete handshake with client", "error", err, "worker_id", id, "remote", r.RemoteAddr)
		}),
	).ServeHTTP(w, r)
}

func (s *WorkerConnServer) ServeWorkerConnection(ctx context.Context, l *slog.Logger, w *WorkerConn, proto string) {
	l.Info("[WorkerConnServer] Worker connected", "os", w.os, "arch", w.arch, "capacity", w.capacity)

	versionIntString := strings.TrimPrefix(proto, "workforce/")
	versionInt, err := strconv.ParseInt(versionIntString, 10, 16)
	if err != nil {
		l.Error("[WorkerConnServer] Worker specified unparsable workforce version number. Dropping connection...", "proto", proto, "error", err)
		return
	}

	worforceProtocolVersion := verreg.Version(versionInt)

	reader := netargv.NewReader(w.conn)
	for msg, err := range reader.Itterate(ctx) {
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean inbetween disconnect from worker, ignoring and breaking the read loop
				break
			}

			l.Error("[WorkerConnServer] Error while reading messages from worker. Mostlikely a desynced connection. Dropping connection...", "error", err)
			return
		}

		factory, err := s.m.messageVerreg.Resolve(worforceProtocolVersion, msg.Verb())
		if err != nil {
			w.SendError("unknown_verb", "unknown message verb: "+msg.Verb())
			continue
		}

		message, err := factory(msg)
		if err != nil {
			w.SendError("invalid_message", err.Error())
			continue
		}

		go s.handleWorkerMessage(ctx, l, w, message)
	}

	s.m.Logger().Info("[WorkerConnServer] Worker disconnected")
}

func (s *WorkerConnServer) handleWorkerMessage(ctx context.Context, l *slog.Logger, w *WorkerConn, message contract.Message) {
	if ctx.Err() != nil {
		s.m.Logger().Error("[WorkerConnServer][handleWorkerMessage] Context already canceld")
		return
	}

	switch msg := message.(type) {
	case *contract.HeartbeatMessage:
		w.UpdateHeartbeat()
		if err := w.Send("heartbeat"); err != nil {
			l.Error("[WorkerConnServer][handleWorkerMessage] Failed to reply heartbeat to worker")
			// TODO: Decide if this is reason to drop the connection or not
		}

		return

	case *contract.ResultMessage:
		s.handleResultMessage(ctx, l, w, msg)
		return

	case *contract.AcceptMessage:
		s.handleAcceptMessage(ctx, l, w, msg)
		return

	case *contract.RejectMessage:
		s.handleRejectMessage(ctx, l, w, msg)
		return

	case *contract.LogMessage:
		s.handleLogMessage(ctx, l, w, msg)
		return

	case *contract.StagedMessage:
		s.handleStagedMessage(ctx, l, w, msg)
		return

	case *contract.StartingMessage:
		s.handleStartingMessage(ctx, l, w, msg)
		return

	case *contract.CapacityMessage:
		l.Info("[WorkerConnServer][handleCapacityMessage] Worker updated its capacity")
		w.OverrideCapacity(msg.Available)
		return

	default:
		w.SendError("unhandled_message", fmt.Sprintf("message of type %T is not handled", message))
		s.m.Logger().Warn("[WorkerConnServer][handleWorkerMessage] Received unexpected message type", "type", fmt.Sprintf("%T", msg))
	}
}

// #####################################################################
//   					   Message handlers
// #####################################################################

func (s *WorkerConnServer) handleAcceptMessage(ctx context.Context, l *slog.Logger, w *WorkerConn, msg *contract.AcceptMessage) {
	l.Info("[WorkerConnServer][handleAcceptMessage] Worker accepted job proposal", "job_id", msg.JobID)

	err := s.m.JobStore().UpdateJob(ctx, msg.JobID, func(j *contract.Job) {
		j.Status = contract.JobStatusAccepted
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleAcceptMessage] Failed to update job", "job_id", msg.JobID, "error", err)
		return
	}

	job, err := s.m.JobStore().GetJob(ctx, msg.JobID)
	if err != nil {
		l.Error("[WorkerConnServer][handleAcceptMessage] Failed to find job", "job_id", msg.JobID, "error", err)

		return
	}

	if err := w.SendWithPayload(contract.FormulateDispatchV0Message(job), job.Payload); err != nil {
		l.Error("[WorkerConnServer][handleAcceptMessage] Failed to write dispatch message to worker", "job_id", msg.JobID, "error", err)
		return
	}
}

func (s *WorkerConnServer) handleStagedMessage(ctx context.Context, l *slog.Logger, w *WorkerConn, msg *contract.StagedMessage) {
	l.Info("[WorkerConnServer][handleAcceptMessage] Worker staged job", "job_id", msg.JobID)

	err := s.m.JobStore().UpdateJob(ctx, msg.JobID, func(j *contract.Job) {
		j.Status = contract.JobStatusProvisioning
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleStagedMessage] Failed to update job", "job_id", msg.JobID, "error", err)
		return
	}
}

func (s *WorkerConnServer) handleRejectMessage(ctx context.Context, l *slog.Logger, w *WorkerConn, msg *contract.RejectMessage) {
	l.Info("[WorkerConnServer][handleRejectMessage] Worker rejected job proposal", "job_id", msg.JobID, "reason", msg.Reason)

	job, err := s.m.JobStore().GetJob(ctx, msg.JobID)
	if err != nil {
		l.Error("[WorkerConnServer][handleRejectMessage] Failed to find job", "job_id", msg.JobID, "error", err)
		w.SendError("invalid_argument", "job("+msg.JobID+") not found")

		return
	}

	// Reset job to Pending and re-queue.
	err = s.m.JobStore().UpdateJob(ctx, msg.JobID, func(j *contract.Job) {
		j.Status = contract.JobStatusPending
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleRejectMessage] Failed to update job", "job_id", msg.JobID, "error", err)
		return
	}

	if err = s.m.workers.restoreCapacity(ctx, w, job.ID, job.Cost); err != nil {
		l.Error("[WorkerConnServer][handleRejectMessage] Failed to restore worker capacity", "error", err)
	}

	// TODO: Keep record (for a certain TTL) that this worker did reject the job proposal if its reason is not draining
	// TODO: So we do not dispatch it to this worker again in the next round

	job.Status = contract.JobStatusPending
	s.m.EnqueueJob(job)
}

func (s *WorkerConnServer) handleStartingMessage(ctx context.Context, l *slog.Logger, w *WorkerConn, msg *contract.StartingMessage) {
	l.Info("[WorkerConnServer][handleStartingMessage] Worker started job", "job_id", msg.JobID)

	job, err := s.m.JobStore().GetJob(ctx, msg.JobID)
	if err != nil {
		l.Error("[WorkerConnServer][handleStartingMessage] Failed to find job", "job_id", msg.JobID, "error", err)

		return
	}

	err = s.m.JobStore().UpdateJob(ctx, job.ID, func(j *contract.Job) {
		j.Status = contract.JobStatusRunning
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleStartingMessage] Failed to update job", "job_id", msg.JobID, "error", err)
	}

	if s.m.WebhookDispatcher() != nil {
		s.m.WebhookDispatcher().FireJobRunning(ctx, job, w.workerID)
	}
}

func (s *WorkerConnServer) handleResultMessage(ctx context.Context, l *slog.Logger, w *WorkerConn, msg *contract.ResultMessage) {
	l.Info("[WorkerConnServer][handleRejectMessage] Worker returned job result", "job_id", msg.JobID, "type", msg.Type)

	job, err := s.m.JobStore().GetJob(ctx, msg.JobID)
	if err != nil {
		l.Error("[WorkerConnServer][handleResultMessage] Failed to find job", "job_id", msg.JobID, "error", err)
		w.SendError("invalid_argument", "job("+msg.JobID+") not found")

		return
	}

	if msg.Type == contract.ResultError {
		s.handleJobError(ctx, l, w, job, msg)
		return
	}

	if msg.Type == contract.ResultSubjobs {
		// The Job emitted child jobs to be ran. The worker is finished at this point,
		// so restore its capacity regardless of whether subjob handling succeeds.
		subjobErr := s.handleSubjobsEmitted(ctx, l, job, msg.Jobs)
		if err := s.m.workers.restoreCapacity(ctx, w, job.ID, job.Cost); err != nil {
			l.Error("[WorkerConnServer][handleResultMessage] Failed to restore worker capacity after subjobs", "job_id", job.ID, "error", err)
		}
		if subjobErr != nil {
			w.SendError("", subjobErr.Error())
			return
		}
	}

	if msg.Type == contract.ResultSuccess {
		s.handleJobCompleted(ctx, l, w, job, msg)
	}
}

// handleSubjobsEmitted is called when a worker sends TypeSubjobsEmitted.
// It creates child Job records, marks the parent as AwaitingChildren, and
// enqueues the children for dispatch.
// TODO: change the signature to match the other and do not emit errors from here
func (s *WorkerConnServer) handleSubjobsEmitted(ctx context.Context, l *slog.Logger, parent *contract.Job, childJobs []contract.ChildJobRequest) error {
	l.Info("[WorkerConnServer][handleSubjobsEmitted] Worker emitted sub jobs", "job_id", parent.ID)

	if parent.Status.IsTerminal() {
		// TODO: feels like that descicion should not be made here
		return nil // race with cancellation; drop silently
	}

	now := time.Now()
	children := make([]*contract.Job, 0, len(childJobs))

	for i, req := range childJobs {
		taskDef, err := s.m.TaskStore().GetTask(ctx, req.Task)
		if err != nil {
			l.Error("[WorkerConnServer][handleSubjobsEmitted] Emitted child job targets unkown task", "task_name", req.Task, "parent_job_id", parent.ID, "error", err)

			// TODO: Add a sential for this specific case
			return errors.Join(err, s.m.JobStore().UpdateJob(ctx, parent.ID, func(j *contract.Job) {
				j.Status = contract.JobStatusFailed
				j.FailureReason = "child job targets unknown task: " + req.Task
				j.UpdatedAt = time.Now()
			}))
		}

		var artifactVersion string
		if s.m.ArtifactRegistry() != nil {
			if req.Version != "" {
				// Validate the version exists by checking available platforms.
				platforms, err := s.m.ArtifactRegistry().ListPlatforms(ctx, taskDef.Name, req.Version)
				if err != nil || len(platforms) == 0 {
					l.Error("[WorkerConnServer][handleSubjobsEmitted] Failed to find child artifact version not found", "task", taskDef.Name, "version", req.Version)

					return errors.Join(err, s.m.JobStore().UpdateJob(ctx, parent.ID, func(j *contract.Job) {
						j.Status = contract.JobStatusFailed
						j.FailureReason = "child artifact " + req.Task + "@" + req.Version + " not found"
						j.UpdatedAt = time.Now()
					}))
				}

				artifactVersion = req.Version
			} else {
				// Resolve latest to pin the version for the child job.
				platforms, err := s.m.ArtifactRegistry().ListPlatforms(ctx, taskDef.Name, "")
				if err != nil || len(platforms) == 0 {
					l.Error("[WorkerConnServer][handleSubjobsEmitted] No released artifact for child task", "task", taskDef.Name)

					return errors.Join(err, s.m.JobStore().UpdateJob(ctx, parent.ID, func(j *contract.Job) {
						j.Status = contract.JobStatusFailed
						j.FailureReason = "no released artifact for " + taskDef.Name
						j.UpdatedAt = time.Now()
					}))
				}

				artifactVersion = platforms[0].Version
			}
		}

		child := &contract.Job{
			ID:              ulid.Make().String(),
			ParentJobID:     parent.ID,
			TaskName:        taskDef.Name,
			ArtifactVersion: artifactVersion,
			Status:          contract.JobStatusPending,
			Phase:           "run",
			Priority:        req.Priority,
			Cost:            taskDef.Cost,
			Payload:         req.Payload,
			CreatedAt:       now.Add(time.Duration(i)),
			UpdatedAt:       now,
		}

		if err := s.m.JobStore().SaveJob(ctx, child); err != nil {
			l.Error("[WorkerConnServer][handleSubjobsEmitted] Failed to save child job", "parent_job_id", parent.ID, "error", err)

			return errors.Join(err, s.m.JobStore().UpdateJob(ctx, parent.ID, func(j *contract.Job) {
				j.Status = contract.JobStatusFailed
				j.FailureReason = fmt.Sprintf("internal error saving child jobs: %s", err.Error())
				j.UpdatedAt = time.Now()
			}))
		}

		children = append(children, child)
	}

	// Transition parent to AwaitingChildren.
	err := s.m.JobStore().UpdateJob(ctx, parent.ID, func(j *contract.Job) {
		j.Status = contract.JobStatusAwaitingChildren
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleSubjobsEmitted] Failed to mark parent awaiting children", "parent_job_id", parent.ID, "error", err)
		return nil
	}

	// Enqueue children for dispatch.
	s.m.EnqueueJobs(children)

	return nil
}

func (s *WorkerConnServer) handleJobCompleted(ctx context.Context, l *slog.Logger, w *WorkerConn, job *contract.Job, msg *contract.ResultMessage) {
	if job.Status.IsTerminal() {
		l.Warn("[WorkerConnServer][handleJobCompleted] Job already terminal — ignoring duplicate", "job_id", job.ID, "status", job.Status)
		return
	}

	err := s.m.JobStore().UpdateJob(ctx, msg.JobID, func(j *contract.Job) {
		j.Status = contract.JobStatusCompleted
		j.Result = msg.Payload
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleJobCompleted] Failed to update job", "job_id", msg.JobID, "error", err)
		return
	}

	if err := s.m.workers.restoreCapacity(ctx, w, job.ID, job.Cost); err != nil {
		l.Error("[WorkerConnServer][handleJobCompleted] Failed to restore worker capacity", "error", err)
		return
	}

	if s.m.WebhookDispatcher() != nil {
		job.Status = contract.JobStatusCompleted
		s.m.WebhookDispatcher().FireJobCompleted(ctx, job)
	}

	// =====================================================================
	// Handling consolidation scheduling if this is the last of the children to finish
	// =====================================================================

	if job.ParentJobID == "" {
		return
	}

	siblings, err := s.m.JobStore().ListChildJobs(ctx, job.ParentJobID)
	if err != nil {
		l.Error("[WorkerConnServer][handleJobCompleted] Failed to list siblings", "parent_job_id", job.ParentJobID, "error", err)

		return
	}

	for _, s := range siblings {
		if !s.Status.IsTerminal() {
			return // siblings still in progress
		}

		if s.Status != contract.JobStatusCompleted {
			return // sibling failed. failure handler will deal with parent
		}
	}

	// All siblings completed — trigger consolidation.
	parent, err := s.m.JobStore().GetJob(ctx, job.ParentJobID)
	if err != nil || parent.Status != contract.JobStatusAwaitingChildren {
		return
	}

	// Build the ConsolidatePayload: original payload + ordered child results.
	consolidatePayload, err := buildConsolidatePayload(parent, siblings)
	if err != nil {
		l.Error("[WorkerConnServer][handleJobCompleted] Failed to build consolidation payload", "parent_job_id", parent.ID, "error", err)
		return
	}

	// Reset job to Pending and re-queue.
	err = s.m.JobStore().UpdateJob(ctx, parent.ID, func(j *contract.Job) {
		j.Status = contract.JobStatusPending
		j.Phase = "consolidate"
		j.Payload = consolidatePayload
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleJobCompleted] Failed to update job", "job_id", msg.JobID, "error", err)
		return
	}

	parent.Status = contract.JobStatusPending
	parent.Phase = contract.JobPhaseConsolidate
	parent.Payload = consolidatePayload

	s.m.EnqueueJob(parent)
}

func (s *WorkerConnServer) handleJobError(ctx context.Context, l *slog.Logger, w *WorkerConn, job *contract.Job, msg *contract.ResultMessage) {
	if job.Status.IsTerminal() {
		return
	}

	// Restore worker capacity for the failed job.
	if err := s.m.workers.restoreCapacity(ctx, w, job.ID, job.Cost); err != nil {
		l.Error("[WorkerConnServer][handleJobError] Failed to restore worker capacity", "error", err)
	}

	job.Attempts += 1
	err := s.m.JobStore().UpdateJob(ctx, job.ID, func(j *contract.Job) {
		j.Attempts = job.Attempts
		j.FailureReason = msg.Reason
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleJobError] Failed to update job attempts", "job_id", job.ID, "error", err)
	}

	if msg.ExitCode == int(s.m.cfg.DefaultPanicExitCode) {
		s.handleFailJob(ctx, l, job, msg)
		return
	}

	if msg.ExitCode >= int(s.m.cfg.DefaultDoNotRetryExitCodeRange.From) && msg.ExitCode <= int(s.m.cfg.DefaultDoNotRetryExitCodeRange.To) {
		s.handleFailJob(ctx, l, job, msg)
		return
	}

	// Resolve effective retry policy from task definition.
	effective := s.m.cfg.DefaultRetryPolicy
	if taskDef, err := s.m.TaskStore().GetTask(ctx, job.TaskName); err == nil {
		effective = mergeRetryPolicy(taskDef.RetryPolicy, s.m.cfg.DefaultRetryPolicy)
	}

	if job.Attempts < effective.MaxAttempts {
		delay := retryDelay(effective, job.Attempts)
		l.Info("[WorkerConnServer][handleJobError] Scheduling retry for failed job", "job_id", job.ID, "attempts", job.Attempts, "delay", delay)

		// TODO: im not liking this whole time.AfterFunc nonsens.
		// TODO: it would work, since AfterFunc spawns a new goroutine for each invokation.
		// TODO: but in scenario's where there are many falures this could spawn a ton of waiting routines
		// TODO: Probably we just want to have the queue have some dispatch time field
		// TODO: Then it could be added to the queue directly
		// TODO: and then it would get picked up in the next dispatch round
		time.AfterFunc(delay, func() {
			j, _ := s.m.JobStore().GetJob(ctx, job.ID)
			if j == nil || j.Status.IsTerminal() {
				return
			}

			err = s.m.JobStore().UpdateJob(ctx, job.ID, func(jk *contract.Job) {
				jk.Status = contract.JobStatusPending
				jk.UpdatedAt = time.Now()
			})

			if err != nil {
				l.Error("[WorkerConnServer][handleJobError] Failed to update job status", "job_id", job.ID, "error", err)
			}

			j.Status = contract.JobStatusPending
			s.m.EnqueueJob(j)
		})

		return
	}

	s.handleFailJob(ctx, l, job, msg)
}

func (s *WorkerConnServer) handleFailJob(ctx context.Context, l *slog.Logger, job *contract.Job, msg *contract.ResultMessage) {
	// Permanently failed.
	err := s.m.JobStore().UpdateJob(ctx, job.ID, func(j *contract.Job) {
		j.Status = contract.JobStatusFailed
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleJobError] Failed to mark job failed", "job_id", job.ID, "error", err)
	}

	if s.m.WebhookDispatcher() != nil {
		job.Status = contract.JobStatusFailed
		s.m.WebhookDispatcher().FireJobFailed(ctx, job, msg.Reason)
	}

	if job.ParentJobID == "" {
		return
	}

	// If this is a child job, fail the parent chain.

	parent, err := s.m.JobStore().GetJob(ctx, job.ParentJobID)
	if err != nil {
		l.Error("[WorkerConnServer][handleJobError] Failed to find parent job", "parent_job_id", job.ParentJobID, "error", err)
		return
	}

	err = s.m.JobStore().UpdateJob(ctx, parent.ID, func(j *contract.Job) {
		j.Status = contract.JobStatusFailed
		j.FailureReason = msg.Reason
		j.UpdatedAt = time.Now()
	})

	if err != nil {
		l.Error("[WorkerConnServer][handleJobError] Failed to update job", "job_id", parent.ID, "error", err)
		return
	}

	if s.m.WebhookDispatcher() != nil {
		s.m.WebhookDispatcher().FireJobFailed(ctx, job, msg.Reason)
	}

}

func (s *WorkerConnServer) handleLogMessage(ctx context.Context, l *slog.Logger, w *WorkerConn, msg *contract.LogMessage) {
	l.Debug("[WorkerConnServer][handleLogMessage] Worker provided log line", "job_id", msg.JobID)

	if err := s.m.LogStore().AppendJobLog(ctx, msg.JobID, msg.Line); err != nil {
		l.Error("[WorkerConnServer][handleLogMessage] Failed to store log line submission", "job_id", msg.JobID, "error", err)
	}
}

// #####################################################################
//              Utilities
// #####################################################################

func mergeRetryPolicy(task, defaults contract.RetryPolicy) contract.RetryPolicy {
	out := task
	if out.MaxAttempts == 0 {
		out.MaxAttempts = defaults.MaxAttempts
	}
	if out.RetryDelay == 0 {
		out.RetryDelay = defaults.RetryDelay
	}
	if out.MaxRetryDelay == 0 {
		out.MaxRetryDelay = defaults.MaxRetryDelay
	}
	return out
}

func retryDelay(p contract.RetryPolicy, attempt int) time.Duration {
	if p.RetryDelay == 0 {
		return 0
	}
	delay := p.RetryDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
	}
	if p.MaxRetryDelay > 0 && delay > p.MaxRetryDelay {
		delay = p.MaxRetryDelay
	}
	return delay
}
