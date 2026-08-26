package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"lowbit.dev/muxx"
	"lowbit.dev/problemjson"
	"lowbit.dev/ulid"
	"lowbit.dev/urlsign"
	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/manager/artifact"
)

// buildMux wires all HTTP routes onto a new ServeMux and returns it.
func (m *Manager) buildMux() http.Handler {
	mux := muxx.New()

	// =====================================================================
	// Simple system health
	// =====================================================================
	healthRL := NewHTTPLimiterCollection(func() *Limiter { return NewLimiter(2, 20) }, RemoteAddrKeyFunc)

	mux.Handle("GET /health", healthRL.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO: maybe we should do some actual introspection here in the future
		w.WriteHeader(200)
		w.Write([]byte(`{"status": true}`))
	})))

	// =====================================================================
	// Worker Connections
	// =====================================================================
	workerConnectRL := NewHTTPLimiterCollection(func() *Limiter { return NewLimiter(1, 10) }, RemoteAddrKeyFunc)

	mux.Handle("POST "+m.cfg.WorkerConnectPath, muxx.Chain(
		NewWorkerConnServer(m),
		workerConnectRL.Middleware,
	))

	// =====================================================================
	// Client authenticated API Surface
	// =====================================================================
	clientRL := NewHTTPLimiterCollection(func() *Limiter { return NewLimiter(10, 30) }, RemoteAddrKeyFunc)

	clientAuth := mux.Group("").Use(m.clientAuth, clientRL.Middleware)

	// ---------------------------------------------------------------------
	// Manager
	// ---------------------------------------------------------------------
	clientAuth.HandleFunc("GET /manager/log", func(w http.ResponseWriter, r *http.Request) {})
	clientAuth.HandleFunc("GET /manager/log/stream", func(w http.ResponseWriter, r *http.Request) {})

	// ---------------------------------------------------------------------
	// Workers
	// ---------------------------------------------------------------------
	clientAuth.HandleFunc("GET /workers", m.handleListWorkers)
	clientAuth.HandleFunc("GET /workers/{id}", m.handleGetWorker)
	clientAuth.HandleFunc("POST /workers/{id}/drain", m.handleDrainWorker)
	clientAuth.HandleFunc("DELETE /workers/{id}", m.handleDisconnectWorker)

	// ---------------------------------------------------------------------
	// Cluster
	// ---------------------------------------------------------------------
	clientAuth.HandleFunc("GET /cluster", m.handleCluster)
	clientAuth.HandleFunc("GET /cluster/queue", m.handleClusterQueue)
	clientAuth.HandleFunc("GET /cluster/metrics", m.handleClusterMetrics)

	// ---------------------------------------------------------------------
	// Jobs
	// ---------------------------------------------------------------------
	clientAuth.HandleFunc("GET /jobs", m.handleListJobs)
	clientAuth.HandleFunc("POST /jobs", m.handleSubmitJob)
	clientAuth.HandleFunc("GET /jobs/{id}", m.handleGetJob)
	clientAuth.HandleFunc("DELETE /jobs/{id}", m.handleCancelJob)
	clientAuth.HandleFunc("GET /jobs/{id}/logs", m.handleJobLogs)
	clientAuth.HandleFunc("GET /jobs/{id}/logs/stream", m.handleJobLogsStream)
	clientAuth.HandleFunc("GET /jobs/{id}/runs", m.handleListJobRuns)
	clientAuth.HandleFunc("GET /jobs/{id}/runs/{runID}/logs", m.handleRunLogs)
	clientAuth.HandleFunc("GET /jobs/{id}/runs/{runID}/logs/stream", m.handleRunLogsStream)
	clientAuth.HandleFunc("GET /jobs/{id}/children", m.handleListChildJobs)

	// ---------------------------------------------------------------------
	// Tasks
	// ---------------------------------------------------------------------
	clientAuth.HandleFunc("GET /tasks", m.handleListTaskDefs)
	clientAuth.HandleFunc("POST /tasks", m.handleCreateTaskDef)
	clientAuth.HandleFunc("GET /tasks/{name}", m.handleGetTaskDef)
	clientAuth.HandleFunc("PUT /tasks/{name}", m.handleUpdateTaskDef)
	clientAuth.HandleFunc("DELETE /tasks/{name}", m.handleDeleteTaskDef)

	clientAuth.HandleFunc("POST /tasks/{name}/artifacts", m.handleTaskArtifactUpload)
	clientAuth.HandleFunc("GET /tasks/{name}/artifacts", m.handleTaskListArtifactVersions)
	clientAuth.HandleFunc("GET /tasks/{name}/artifacts/latest", m.handleTaskArtifactLatest)

	// Task artifact download — protected by signed URL when ArtifactSigningKey is configured.
	mux.Handle("GET /tasks/{name}/artifacts/{version}/{os}/{arch}", muxx.Chain(
		http.HandlerFunc(m.handleTaskArtifactDownload),
		m.signedArtifactMiddleware,
		clientRL.Middleware,
	))

	return mux
}

// ---- middleware ----

func (m *Manager) clientAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.cfg.ClientAuthFunc != nil {
			if err := m.cfg.ClientAuthFunc(r); err != nil {
				problemjson.Unauthorized(problemjson.Error(err)).ServeHTTP(w, r)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// signedArtifactMiddleware validates the ?exp and ?sig query parameters on artifact
// download requests when ArtifactSigningKey is configured. If no signing key is set,
// the middleware is a no-op and requests pass through unchanged (backward-compatible).
func (m *Manager) signedArtifactMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.urlSigner == nil {
			next.ServeHTTP(w, r)
			return
		}

		verifiable := m.cfg.Domain + r.URL.String()
		m.Logger().Debug("[Manager][HTTP][signedArtifactMiddleware] Verifying signed url", "url", verifiable)

		if err := m.urlSigner.Verify(verifiable); err != nil {
			detail := "You are unauthorized to access this resource"
			if errors.Is(err, urlsign.ErrExpired) {
				detail = "The URL has expired"
			} else if errors.Is(err, urlsign.ErrInvalidSig) {
				detail = "The provided signature is invalid"
			} else if errors.Is(err, urlsign.ErrMissingParams) {
				detail = "This resource requires a valid signature to access"
			}

			m.Logger().Debug("[Manager][HTTP][signedArtifactMiddleware] URL invalid", "error", err)

			problemjson.Unauthorized(problemjson.Detail(detail)).ServeHTTP(w, r)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ---- /workers ----

type workerInfo struct {
	WorkerID          string `json:"worker_id"`
	OS                string `json:"os"`
	Arch              string `json:"arch"`
	Capacity          int    `json:"capacity"`
	AvailableCapacity int    `json:"available_capacity"`
	State             string `json:"state"`
	ConnectedAt       string `json:"connected_at"`
	TasksRunning      int    `json:"tasks_running"`
}

func (m *Manager) handleListWorkers(w http.ResponseWriter, _ *http.Request) {
	workers := m.workers.allWorkers()
	out := make([]workerInfo, 0, len(workers))

	for _, wc := range workers {
		out = append(out, workerInfo{
			WorkerID:          wc.workerID,
			OS:                wc.os,
			Arch:              wc.arch,
			Capacity:          wc.capacity,
			AvailableCapacity: int(wc.AvailableCapacity()),
			State:             wc.State().String(),
			ConnectedAt:       wc.connectedAt.UTC().Format(time.RFC3339),

			// TODO: Add tasksRunning capability to the worker conn
			// TasksRunning:      int(wc.tasksRunning),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type workerDetailInfo struct {
	WorkerID          string   `json:"worker_id"`
	OS                string   `json:"os"`
	Arch              string   `json:"arch"`
	State             string   `json:"state"`
	Capacity          int      `json:"capacity"`
	AvailableCapacity int      `json:"available_capacity"`
	TasksRunning      int      `json:"tasks_running"`
	InFlightJobIDs    []string `json:"in_flight_job_ids"`
	ConnectedAt       string   `json:"connected_at"`
	LastHeartbeatAt   string   `json:"last_heartbeat_at"`
	LastIdleAt        string   `json:"last_idle_at"`
}

func (m *Manager) handleGetWorker(w http.ResponseWriter, r *http.Request) {
	worker, ok := m.workers.GetWorker(r.PathValue("id"))
	if !ok {
		problemjson.NotFound(problemjson.Detail("worker not found")).ServeHTTP(w, r)
		return
	}

	writeJSON(w, http.StatusOK, workerDetailInfo{
		WorkerID:          worker.workerID,
		OS:                worker.os,
		Arch:              worker.arch,
		State:             worker.State().String(),
		Capacity:          worker.capacity,
		AvailableCapacity: worker.AvailableCapacity(),
		// TasksRunning:      int(wc.tasksRunning.Load()),
		InFlightJobIDs:  worker.inFlightIDs(),
		ConnectedAt:     worker.connectedAt.UTC().Format(time.RFC3339),
		LastHeartbeatAt: worker.LastHeartbeat().UTC().Format(time.RFC3339),
		LastIdleAt:      worker.LastIdleTime().UTC().Format(time.RFC3339),
	})
}

func (m *Manager) handleDrainWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := m.DrainWorker(id); err != nil {
		problemjson.NotFound(problemjson.Error(err)).ServeHTTP(w, r)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (m *Manager) handleDisconnectWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := m.DisconnectWorker(id); err != nil {
		problemjson.NotFound(problemjson.Error(err)).ServeHTTP(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- /cluster ----

type clusterWorkerCounts struct {
	Total     int `json:"total"`
	Online    int `json:"online"`
	Pressured int `json:"pressured"`
	Draining  int `json:"draining"`
	Offline   int `json:"offline"`
}

type clusterCapacity struct {
	Total       int      `json:"total"`
	Available   int      `json:"available"`
	Utilization *float64 `json:"utilization,omitempty"`
}

type clusterPlatformSummary struct {
	OS                string `json:"os"`
	Arch              string `json:"arch"`
	Workers           int    `json:"workers"`
	TotalCapacity     int    `json:"total_capacity"`
	AvailableCapacity int    `json:"available_capacity"`
}

type clusterQueue struct {
	Depth int `json:"depth"`
}

type clusterSummaryResponse struct {
	Workers   clusterWorkerCounts      `json:"workers"`
	Capacity  clusterCapacity          `json:"capacity"`
	Queue     clusterQueue             `json:"queue"`
	Platforms []clusterPlatformSummary `json:"platforms"`
}

func (m *Manager) handleCluster(w http.ResponseWriter, r *http.Request) {
	workers := m.workers.allWorkers()

	var counts clusterWorkerCounts
	totalCap, availCap := 0, 0
	platformMap := make(map[string]*clusterPlatformSummary)

	for _, wc := range workers {
		counts.Total++
		switch wc.State() {
		case contract.WorkerStateOnline:
			counts.Online++
		case contract.WorkerStatePressure:
			counts.Pressured++
		case contract.WorkerStateDraining:
			counts.Draining++
		case contract.WorkerStateOffline:
			counts.Offline++
		}

		tc := wc.capacity
		ac := int(wc.AvailableCapacity())
		totalCap += tc

		if wc.state == contract.WorkerStateOnline {
			availCap += ac
		}

		key := platformKey(wc.os, wc.arch)
		if platformMap[key] == nil {
			platformMap[key] = &clusterPlatformSummary{OS: wc.os, Arch: wc.arch}
		}

		platformMap[key].Workers++
		platformMap[key].TotalCapacity += tc
		platformMap[key].AvailableCapacity += ac
	}

	cap := clusterCapacity{Total: totalCap, Available: availCap}
	if totalCap > 0 {
		u := 1 - float64(availCap)/float64(totalCap)
		cap.Utilization = &u
	}

	platforms := make([]clusterPlatformSummary, 0, len(platformMap))
	for _, p := range platformMap {
		platforms = append(platforms, *p)
	}

	queueDepth := m.queue.Size()

	writeJSON(w, http.StatusOK, clusterSummaryResponse{
		Workers:   counts,
		Capacity:  cap,
		Queue:     clusterQueue{Depth: queueDepth},
		Platforms: platforms,
	})
}

func (m *Manager) handleClusterQueue(w http.ResponseWriter, r *http.Request) {
	snap := m.QueueInfo(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{
		"depth":       snap.Depth,
		"by_platform": snap.ByPlatform,
		"by_priority": snap.ByPriority,
	})
}

func (m *Manager) handleClusterMetrics(w http.ResponseWriter, r *http.Request) {
	jobCounts, err := m.JobStore().CountJobsByStatus(r.Context())
	if err != nil {
		problemjson.InternalServerError(problemjson.Error(err)).ServeHTTP(w, r)
		return
	}

	workers := m.workers.allWorkers()
	totalCap, availCap := 0, 0
	for _, wc := range workers {
		totalCap += wc.capacity
		availCap += wc.AvailableCapacity()
	}

	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"jobs": map[string]int{
			"pending":           jobCounts[contract.JobStatusPending],
			"proposing":         jobCounts[contract.JobStatusProposing],
			"provisioning":      jobCounts[contract.JobStatusProvisioning],
			"running":           jobCounts[contract.JobStatusRunning],
			"awaiting_children": jobCounts[contract.JobStatusAwaitingChildren],
			"completed":         jobCounts[contract.JobStatusCompleted],
			"failed":            jobCounts[contract.JobStatusFailed],
			"cancelled":         jobCounts[contract.JobStatusCancelled],
		},
		"workers":     map[string]int{"connected": len(workers)},
		"capacity":    map[string]int{"total": totalCap, "available": availCap},
		"queue_depth": m.queue.Size(),
	})
}

// ---- /jobs ----

type submitJobRequest struct {
	Task    string               `json:"task"`
	Payload contract.JsonOrBytes `json:"payload"`
	Webhook *webhookConfig       `json:"webhook,omitempty"`
}

type webhookConfig struct {
	URL     string            `json:"url"`
	Events  []string          `json:"events,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (m *Manager) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		problemjson.BadRequest(problemjson.Error(err)).ServeHTTP(w, r)
		return
	}

	if req.Task == "" {
		problemjson.BadRequest(problemjson.Detail("task name is required")).ServeHTTP(w, r)
		return
	}

	taskDef, err := m.TaskStore().GetTask(r.Context(), req.Task)
	if err != nil {
		problemjson.NotFound(problemjson.Detail("unknown task: "+req.Task)).ServeHTTP(w, r)
		return
	}

	now := time.Now()
	job := &contract.Job{
		ID:        ulid.Make().String(),
		TaskName:  req.Task,
		Status:    contract.JobStatusPending,
		Phase:     "run",
		Cost:      taskDef.Cost,
		Payload:   req.Payload,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if req.Webhook != nil && req.Webhook.URL != "" {
		job.WebhookURL = req.Webhook.URL
		job.WebhookEvents = req.Webhook.Events
		job.WebhookHeaders = sanitizeWebhookHeaders(req.Webhook.Headers)
	}

	if err := m.JobStore().SaveJob(r.Context(), job); err != nil {
		m.cfg.Logger.Error("submit job: save", "error", err)
		problemjson.InternalServerError(problemjson.Detail("failed to save job")).ServeHTTP(w, r)
		return
	}

	m.EnqueueJob(job)
	writeJSON(w, http.StatusAccepted, job)
}

func (m *Manager) handleListJobs(w http.ResponseWriter, r *http.Request) {
	cursor := r.URL.Query().Get("cursor")
	limit := parseLimit(r.URL.Query().Get("limit"), 20)

	jobs, nextCursor, err := m.JobStore().ListJobs(r.Context(), cursor, limit)
	if err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to list jobs")).ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "next_cursor": nextCursor})
}

func (m *Manager) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := m.JobStore().GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		problemjson.NotFound(problemjson.Detail("job not found")).ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (m *Manager) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	job, err := m.JobStore().GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		problemjson.NotFound(problemjson.Detail("job not found")).ServeHTTP(w, r)
		return
	}
	if job.Status.IsTerminal() {
		problemjson.Conflict(problemjson.Detail("job is already in a terminal state: "+string(job.Status))).ServeHTTP(w, r)
		return
	}

	//
	go m.cancelJobByID(context.Background(), r.PathValue("id"))
	w.WriteHeader(http.StatusAccepted)
}

// ---- /jobs/{id}/logs ----

func (m *Manager) handleJobLogs(w http.ResponseWriter, r *http.Request) {
	if _, err := m.JobStore().GetJob(r.Context(), r.PathValue("id")); err != nil {
		problemjson.NotFound(problemjson.Detail("job not found")).ServeHTTP(w, r)
		return
	}
	rc, err := m.LogStore().GetRootJobLogReader(r.Context(), r.PathValue("id"))
	if err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to read logs")).ServeHTTP(w, r)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// ---- /jobs/{id}/logs/stream (SSE) ----

func (m *Manager) handleJobLogsStream(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	job, err := m.JobStore().GetJob(r.Context(), jobID)
	if err != nil {
		problemjson.NotFound(problemjson.Detail("job not found")).ServeHTTP(w, r)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		problemjson.InternalServerError(problemjson.Detail("streaming not supported")).ServeHTTP(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// If already terminal, emit buffered logs and close.
	if job.Status.IsTerminal() {
		rc, _ := m.LogStore().GetRootJobLogReader(r.Context(), jobID)
		if rc != nil {
			emitLogLines(w, jobID, rc)
			rc.Close()
		}
		writeSSEEvent(w, "done", "{}")
		flusher.Flush()
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	ch, err := m.LogStore().SubscribeJobLogs(ctx, jobID)
	if err == nil {
		go func() {
			for chunk := range ch {
				for _, line := range strings.Split(strings.TrimRight(string(chunk), "\n"), "\n") {
					if line == "" {
						continue
					}
					data, _ := json.Marshal(map[string]string{"job_id": jobID, "line": line})
					writeSSEEvent(w, "log", string(data))
					flusher.Flush()
				}
			}
		}()
	}

	<-ctx.Done()
	writeSSEEvent(w, "done", "{}")
	flusher.Flush()
}

// ---- /jobs/{id}/runs ----

func (m *Manager) handleListJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if _, err := m.JobStore().GetJob(r.Context(), jobID); err != nil {
		problemjson.NotFound(problemjson.Detail("job not found")).ServeHTTP(w, r)
		return
	}
	runs, err := m.RunStore().ListJobRuns(r.Context(), jobID)
	if err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to list runs")).ServeHTTP(w, r)
		return
	}
	if runs == nil {
		runs = []*contract.JobRun{}
	}
	writeJSON(w, http.StatusOK, runs)
}

// ---- /jobs/{id}/runs/{runID}/logs ----

func (m *Manager) handleRunLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runID")
	rc, err := m.LogStore().GetRunLogReader(r.Context(), runID)
	if err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to read run logs")).ServeHTTP(w, r)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// ---- /jobs/{id}/runs/{runID}/logs/stream (SSE) ----

func (m *Manager) handleRunLogsStream(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runID")

	flusher, ok := w.(http.Flusher)
	if !ok {
		problemjson.InternalServerError(problemjson.Detail("streaming not supported")).ServeHTTP(w, r)
		return
	}

	run, err := m.RunStore().GetRun(r.Context(), runID)
	if err != nil {
		problemjson.NotFound(problemjson.Detail("run not found")).ServeHTTP(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	if run.Status == contract.RunStatusCompleted || run.Status == contract.RunStatusFailed {
		rc, _ := m.LogStore().GetRunLogReader(r.Context(), runID)
		if rc != nil {
			emitLogLines(w, run.JobID, rc)
			rc.Close()
		}
		writeSSEEvent(w, "done", "{}")
		flusher.Flush()
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	ch, err := m.LogStore().SubscribeRunLogs(ctx, runID)
	if err == nil {
		go func() {
			for chunk := range ch {
				for _, line := range strings.Split(strings.TrimRight(string(chunk), "\n"), "\n") {
					if line == "" {
						continue
					}
					data, _ := json.Marshal(map[string]string{"run_id": runID, "job_id": run.JobID, "line": line})
					writeSSEEvent(w, "log", string(data))
					flusher.Flush()
				}
			}
		}()
	}

	<-ctx.Done()
	writeSSEEvent(w, "done", "{}")
	flusher.Flush()
}

// ---- /jobs/{id}/children ----

func (m *Manager) handleListChildJobs(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if _, err := m.JobStore().GetJob(r.Context(), jobID); err != nil {
		problemjson.NotFound(problemjson.Detail("job not found")).ServeHTTP(w, r)
		return
	}
	children, err := m.JobStore().ListChildJobs(r.Context(), jobID)
	if err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to list child jobs")).ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": children})
}

// ---- /tasks (task definitions) ----

func (m *Manager) handleListTaskDefs(w http.ResponseWriter, r *http.Request) {
	tasks, err := m.TaskStore().ListTasks(r.Context())
	if err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to list tasks")).ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (m *Manager) handleCreateTaskDef(w http.ResponseWriter, r *http.Request) {
	var task contract.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		problemjson.BadRequest(problemjson.Error(err)).ServeHTTP(w, r)
		return
	}
	if task.Name == "" {
		problemjson.BadRequest(problemjson.Detail("task name is required")).ServeHTTP(w, r)
		return
	}
	if err := m.TaskStore().SaveTask(r.Context(), &task); err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to save task")).ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusCreated, task)
}

func (m *Manager) handleGetTaskDef(w http.ResponseWriter, r *http.Request) {
	task, err := m.TaskStore().GetTask(r.Context(), r.PathValue("name"))
	if err != nil {
		problemjson.NotFound(problemjson.Detail("task not found")).ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (m *Manager) handleUpdateTaskDef(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var updates contract.Task
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		problemjson.BadRequest(problemjson.Error(err)).ServeHTTP(w, r)
		return
	}

	if err := m.TaskStore().UpdateTask(r.Context(), name, func(t *contract.Task) {
		t.Limitations = updates.Limitations
		t.RetryPolicy = updates.RetryPolicy
		if updates.Cost != 0 {
			t.Cost = updates.Cost
		}
		t.NoResult = updates.NoResult
	}); err != nil {
		problemjson.NotFound(problemjson.Detail("task not found")).ServeHTTP(w, r)
		return
	}

	task, _ := m.TaskStore().GetTask(r.Context(), name)
	writeJSON(w, http.StatusOK, task)
}

func (m *Manager) handleDeleteTaskDef(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := m.TaskStore().DeleteTask(r.Context(), name); err != nil {
		problemjson.NotFound(problemjson.Detail("task not found")).ServeHTTP(w, r)
		return
	}
	// Cascade: delete all artifact versions for this task.
	if m.ArtifactRegistry() != nil {
		if err := m.ArtifactRegistry().DeleteArtifact(r.Context(), name); err != nil {
			m.cfg.Logger.Warn("delete task: artifact cascade failed", "task", name, "error", err)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- /artifacts (legacy routes, kept for backward compatibility) ----

func (m *Manager) handleArtifactUpload(w http.ResponseWriter, r *http.Request) {
	m.publishArtifact(w, r, r.PathValue("task_type"))
}

func (m *Manager) handleListArtifactVersions(w http.ResponseWriter, r *http.Request) {
	m.listArtifactVersions(w, r, r.PathValue("task_type"))
}

func (m *Manager) handleArtifactLatest(w http.ResponseWriter, r *http.Request) {
	m.artifactLatest(w, r, r.PathValue("task_type"))
}

func (m *Manager) handleArtifactDownload(w http.ResponseWriter, r *http.Request) {
	m.serveArtifact(w, r, r.PathValue("task_type"), r.PathValue("version"), r.PathValue("os"), r.PathValue("arch"))
}

// ---- /tasks/{name}/artifacts (preferred routes) ----

func (m *Manager) handleTaskArtifactUpload(w http.ResponseWriter, r *http.Request) {
	m.publishArtifact(w, r, r.PathValue("name"))
}

func (m *Manager) handleTaskListArtifactVersions(w http.ResponseWriter, r *http.Request) {
	m.listArtifactVersions(w, r, r.PathValue("name"))
}

func (m *Manager) handleTaskArtifactLatest(w http.ResponseWriter, r *http.Request) {
	m.artifactLatest(w, r, r.PathValue("name"))
}

func (m *Manager) handleTaskArtifactDownload(w http.ResponseWriter, r *http.Request) {
	m.serveArtifact(w, r, r.PathValue("name"), r.PathValue("version"), r.PathValue("os"), r.PathValue("arch"))
}

// ---- shared artifact handler implementations ----

// publishArtifact handles multipart artifact upload for a given artifact name (= task name).
// Per-platform dependency lists are supplied as optional "{platform}_deps" form fields
// containing comma-separated binary names (e.g. linux_amd64_deps=ffmpeg,jq).
func (m *Manager) publishArtifact(w http.ResponseWriter, r *http.Request, artifactName string) {
	if m.ArtifactRegistry() == nil {
		problemjson.ServiceUnavailable(problemjson.Detail("artifact registry not configured")).ServeHTTP(w, r)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, m.cfg.MaxArtifactUploadSize)
	if err := r.ParseMultipartForm(2 << 30); err != nil {
		problemjson.BadRequest(problemjson.Error(err)).ServeHTTP(w, r)
		return
	}

	version := r.FormValue("version")
	if version == "" {
		problemjson.BadRequest(problemjson.Detail("version is required")).ServeHTTP(w, r)
		return
	}
	preRelease := r.FormValue("pre_release") == "true"

	// Collect per-platform dependency lists from "{platform}_deps" form values.
	platformDeps := make(map[string][]string)
	for key, vals := range r.MultipartForm.Value {
		if strings.HasSuffix(key, "_deps") && len(vals) > 0 {
			platform := strings.TrimSuffix(key, "_deps")
			var deps []string
			for _, part := range strings.Split(vals[0], ",") {
				if d := strings.TrimSpace(part); d != "" {
					deps = append(deps, d)
				}
			}
			if len(deps) > 0 {
				platformDeps[platform] = deps
			}
		}
	}

	binaries := make(map[string]io.Reader)
	for key, headers := range r.MultipartForm.File {
		// Skip "_deps" metadata fields that happen to be in the file section (shouldn't occur,
		// but guard defensively). Platform keys must not contain a period (not filenames).
		if strings.HasSuffix(key, "_deps") {
			continue
		}
		if strings.Contains(key, "_") {
			f, err := headers[0].Open()
			if err != nil {
				problemjson.BadRequest(problemjson.Detail("failed to open file: "+key)).ServeHTTP(w, r)
				return
			}
			defer f.Close()
			binaries[key] = f
		}
	}

	if len(binaries) == 0 {
		problemjson.BadRequest(problemjson.Detail("no platform binaries provided (expected fields like linux_amd64)")).ServeHTTP(w, r)
		return
	}

	av := artifact.ArtifactVersion{
		Artifact:     artifactName,
		Version:      version,
		PreRelease:   preRelease,
		PublishedAt:  time.Now(),
		PlatformDeps: platformDeps,
	}
	if err := m.ArtifactRegistry().Publish(r.Context(), av, binaries); err != nil {
		if errors.Is(err, artifact.ErrAlreadyExists) {
			problemjson.Conflict(problemjson.Detail("version already exists for this platform")).ServeHTTP(w, r)
			return
		}
		problemjson.InternalServerError(problemjson.Detail("publish failed")).ServeHTTP(w, r)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (m *Manager) listArtifactVersions(w http.ResponseWriter, r *http.Request, artifactName string) {
	if m.ArtifactRegistry() == nil {
		problemjson.ServiceUnavailable(problemjson.Detail("artifact registry not configured")).ServeHTTP(w, r)
		return
	}
	versions, err := m.ArtifactRegistry().ListVersions(r.Context(), artifactName)
	if err != nil {
		problemjson.InternalServerError(problemjson.Detail("failed to list versions")).ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusOK, versions)
}

func (m *Manager) artifactLatest(w http.ResponseWriter, r *http.Request, artifactName string) {
	if m.ArtifactRegistry() == nil {
		problemjson.ServiceUnavailable(problemjson.Detail("artifact registry not configured")).ServeHTTP(w, r)
		return
	}
	versions, err := m.ArtifactRegistry().ListVersions(r.Context(), artifactName)
	if err != nil || len(versions) == 0 {
		problemjson.NotFound(problemjson.Detail("no versions found")).ServeHTTP(w, r)
		return
	}
	for _, v := range versions {
		if !v.PreRelease {
			writeJSON(w, http.StatusOK, v)
			return
		}
	}
	problemjson.NotFound(problemjson.Detail("no released version found")).ServeHTTP(w, r)
}

func (m *Manager) serveArtifact(w http.ResponseWriter, r *http.Request, artifactName, version, os_, arch string) {
	if m.ArtifactRegistry() == nil {
		problemjson.ServiceUnavailable(problemjson.Detail("artifact registry not configured")).ServeHTTP(w, r)
		return
	}

	// If the registry can serve files directly (e.g. FsRegistry), use that.
	if fs, ok := m.ArtifactRegistry().(artifact.FileServer); ok {
		path, err := fs.ServeFile(artifactName, version, os_, arch)
		if err != nil {
			problemjson.NotFound(problemjson.Detail("artifact not found")).ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, path)
		return
	}

	// Fall back to resolving and redirecting to an external URL.
	platform, err := m.ArtifactRegistry().ResolveVersion(r.Context(), artifactName, version, os_, arch)
	if err != nil {
		problemjson.NotFound(problemjson.Detail("artifact not found")).ServeHTTP(w, r)
		return
	}
	if platform.URL != "" {
		http.Redirect(w, r, platform.URL, http.StatusFound)
		return
	}
	problemjson.NotFound(problemjson.Detail("artifact binary not available")).ServeHTTP(w, r)
}

// ---- utilities ----

func sanitizeWebhookHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for k, v := range headers {
		if strings.HasPrefix(k, "X-") || k == "Authorization" {
			out[k] = v
		}
	}

	return out
}

func parseLimit(s string, def int) int {
	if s == "" {
		return def
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	if n <= 0 || n > 1000 {
		return def
	}
	return n
}

func writeSSEEvent(w http.ResponseWriter, event, data string) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}

func emitLogLines(w http.ResponseWriter, taskID string, rc io.Reader) {
	buf := make([]byte, 4096)
	var line strings.Builder
	for {
		n, err := rc.Read(buf)
		for _, b := range buf[:n] {
			if b == '\n' {
				if line.Len() > 0 {
					data, _ := json.Marshal(map[string]string{"task_id": taskID, "line": line.String()})
					writeSSEEvent(w, "log", string(data))
					line.Reset()
				}
			} else {
				line.WriteByte(b)
			}
		}
		if err != nil {
			break
		}
	}
}
