package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"lowbit.dev/ulid"
	"lowbit.dev/workforce/contract"
)

// webhookEvent is the envelope sent to webhook endpoints.
type webhookEvent struct {
	Event       string         `json:"event"`
	JobID       string         `json:"job_id"`
	ParentJobID string         `json:"parent_job_id,omitempty"`
	Timestamp   time.Time      `json:"timestamp"`
	Data        map[string]any `json:"data,omitempty"`
}

// All known event names.
const (
	eventTaskProposing = "task.proposing"
	eventJobRunning    = "job.running"
	eventJobProgress   = "job.progress"
	eventJobCompleted  = "job.completed"
	eventJobFailed     = "job.failed"
	eventJobCancelled  = "job.cancelled"
)

type WebhookDispatcherConfig struct {
	// WebhookTimeout is the per-request HTTP timeout for webhook delivery attempts. Default: 1s.
	WebhookTimeout time.Duration

	// WebhookMaxBackoff caps the exponential backoff delay between webhook retries. Default: 5m.
	WebhookMaxBackoff time.Duration

	// MaxWebhookAttempts is the maximum number of delivery attempts for a single WebhookEntry.
	// Zero means retry indefinitely.
	MaxWebhookAttempts int
}

// webhookDispatcher fires webhook events and manages the retry queue.
type WebhookDispatcher struct {
	store  WebhookStore
	cfg    *WebhookDispatcherConfig
	logger *slog.Logger
	client *http.Client
	wakeCh chan struct{}
}

func NewWebhookDispatcher(store WebhookStore, cfg *WebhookDispatcherConfig, logger *slog.Logger) *WebhookDispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.WebhookTimeout <= 0 {
		cfg.WebhookTimeout = time.Second
	}
	if cfg.WebhookMaxBackoff <= 0 {
		cfg.WebhookMaxBackoff = 5 * time.Minute
	}

	return &WebhookDispatcher{
		store:  store,
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: cfg.WebhookTimeout},
		wakeCh: make(chan struct{}, 1),
	}
}

// run is the webhook delivery loop. It blocks until ctx is cancelled.
func (w *WebhookDispatcher) Run(ctx context.Context) error {
	// On startup we dispatch the first wake signal which would trigger the backlog procesing
	w.wake()

	timer := time.NewTimer(time.Hour)
	timer.Stop()

	for {
		timer.Reset(w.processPending(ctx))

		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("context ended: %w", ctx.Err())

		case <-timer.C:
			// The timer fired naturally. The channel is now empty,
			// so we don't need to do anything special before the next Reset().

		case <-w.wakeCh:
			if !timer.Stop() {
				// Drain the channel to ensure the old signal doesn't
				// trigger a false wake on the next iteration of the loop.
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

// processPending fetches due webhooks, attempts delivery, schedules retries,
// and returns the duration the dispatcher should sleep before the next retry.
func (w *WebhookDispatcher) processPending(ctx context.Context) time.Duration {
	entries, err := w.store.DequeueWebhooks(ctx)
	if err != nil {
		w.logger.Error("webhook queue: dequeue", "error", err)
	}

	nextWake := time.Time{}

	for _, e := range entries {
		if err := w.deliver(ctx, e); err != nil {
			w.logger.Warn("webhook delivery failed",
				"webhook_id", e.ID, "job_id", e.JobID, "url", e.URL,
				"attempts", e.Attempts+1, "error", err)

			next := w.scheduleRetry(ctx, e)
			if !next.IsZero() && (nextWake.IsZero() || next.Before(nextWake)) {
				nextWake = next
			}
		}
	}

	// If no retries are scheduled, park for an hour (until woken manually).
	if nextWake.IsZero() {
		return time.Hour
	}

	// Otherwise, sleep until the next required wake time.
	delay := time.Until(nextWake)
	if delay < 0 {
		return 0 // The next retry is already past due, don't wait at all.
	}

	return delay
}

// wake signals the delivery loop to wake up immediately.
func (w *WebhookDispatcher) wake() {
	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
}

// deliver attempts a single HTTP POST delivery. Returns nil on HTTP 200.
func (w *WebhookDispatcher) deliver(ctx context.Context, e *WebhookEntry) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.URL, bytes.NewReader(e.Payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Extract event name from payload for the X-Workforce-Event header.
	var env webhookEvent
	if json.Unmarshal(e.Payload, &env) == nil {
		req.Header.Set("X-Workforce-Event", e.Event)
	}

	// Forward client-supplied headers (X-* and Authorization only).
	for k, v := range e.Headers {
		if strings.HasPrefix(k, "X-") || k == "Authorization" {
			req.Header.Set(k, v)
		}
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-200 response: %d", resp.StatusCode)
	}

	return w.store.AckWebhook(ctx, e.ID)
}

// scheduleRetry updates the retry counter and next attempt time.
// Returns the scheduled next attempt time.
func (w *WebhookDispatcher) scheduleRetry(ctx context.Context, e *WebhookEntry) time.Time {
	attempts := e.Attempts + 1

	if w.cfg.MaxWebhookAttempts > 0 && attempts >= w.cfg.MaxWebhookAttempts {
		w.logger.Error("webhook delivery exhausted",
			"webhook_id", e.ID, "job_id", e.JobID, "url", e.URL,
			"attempts", attempts)
		_ = w.store.AckWebhook(ctx, e.ID) // remove from queue
		return time.Time{}
	}

	backoff := time.Duration(math.Pow(2, float64(attempts-1))) * time.Second
	if backoff > w.cfg.WebhookMaxBackoff {
		backoff = w.cfg.WebhookMaxBackoff
	}

	next := time.Now().Add(backoff)
	_ = w.store.NackWebhook(ctx, e.ID, attempts, next)
	return next
}

func (w *WebhookDispatcher) ShouldDispatchEvent(job *contract.Job, event string) bool {
	if job.WebhookURL == "" {
		return false
	}

	if len(job.WebhookEvents) > 0 && !sliceContains(job.WebhookEvents, event) {
		return false
	}

	return true
}

// enqueue stores a new webhook entry and wakes the delivery loop.
func (w *WebhookDispatcher) enqueue(ctx context.Context, job *contract.Job, event string, data map[string]any) {
	if job.WebhookURL == "" {
		return
	}

	if len(job.WebhookEvents) > 0 && !sliceContains(job.WebhookEvents, event) {
		return
	}

	env := webhookEvent{
		Event:     event,
		JobID:     job.ID,
		Timestamp: time.Now(),
		Data:      data,
	}

	if job.ParentJobID != "" {
		env.ParentJobID = job.ParentJobID
	}

	payload, err := json.Marshal(env)
	if err != nil {
		w.logger.Error("[WebhookDispatcher][enqueu]: Failed to marshall webhook event", "event", event, "error", err)
		return
	}

	entry := &WebhookEntry{
		ID:            ulid.Make().String(),
		JobID:         job.ID,
		Event:         event,
		URL:           job.WebhookURL,
		Payload:       payload,
		Headers:       job.WebhookHeaders,
		NextAttemptAt: time.Now(),
		CreatedAt:     time.Now(),
	}

	if err := w.store.EnqueueWebhook(ctx, entry); err != nil {
		w.logger.Error("webhook: enqueue", "event", event, "error", err)
		return
	}
	w.wake()
}

// ---- convenience fire methods ----

func (w *WebhookDispatcher) FireJobProposing(ctx context.Context, job *contract.Job, workerID string) {
	w.enqueue(ctx, job, eventTaskProposing, map[string]any{
		"task_name": job.TaskName,
		"cost":      job.Cost,
		"worker_id": workerID,
	})
}

func (w *WebhookDispatcher) FireJobRunning(ctx context.Context, job *contract.Job, workerID string) {
	w.enqueue(ctx, job, eventJobRunning, map[string]any{
		"worker_id": workerID,
	})
}

func (w *WebhookDispatcher) FireJobFailed(ctx context.Context, job *contract.Job, reason string) {
	w.enqueue(ctx, job, eventJobFailed, map[string]any{"failure_reason": reason})
}

func (w *WebhookDispatcher) FireJobCompleted(ctx context.Context, job *contract.Job) {
	w.enqueue(ctx, job, eventJobCompleted, map[string]any{
		"result": job.Result,
	})
}

func (w *WebhookDispatcher) FireJobCancelled(ctx context.Context, job *contract.Job) {
	w.enqueue(ctx, job, eventJobCancelled, nil)
}

func (w *WebhookDispatcher) FireJobProgress(ctx context.Context, job *contract.Job, completed, total int) {
	w.enqueue(ctx, job, eventJobProgress, map[string]any{
		"completed_tasks": completed,
		"total_tasks":     total,
	})
}

func sliceContains(s []string, v string) bool {
	for _, item := range s {
		if item == v {
			return true
		}
	}
	return false
}
