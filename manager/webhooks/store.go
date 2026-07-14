package webhooks

import (
	"context"
	"time"

	"lowbit.dev/workforce/contract"
)

// WebhookEntry is a persistable record of a pending webhook delivery.
// An entry remains in the Store until the target URL responds with HTTP 200.
type WebhookEntry struct {
	ID      string
	JobID   string // root job this delivery is associated with
	Event   string
	URL     string
	Payload contract.JsonOrBytes // JSON-encoded webhook event payload
	// Headers holds custom client-supplied headers to forward with delivery.
	// Only X-* prefixed keys and "Authorization" are stored here.
	Headers       map[string]string
	Attempts      int
	NextAttemptAt time.Time
	CreatedAt     time.Time
}

// WebhookStore persists the delivery queue.
type WebhookStore interface {
	// EnqueueWebhook stores a new pending webhook delivery.
	EnqueueWebhook(ctx context.Context, entry *WebhookEntry) error
	// DequeueWebhooks returns all entries due for delivery (NextAttemptAt <= now).
	DequeueWebhooks(ctx context.Context) ([]*WebhookEntry, error)
	// AckWebhook removes a successfully delivered entry from the queue.
	AckWebhook(ctx context.Context, id string) error
	// NackWebhook increments the attempt counter and schedules the next retry.
	NackWebhook(ctx context.Context, id string, attempts int, nextAttemptAt time.Time) error
}
