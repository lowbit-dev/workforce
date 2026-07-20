package storetest

import (
	"testing"

	"lowbit.dev/workforce/manager"
	"lowbit.dev/workforce/manager/store"
	"lowbit.dev/workforce/manager/webhooks"
)

func TestMemStoreJobStore(t *testing.T) {
	RunJobStoreTests(t, func() manager.JobStore { return store.NewMemStore() })
}

func TestMemStoreTaskStore(t *testing.T) {
	RunTaskStoreTests(t, func() manager.TaskStore { return store.NewMemStore() })
}

func TestMemStoreLogStore(t *testing.T) {
	RunLogStoreTests(t, func() logRunStore { return store.NewMemStore() })
}

func TestMemStoreWebhookStore(t *testing.T) {
	RunWebhookStoreTests(t, func() webhooks.WebhookStore { return store.NewMemStore() })
}
