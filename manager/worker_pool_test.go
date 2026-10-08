package manager

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func TestUnregister_StaleConnectionKeepsReplacement(t *testing.T) {
	requeued := make(chan []string, 4)
	pool := NewWorkerPool(slog.New(slog.NewTextHandler(io.Discard, nil)), make(chan struct{}, 1), func(ids []string) {
		requeued <- ids
	})

	oldConn, oldPeer := net.Pipe()
	defer oldConn.Close()
	defer oldPeer.Close()

	newConn, newPeer := net.Pipe()
	defer newConn.Close()
	defer newPeer.Close()

	old := NewWorkerConn("worker-a", "linux", "amd64", 10, oldConn)
	pool.register(old)
	if err := old.AssignTask("job-1", 1); err != nil {
		t.Fatalf("assign task: %v", err)
	}

	replacement := NewWorkerConn("worker-a", "linux", "amd64", 10, newConn)
	pool.register(replacement)

	select {
	case ids := <-requeued:
		if len(ids) != 1 || ids[0] != "job-1" {
			t.Fatalf("expected job-1 to be re-queued on reconnect, got %v", ids)
		}
	case <-time.After(time.Second):
		t.Fatal("expected in-flight job to be re-queued on reconnect")
	}

	// The read loop of the replaced connection ends and unregisters it.
	pool.unregister(old)

	if got, ok := pool.GetWorker("worker-a"); !ok || got != replacement {
		t.Fatal("expected the replacement connection to stay registered")
	}

	select {
	case ids := <-requeued:
		t.Fatalf("expected in-flight jobs to be re-queued once, got a second re-queue of %v", ids)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestEligibleWorkers_ExcludesRejected_WhenPlatformsEmpty(t *testing.T) {
	t.Helper()

	signal := make(chan struct{}, 1)
	pool := NewWorkerPool(slog.New(slog.NewTextHandler(io.Discard, nil)), signal, nil)

	connA, connAOther := net.Pipe()
	defer connA.Close()
	defer connAOther.Close()

	connB, connBOther := net.Pipe()
	defer connB.Close()
	defer connBOther.Close()

	workerA := NewWorkerConn("worker-a", "linux", "amd64", 10, connA)
	workerB := NewWorkerConn("worker-b", "linux", "amd64", 10, connB)

	pool.register(workerA)
	pool.register(workerB)

	workerA.rejectedJobsCache.Put("job-1", struct{}{})

	eligible := pool.eligibleWorkers(nil, 1, "job-1")
	if len(eligible) != 1 {
		t.Fatalf("expected 1 eligible worker, got %d", len(eligible))
	}
	if eligible[0].workerID != "worker-b" {
		t.Fatalf("expected worker-b to remain eligible, got %s", eligible[0].workerID)
	}
}
