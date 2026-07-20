package manager

import (
	"io"
	"log/slog"
	"net"
	"testing"
)

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
