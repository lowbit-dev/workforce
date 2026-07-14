package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"lowbit.dev/rungroup"
)

// systemMetrics is a point-in-time snapshot of whole-machine resource utilisation.
type systemMetrics struct {
	CPUPercent float64
	MemPercent float64
	SampledAt  time.Time
}

func (sm *systemMetrics) IsOverLimit(cpuLimit, memoryLimit float64) bool {
	return (cpuLimit > 0 && sm.CPUPercent >= cpuLimit) ||
		(memoryLimit > 0 && sm.MemPercent >= memoryLimit)
}

// metricsSampler keeps a cached systemMetrics snapshot refreshed by a background goroutine.
// The platform-specific sampleOnce function is provided at construction time (see
// metrics_linux.go and metrics_other.go).
type metricsSampler struct {
	mu         sync.RWMutex
	last       systemMetrics
	interval   time.Duration
	logger     *slog.Logger
	sampleOnce func() (cpu, mem float64, err error)
}

// newMetricsSampler returns a sampler wired to the platform implementation.
func newMetricsSampler(interval time.Duration, logger *slog.Logger) *metricsSampler {
	return &metricsSampler{
		interval: interval,
		logger:   logger,
		sampleOnce: func() (cpu float64, mem float64, err error) {
			return 0, 0, errors.New("Not implemeneted")
		},
	}
}

// snapshot returns the most recently sampled metrics.
func (m *metricsSampler) snapshot() systemMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.last
}

// run starts the background sampling loop. It blocks until ctx is cancelled.
func (m *metricsSampler) Run(ctx context.Context) error {
	return rungroup.NewIntervalService(m.interval, func(ctx context.Context) error {
		cpu, mem, err := m.sampleOnce()
		if err != nil {
			m.logger.Debug("[Worker][MetricsSampler][Run] Metrics sample failed", "error", err)
			return err
		}

		m.mu.Lock()
		m.last = systemMetrics{
			CPUPercent: cpu,
			MemPercent: mem,
			SampledAt:  time.Now(),
		}
		m.mu.Unlock()

		return nil
	}).Run(ctx)
}
