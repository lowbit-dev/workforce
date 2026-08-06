package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"lowbit.dev/rungroup"
	"lowbit.dev/workforce/worker/window"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
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
	mu     sync.RWMutex
	window *window.MuWindow[systemMetrics]

	last          systemMetrics
	interval      time.Duration
	logger        *slog.Logger
	sampleTimeout time.Duration
}

// newMetricsSampler returns a sampler wired to the platform implementation.
func newMetricsSampler(interval time.Duration, sampleTimout time.Duration, logger *slog.Logger) *metricsSampler {
	return &metricsSampler{
		interval:      interval,
		logger:        logger,
		sampleTimeout: sampleTimout,
		window:        window.NewMu[systemMetrics](5),
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
		sampleCtx, cancel := context.WithTimeout(ctx, m.sampleTimeout)
		defer cancel()

		metrics, err := m.SampleOnce(sampleCtx)
		if err != nil {
			m.logger.Debug("[Worker][MetricsSampler][Run] Metrics sample failed", "error", err)
			return err
		}

		m.window.Push(metrics)

		m.mu.Lock()
		m.last = m.CalculateSystemEMA()
		m.mu.Unlock()

		return nil
	}).Run(ctx)
}

func (m *metricsSampler) SampleOnce(ctx context.Context) (systemMetrics, error) {
	metrics := systemMetrics{SampledAt: time.Now()}

	cpuPercent, err := cpu.PercentWithContext(ctx, time.Second, false)
	if err != nil {
		return metrics, fmt.Errorf("failed to mesure cpu usage: %w", err)
	}

	metrics.CPUPercent = cpuPercent[0]

	v, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return metrics, fmt.Errorf("failed to obtain virtual memory: %w", err)
	}

	metrics.MemPercent = v.UsedPercent

	return metrics, nil
}

// CalculateSystemSMA uses the window's built-in Average method. (Simple Moving Average)
func (m *metricsSampler) CalculateSystemSMA() systemMetrics {
	return m.window.Average(addMetrics, divideMetrics)
}

// CalculateSystemEMA applies the EMA logic safely using the Each iterator. (Exponential Moving Average)
func (m *metricsSampler) CalculateSystemEMA() systemMetrics {
	var ema systemMetrics
	count := m.window.Count()
	if count == 0 {
		return ema
	}

	alpha := 2.0 / float64(count+1)
	isFirst := true

	m.window.Each(func(next systemMetrics) {
		if isFirst {
			ema = next
			isFirst = false
		} else {
			ema = combineMetrics(ema, next, alpha)
		}
	})

	return ema
}

// systemMetricsDelta represents the difference between two systemMetrics readings.
type systemMetricsDelta struct {
	CPUPercentDelta float64
	MemPercentDelta float64
	TimeElapsed     time.Duration
}

// CalculateMetricsDelta computes the absolute difference between a current
// and previous metrics reading.
func CalculateMetricsDelta(prev, curr systemMetrics) systemMetricsDelta {
	return systemMetricsDelta{
		CPUPercentDelta: curr.CPUPercent - prev.CPUPercent,
		MemPercentDelta: curr.MemPercent - prev.MemPercent,
		TimeElapsed:     curr.SampledAt.Sub(prev.SampledAt),
	}
}

// addMetrics sums the metrics. By assigning `next.SampledAt`,
// we ensure the accumulator continually carries forward the newest timestamp.
func addMetrics(acc, next systemMetrics) systemMetrics {
	return systemMetrics{
		CPUPercent: acc.CPUPercent + next.CPUPercent,
		MemPercent: acc.MemPercent + next.MemPercent,
		SampledAt:  next.SampledAt,
	}
}

// divideMetrics finalizes the SMA. It applies the division to the floats
// but passes the timestamp through untouched.
func divideMetrics(total systemMetrics, count int) systemMetrics {
	c := float64(count)
	return systemMetrics{
		CPUPercent: total.CPUPercent / c,
		MemPercent: total.MemPercent / c,
		SampledAt:  total.SampledAt,
	}
}

// -- EMA Callback --

// combineMetrics applies the exponential smoothing formula.
// Like addMetrics, it continuously adopts the `next` timestamp.
func combineMetrics(ema, next systemMetrics, alpha float64) systemMetrics {
	return systemMetrics{
		CPUPercent: (next.CPUPercent * alpha) + (ema.CPUPercent * (1.0 - alpha)),
		MemPercent: (next.MemPercent * alpha) + (ema.MemPercent * (1.0 - alpha)),
		SampledAt:  next.SampledAt,
	}
}
