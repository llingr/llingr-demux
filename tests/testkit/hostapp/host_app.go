// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package hostapp

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-demux/tests/testkit/scenario"
	"github.com/llingr/llingr-nexus/nexus"
)

// HostApp simulates scenario processing workload with configurable latency and jitter.
type HostApp struct {
	startMicro       *atomic.Int64
	finishMicro      *atomic.Int64
	metricsCount     *atomic.Int64
	processorLatency time.Duration
	jitter           float64 // 0.0 to 1.0 (percentage)
	jitterRange      int64   // calculated from jitter percentage
	errorInjector    func(context.Context, *nexus.Message[scenario.TestMessage]) error
}

// NewHostApp creates a host application simulator.
// processorLatency: simulated processing time per scenario
// jitter: percentage variance in latency (0.0 to 1.0)
func NewHostApp(processorLatency time.Duration, jitter float64) *HostApp {
	jitterRange := int64(float64(processorLatency.Microseconds()) * jitter)

	return &HostApp{
		startMicro:       &atomic.Int64{},
		finishMicro:      &atomic.Int64{},
		metricsCount:     &atomic.Int64{},
		processorLatency: processorLatency,
		jitter:           jitter,
		jitterRange:      jitterRange,
	}
}

// InjectProcessError configures error injection for ProcessMessage.
func (h *HostApp) InjectProcessError(fn func(context.Context, *nexus.Message[scenario.TestMessage]) error) {
	h.errorInjector = fn
}

// ProcessMessage simulates scenario processing with configurable latency and jitter.
func (h *HostApp) ProcessMessage(ctx context.Context, msg *nexus.Message[scenario.TestMessage]) error {
	// check for injected error
	if h.errorInjector != nil {
		if err := h.errorInjector(ctx, msg); err != nil {
			return err
		}
	}

	// simulate processing work
	processorLatency := h.processorLatency
	if processorLatency > 0 {
		jitter := h.jitter
		jitterRange := h.jitterRange

		if jitter != 0 {
			jit := rand.Int63n(2*jitterRange+1) - jitterRange //nolint:gosec // G404: jitter for test simulation
			actualLatency := processorLatency + time.Duration(jit)
			if actualLatency > 0 {
				time.Sleep(actualLatency)
			}
		} else {
			time.Sleep(processorLatency)
		}
	}

	return nil
}

// WriteDeadLetter handles messages that failed processing.
func (h *HostApp) WriteDeadLetter(_ context.Context, _ *nexus.Message[scenario.TestMessage], _ error) error {
	return nil
}

// MetricsSink collects metrics and tracks timing information for efficiency calculations.
func (h *HostApp) MetricsSink(_ nexus.SinkContext, metrics nexus.Metrics) error {
	processStartTime := metrics.ProcessStartTime
	processorLatency := h.processorLatency

	// track earliest start and latest finish times
	startTs := processStartTime.UnixMicro()
	finishTs := processStartTime.Add(processorLatency).UnixMicro()

	startMicro := h.startMicro
	for {
		current := startMicro.Load()
		if current == 0 || startTs < current {
			if startMicro.CompareAndSwap(current, startTs) {
				break
			}
		} else {
			break
		}
	}

	finishMicro := h.finishMicro
	for {
		current := finishMicro.Load()
		if finishTs > current {
			if finishMicro.CompareAndSwap(current, finishTs) {
				break
			}
		} else {
			break
		}
	}

	h.metricsCount.Add(1)
	return nil
}

// GetMetricsCount returns the number of metrics collected.
func (h *HostApp) GetMetricsCount() int64 {
	return h.metricsCount.Load()
}

// GetProcessingWindow returns the earliest ProcessStartTime and latest finish time
// as recorded by the MetricsSink. Returns zero times if no metrics have been collected.
func (h *HostApp) GetProcessingWindow() (start, finish time.Time) {
	s := h.startMicro.Load()
	f := h.finishMicro.Load()
	if s == 0 || f == 0 {
		return time.Time{}, time.Time{}
	}
	return time.UnixMicro(s), time.UnixMicro(f)
}

// CalculateEfficiency computes actual vs theoretical TPS and efficiency percentage.
// For zero latency (pure ctrl overhead measurement), theoreticalTPS and efficiency return -1.
func (h *HostApp) CalculateEfficiency(messageCount, concurrentKeys int) (actualTPS, theoreticalTPS, efficiency float64) {
	start := h.startMicro.Load()
	finish := h.finishMicro.Load()

	if start == 0 || finish == 0 || finish <= start {
		return 0, 0, 0
	}

	durationMicros := finish - start
	actualTPS = float64(messageCount) / float64(durationMicros) * 1_000_000 //nolint:mnd // micros to seconds

	if h.processorLatency > 0 {
		theoreticalTPS = float64(concurrentKeys) *
			(1_000_000.0 / float64(h.processorLatency.Microseconds())) //nolint:mnd // micros to seconds
		efficiency = (actualTPS / theoreticalTPS) * 100.0 //nolint:mnd // percentage
	} else {
		// Zero latency: theoretical is infinite, use -1 to indicate N/A
		theoreticalTPS = -1
		efficiency = -1
	}

	return actualTPS, theoreticalTPS, efficiency
}

const perfData = `messages: %d, concurrent_keys: %d, simulated_latency: %v, tps_theoretical: %.0f, tps_actual: %.1f, pct: %.1f%%`

// PrintPerformanceSummary returns a formatted string with TPS metrics.
func (h *HostApp) PrintPerformanceSummary(messageCount, concurrentKeys int) string {
	actualTPS, theoreticalTPS, efficiency := h.CalculateEfficiency(messageCount, concurrentKeys)

	return fmt.Sprintf(perfData, messageCount, concurrentKeys, h.processorLatency, theoreticalTPS,
		actualTPS, efficiency)
}
