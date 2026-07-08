// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-demux/tests/testkit/broker"
	"github.com/llingr/llingr-demux/tests/testkit/hostapp"
	"github.com/llingr/llingr-demux/tests/testkit/scenario"
	"github.com/llingr/llingr-nexus/nexus"
)

const topicName = "llingr-demux-test-topic"

// e2eTestConfig returns a common config for e2e tests.
func e2eTestConfig(concurrentKeys int) config.DemuxConfig {
	return config.DemuxConfig{
		ConcurrentKeys:               concurrentKeys,
		PerKeyBufferLen:              5,
		WorkerShardsCount:            16,
		AutoCommitInterval:           100 * time.Millisecond,
		AcquireCommitGuardTimeout:    50 * time.Millisecond,
		PollTimeout:                  10 * time.Millisecond,
		AwaitAssignmentsTimeout:      5 * time.Second,
		DrainTimeout:                 30 * time.Second,
		RebalancePausePollingTimeout: 5 * time.Second,
	}
}

// Test_EndToEnd_Messages_InOrder verifies the entire demux pipeline:
// - Messages polled from mock broker in sequence
// - Processed concurrently across multiple workers with per-key ordering preserved
// - Offsets committed in ascending order per partition
// - Metrics collected for all messages
// - No data loss
// - Performance efficiency tracked
func Test_EndToEnd_Messages_InOrder(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	const (
		messageCount      = 50_000
		concurrentKeys    = 500
		numPartitions     = 10
		processorLatency  = 100 * time.Millisecond
		jitter            = 0.0  // no jitter for deterministic test
		requireEfficiency = 0.97 // minimum 97% efficiency at 5,000 TPS, even on low power machines
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages := scenario.GenerateMessages(messageCount, numPartitions)

	// in-memory mock
	mockBroker := broker.NewMockBroker(messages, func() {})

	// host app simulates latency and errors
	hostApp := hostapp.NewHostApp(processorLatency, jitter)

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys: concurrentKeys,
	}

	// Build consumer using builder pattern
	builder := demux.NewBuilder(topicName, hostApp.ProcessMessage, hostApp.WriteDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(hostApp.MetricsSink).
		WithExtractEnvelope(hostapp.SimpleEnvelopeExtractor).
		WithLogger(logger)

	consumer := builder.Build(mockBroker)

	// configure broker to trigger rebalance when Subscribe is called
	// (simulates broker clients that automatically assign partitions)
	rebalanceDone := make(chan struct{}, 1)
	mockBroker.SetRebalanceCallback(func() {
		defer func() {
			rebalanceDone <- struct{}{}
		}()
		time.Sleep(10 * time.Millisecond) // small delay to simulate Kafka
		rebalanceInfo := make([]nexus.RebalanceInfo, numPartitions)
		for i := int32(0); i < numPartitions; i++ {
			rebalanceInfo[i] = nexus.RebalanceInfo{
				Partition: i,
			}
		}
		if err := consumer.TriggerRebalance(nexus.Assign, rebalanceInfo); err != nil {
			t.Errorf("TriggerRebalance failed: %v", err)
		}
	})

	// subscribe and start processing
	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	select {
	case <-rebalanceDone:
		t.Logf("rebalance callback received")
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for rebalance")
	}

	// wait for all messages to be processed (poll every second for up to 2 minutes)
	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if hostApp.GetMetricsCount() >= int64(messageCount) {
			break
		}
		<-ticker.C
	}

	// verify completion
	metricsCount := hostApp.GetMetricsCount()
	if metricsCount != int64(messageCount) {
		t.Fatalf("Expected %d metrics, got %d", messageCount, metricsCount)
	}

	if hostApp.GetMetricsCount() != messageCount {
		t.Errorf("metrics collected: %d, want %d", hostApp.GetMetricsCount(), messageCount)
	}

	// verify commits are ascending per partition
	committedOffsets := mockBroker.GetCommittedOffsets()
	if len(committedOffsets) != numPartitions {
		t.Errorf("Committed partitions = %d, want %d", len(committedOffsets), numPartitions)
	}

	// verify each partition has expected commits
	messagesPerPartition := messageCount / numPartitions
	for partition := int32(0); partition < numPartitions; partition++ {
		offset := mockBroker.GetCommittedOffset(partition)
		if offset < 0 {
			t.Errorf("Partition %d: no commits", partition)
			continue
		}

		// committed offset should be highest offset in partition
		expectedOffset := int64(messagesPerPartition - 1)
		if offset != expectedOffset {
			t.Errorf("Partition %d: committed offset = %d, want %d",
				partition, offset, expectedOffset)
		}
	}

	// calculate and verify performance efficiency
	actualTPS, theoreticalTPS, efficiency := hostApp.CalculateEfficiency(messageCount, concurrentKeys)
	t.Logf("%s", hostApp.PrintPerformanceSummary(messageCount, concurrentKeys))

	// verify efficiency meets minimum threshold
	if efficiency < requireEfficiency {
		t.Errorf("Efficiency %.2f%% is below required %.2f%%",
			efficiency, requireEfficiency*100)
	}

	// verify actual TPS is above minimum threshold
	minTPS := theoreticalTPS * requireEfficiency
	if actualTPS < minTPS {
		t.Errorf("Actual TPS %.2f is below minimum %.2f (%.0f%% of theoretical)",
			actualTPS, minTPS, requireEfficiency*100)
	}

	// verify no errors were logged
	if logger.HasErrors() {
		t.Errorf("Unexpected errors logged: %v", logger.Errors())
	}
}

// Test_EndToEnd_WithProcessErrors verifies error handling and dead letter functionality.
func Test_EndToEnd_WithProcessErrors(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	const (
		messageCount     = 10_000
		concurrentKeys   = 500
		numPartitions    = 4
		processorLatency = 5 * time.Millisecond
		jitter           = 0.1
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages := scenario.GenerateMessages(messageCount, numPartitions)
	mockBroker := broker.NewMockBroker(messages, func() {})
	hostApp := hostapp.NewHostApp(processorLatency, jitter)
	deadLetterCollector := hostapp.NewDeadLetterCollector()
	logger := mocklogger.NewRecordingLogger()

	// inject errors for every 100th scenario
	hostApp.InjectProcessError(func(_ context.Context, msg *nexus.Message[scenario.TestMessage]) error {
		if msg.Payload.ID%100 == 0 {
			return fmt.Errorf("simulated process error for scenario %d", msg.Payload.ID)
		}
		return nil
	})

	cfg := e2eTestConfig(concurrentKeys)

	// Build consumer using builder pattern
	builder := demux.NewBuilder(topicName, hostApp.ProcessMessage, deadLetterCollector.WriteDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(hostApp.MetricsSink).
		WithExtractEnvelope(hostapp.SimpleEnvelopeExtractor).
		WithLogger(logger)

	consumer := builder.Build(mockBroker)

	// configure broker to trigger rebalance when Subscribe is called
	mockBroker.SetRebalanceCallback(broker.MakeAssignAllPartitionsCallback(t, consumer, numPartitions))

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// wait for completion
	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if hostApp.GetMetricsCount() >= int64(messageCount) {
			break
		}
		<-ticker.C
	}

	time.Sleep(500 * time.Millisecond)

	// verify all messages processed
	if count := hostApp.GetMetricsCount(); count != int64(messageCount) {
		t.Fatalf("Metrics count = %d, want %d", count, messageCount)
	}

	// verify expected number of dead letters (every 100th scenario)
	expectedDeadLetters := messageCount / 100
	if dlCount := deadLetterCollector.Count(); dlCount != expectedDeadLetters {
		t.Errorf("Dead letter count = %d, want %d", dlCount, expectedDeadLetters)
	}

	// verify dead letter reasons
	deadLetters := deadLetterCollector.GetDeadLetters()
	for i, dl := range deadLetters {
		if dl.Reason == nil {
			t.Errorf("Dead letter %d has nil reason", i)
		}
		if dl.Message == nil {
			t.Errorf("Dead letter %d has nil scenario", i)
		}
	}

	t.Logf("\n%s", hostApp.PrintPerformanceSummary(messageCount, concurrentKeys))
}

// Test_EndToEnd_CommitErrors verifies behaviour when commit operations fail.
func Test_EndToEnd_CommitErrors(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	const (
		messageCount     = 5_000
		concurrentKeys   = 250
		numPartitions    = 2
		processorLatency = 2 * time.Millisecond
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages := scenario.GenerateMessages(messageCount, numPartitions)
	mockBroker := broker.NewMockBroker(messages, func() {})
	hostApp := hostapp.NewHostApp(processorLatency, 0.0)
	deadLetterCollector := hostapp.NewDeadLetterCollector()
	logger := mocklogger.NewRecordingLogger()

	// inject commit error initially
	mockBroker.InjectCommitError(fmt.Errorf("simulated commit failure"))

	cfg := e2eTestConfig(concurrentKeys)

	// Build consumer using builder pattern
	builder := demux.NewBuilder(topicName, hostApp.ProcessMessage, deadLetterCollector.WriteDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(hostApp.MetricsSink).
		WithExtractEnvelope(hostapp.SimpleEnvelopeExtractor).
		WithLogger(logger)

	consumer := builder.Build(mockBroker)

	// configure broker to trigger rebalance when Subscribe is called
	mockBroker.SetRebalanceCallback(broker.MakeAssignAllPartitionsCallback(t, consumer, numPartitions))

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// let some processing happen with commit errors
	time.Sleep(500 * time.Millisecond)

	// clear commit error
	mockBroker.InjectCommitError(nil)

	// wait for completion
	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if hostApp.GetMetricsCount() >= int64(messageCount) {
			break
		}
		<-ticker.C
	}

	time.Sleep(1 * time.Second) // allow final commits

	// verify all messages processed
	if count := hostApp.GetMetricsCount(); count != int64(messageCount) {
		t.Fatalf("Metrics count = %d, want %d", count, messageCount)
	}

	// verify commit errors were logged
	if !logger.ContainsError("commit") && !logger.ContainsError("offset") {
		t.Log("Note: Expected commit error to be logged")
	}

	// verify final commits succeeded
	committedOffsets := mockBroker.GetCommittedOffsets()
	if len(committedOffsets) == 0 {
		t.Error("No offsets were committed after clearing error")
	}

	t.Logf("\n%s", hostApp.PrintPerformanceSummary(messageCount, concurrentKeys))
}

// Test_EndToEnd_StressTest_SaturateWorkers hammers the demux pipeline with an infinite
// stream of messages for 30 seconds, saturating workers with 1000 concurrent keys
// and random 0-1ms processing delays.
//
// Verification:
//   - Progress logged every second (must be making progress)
//   - All 100 partitions must have committed == returned offsets at end
//   - Gap buffer stress: random control-record gaps every ~5000 messages
//
// Memory footprint: ~2.4KB for broker tracking (no message storage)
func Test_EndToEnd_StressTest_SaturateWorkers(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	const (
		testDuration  = 30 * time.Second
		numPartitions = 100
		progressTick  = 1 * time.Second
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Streaming broker generates infinite messages
	streamingBroker := broker.NewStreamingMockBroker()

	// Stress host app with random 0-1ms delay per message
	stressApp := newStressHostApp()

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		// ConcurrentKeys defaults to 250 - optimal for throughput
		PerKeyBufferLen:    32, // larger buffer for sustained throughput
		AutoCommitInterval: 100 * time.Millisecond,
		PollTimeout:        1 * time.Millisecond, // fast polling
	}

	builder := demux.NewBuilder(topicName, stressApp.ProcessMessage, stressApp.WriteDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(stressApp.MetricsSink).
		WithExtractEnvelope(streamingBroker.ExtractEnvelope).
		WithLogger(logger)

	consumer := builder.Build(streamingBroker)

	// Configure rebalance callback
	rebalanceDone := make(chan struct{})
	streamingBroker.SetRebalanceCallback(func() {
		time.Sleep(10 * time.Millisecond)
		rebalanceInfo := make([]nexus.RebalanceInfo, numPartitions)
		for i := int32(0); i < numPartitions; i++ {
			rebalanceInfo[i] = nexus.RebalanceInfo{Partition: i}
		}
		if err := consumer.TriggerRebalance(nexus.Assign, rebalanceInfo); err != nil {
			t.Errorf("TriggerRebalance failed: %v", err)
		}
		close(rebalanceDone)
	})

	// Start consuming
	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	select {
	case <-rebalanceDone:
		t.Log("partitions assigned, starting stress test")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for partition assignment")
	}

	// Run stress test with progress logging
	startTime := time.Now()
	ticker := time.NewTicker(progressTick)
	defer ticker.Stop()

	var lastPolled, lastMetrics int64
	deadline := time.Now().Add(testDuration)

	for time.Now().Before(deadline) {
		<-ticker.C

		polled := streamingBroker.PolledCount()
		metrics := stressApp.GetMetricsCount()
		elapsed := time.Since(startTime).Seconds()

		polledDelta := polled - lastPolled
		metricsDelta := metrics - lastMetrics

		t.Logf("[%.0fs] polled: %d (+%d/s), done: %d (+%d/s), pre-commit: %d",
			elapsed, polled, polledDelta, metrics, metricsDelta, polled-metrics)

		// Verify we're making progress
		if polledDelta == 0 {
			t.Error("no progress in last second - pipeline may be stuck")
		}

		lastPolled = polled
		lastMetrics = metrics
	}

	// Stop broker completely - Poll now returns nothing
	t.Log("stopping broker, all in-flight messages should drain...")
	streamingBroker.Stop()
	finalPolled := streamingBroker.PolledCount()

	// Wait for all messages to drain - with 1000 concurrent keys, max ~1000 truly in-flight
	// At 10-12ms per message, should drain within 15 seconds max.
	// In-flight is measured at the metrics PORT (lossless lifecycle accounting:
	// Collected + Dropped + SendFailed), not at the sink, whose receipts are
	// at-most-once under load and previously made this loop burn its whole
	// budget waiting for telemetry that was legitimately dropped.
	demuxConsumer := consumer.(*demux.Consumer[scenario.TestMessage]) //nolint:forcetypeassert // test: known type from builder
	drainStart := time.Now()
	drainDeadline := drainStart.Add(30 * time.Second)
	var lastLogTime time.Time
	for time.Now().Before(drainDeadline) {
		stats := demuxConsumer.MetricsStats()
		inflight := finalPolled - (stats.Collected + stats.Dropped + stats.SendFailed)
		if inflight == 0 {
			t.Logf("pipeline fully drained in %v", time.Since(drainStart))
			break
		}
		if time.Since(lastLogTime) >= 1*time.Second {
			t.Logf("draining: %d remaining (should be <= 1000 concurrent keys)", inflight)
			lastLogTime = time.Now()
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Graceful shutdown
	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown warning: %v", err)
	}

	// Brief pause to ensure final commits are flushed
	time.Sleep(200 * time.Millisecond)

	// Final stats
	finalMetrics := stressApp.GetMetricsCount()
	polled, committed := streamingBroker.GetStats()
	elapsed := time.Since(startTime)

	// Final internal metrics collector stats (port-level lifecycle accounting)
	metricsStats := demuxConsumer.MetricsStats()

	t.Logf("\n=== STRESS TEST COMPLETE ===")
	t.Logf("Duration: %v", elapsed)
	t.Logf("Messages polled: %d", polled)
	t.Logf("Total committed offset sum: %d", committed)
	t.Logf("Metrics collector: collected=%d, dropped=%d, sendFailed=%d", metricsStats.Collected, metricsStats.Dropped, metricsStats.SendFailed)
	t.Logf("Metrics sink received: %d", finalMetrics)
	t.Logf("Throughput: %.0f msg/sec", float64(polled)/elapsed.Seconds())
	t.Logf("Avg time-to-processed (queue + process): %v", stressApp.GetAverageProcessedTime())
	t.Logf("Avg end-to-end latency (ReadTime -> WatermarkAdvanceTime): %v", stressApp.GetAverageLatency())

	// Verify all messages drained. The drain signal is the metrics PORT, not the
	// sink: every completed work-item lifecycle is accounted losslessly into
	// exactly one of the collector's three atomics (Collected on sink delivery,
	// Dropped on channel overflow, SendFailed on sink error), while sink
	// receipts are at-most-once by contract (the collector drops telemetry
	// rather than block the hot path). Asserting sink receipts == polled
	// misread ordinary overflow drops under machine load as messages stuck in
	// the pipeline. If anything were genuinely stuck, its lifecycle would never
	// reach the port and this equality still fails.
	lifecycles := metricsStats.Collected + metricsStats.Dropped + metricsStats.SendFailed
	if lifecycles != polled {
		t.Errorf("DRAIN FAILED: only %d of %d message lifecycles completed (%d stuck in pipeline)",
			lifecycles, polled, polled-lifecycles)
	}

	// Telemetry drops are an environmental property (collector-goroutine
	// scheduling under load), not an engine defect: surface, don't fail.
	if metricsStats.Dropped > 0 {
		t.Logf("metrics collector dropped %d telemetry records under load (drop-on-overflow contract)",
			metricsStats.Dropped)
	}

	// Sink received exactly what collector sent
	if finalMetrics != metricsStats.Collected {
		t.Errorf("metrics sink mismatch: sink got %d, collector sent %d", finalMetrics, metricsStats.Collected)
	}

	// Verify all partitions committed correctly
	if err := streamingBroker.VerifyAllCommitted(); err != nil {
		t.Errorf("commit verification failed: %v", err)
	}

	// Verify no errors logged
	if logger.HasErrors() {
		t.Errorf("errors logged during stress test: %v", logger.Errors())
	}

	t.Logf("Stress test passed: %d messages at %.0f/sec, all drained and committed",
		finalMetrics, float64(finalMetrics)/elapsed.Seconds())
}

// stressHostApp processes messages with random 0-1ms delay to saturate workers.
type stressHostApp struct {
	metricsCount     atomic.Int64
	totalLatencyNs   atomic.Int64 // sum of end-to-end latencies in nanoseconds
	totalProcessedNs atomic.Int64 // sum of queue + process time in nanoseconds
}

func newStressHostApp() *stressHostApp {
	return &stressHostApp{}
}

func (h *stressHostApp) ProcessMessage(_ context.Context, _ *nexus.Message[scenario.TestMessage]) error {
	// No delay - full speed stress test
	return nil
}

func (h *stressHostApp) WriteDeadLetter(_ context.Context, _ *nexus.Message[scenario.TestMessage], _ error) error {
	return nil
}

func (h *stressHostApp) MetricsSink(_ nexus.SinkContext, m nexus.Metrics) error {
	h.metricsCount.Add(1)
	// Capture end-to-end latency: ReadTime -> WatermarkAdvanceTime
	latency := m.WatermarkAdvanceTime.Sub(m.ReadTime)
	h.totalLatencyNs.Add(int64(latency))
	// Capture time to process: (ProcessStartTime - ReadTime) + ProcessDuration
	queueTime := m.ProcessStartTime.Sub(m.ReadTime)
	processedTime := queueTime + m.ProcessDuration
	h.totalProcessedNs.Add(int64(processedTime))
	return nil
}

func (h *stressHostApp) GetMetricsCount() int64 {
	return h.metricsCount.Load()
}

func (h *stressHostApp) GetAverageLatency() time.Duration {
	count := h.metricsCount.Load()
	if count == 0 {
		return 0
	}
	return time.Duration(h.totalLatencyNs.Load() / count)
}

func (h *stressHostApp) GetAverageProcessedTime() time.Duration {
	count := h.metricsCount.Load()
	if count == 0 {
		return 0
	}
	return time.Duration(h.totalProcessedNs.Load() / count)
}
