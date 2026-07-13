// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// TestDrainLifecycle saturates the pipeline with in-flight work across all guard
// slots, then triggers a partition revoke to exercise the full drain lifecycle:
// workers drain → offsets flush → partitions marked revoked.
//
// Every key's first message sleeps 100ms (filling all guard slots with slow work).
// Subsequent messages per key sleep 0-10ms randomly. The revoke fires while the
// first messages are still processing, forcing the drain to wait for real in-flight
// work to complete before committing offsets.
//
// Assertions:
//   - Drain completes in 100-200ms (not too fast, not stuck)
//   - Every message is processed
//   - Every message appears in the metrics sink with correct partition/offset
//   - Final committed offset per partition equals the highest offset sent
//   - No duplicate or regressing offset commits
func TestDrainLifecycle(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	const (
		numKeys       = 50
		msgsPerKey    = 16
		numPartitions = 4
		totalMessages = numKeys * msgsPerKey // 800
	)

	// Generate messages interleaved: all first-per-key messages arrive before any
	// second-per-key, ensuring every guard slot fills with a slow (100ms) worker
	// before fast followers start queuing behind them.
	rng := rand.New(rand.NewSource(42)) //nolint:gosec
	messages := make([]slamMessage, 0, totalMessages)
	partitionCounters := make([]int64, numPartitions)

	for m := 0; m < msgsPerKey; m++ {
		for k := 0; k < numKeys; k++ {
			p := int32(k % numPartitions)
			var delay int64
			if m == 0 {
				delay = 100_000 // 100ms - keeps workers busy through the revoke
			} else {
				delay = int64(rng.Intn(10_000)) // 0-10ms - fast followers
			}
			messages = append(messages, slamMessage{
				ID:          len(messages),
				Key:         fmt.Sprintf("key-%d", k),
				Partition:   p,
				Offset:      partitionCounters[p],
				DelayMicros: delay,
			})
			partitionCounters[p]++
		}
	}

	broker := newSlamBroker(messages)

	// Track processed messages in the callback
	var processed atomic.Int64

	processMessage := func(_ context.Context, msg *nexus.Message[slamMessage]) error {
		if msg.Payload != nil && msg.Payload.DelayMicros > 0 {
			time.Sleep(time.Duration(msg.Payload.DelayMicros) * time.Microsecond)
		}
		processed.Add(1)
		return nil
	}

	writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
		return nil
	}

	// Track every metric callback: per-partition offset set for completeness check
	var metricsCount atomic.Int64
	var metricsMu sync.Mutex
	metricsByPartition := make(map[int32]map[int64]bool) // partition → set of offsets seen

	metricsSink := func(_ nexus.SinkContext, metrics nexus.Metrics) error {
		metricsCount.Add(1)
		metricsMu.Lock()
		if metricsByPartition[metrics.Partition] == nil {
			metricsByPartition[metrics.Partition] = make(map[int64]bool)
		}
		metricsByPartition[metrics.Partition][metrics.Offset] = true
		metricsMu.Unlock()
		return nil
	}

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:     numKeys, // all keys get a worker - every guard slot filled
		WorkerShardsCount:  8,
		AutoCommitInterval: 50 * time.Millisecond,
		PollTimeout:        1 * time.Millisecond,
		DrainTimeout:       5 * time.Second, // generous - test asserts much faster
	}

	builder := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
		WithContext(context.Background()).
		WithDemuxConfig(cfg).
		WithMetricsSink(metricsSink).
		WithExtractEnvelope(broker.ExtractEnvelope).
		WithOverflowGuard(make(chan struct{}, 2)).
		WithLogger(logger)

	consumer := builder.Build(broker)

	// Assign all partitions
	rebalanceDone := make(chan struct{})
	broker.rebalanceFunc = func() {
		time.Sleep(5 * time.Millisecond)
		partitions := make([]nexus.RebalanceInfo, numPartitions)
		for i := 0; i < numPartitions; i++ {
			partitions[i] = nexus.RebalanceInfo{Partition: int32(i)}
		}
		if err := consumer.TriggerRebalance(nexus.Assign, partitions); err != nil {
			t.Errorf("TriggerRebalance(Assign) failed: %v", err)
		}
		close(rebalanceDone)
	}

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	select {
	case <-rebalanceDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for assign")
	}

	// Let all 800 messages flood into the pipeline. At ~450K polls/sec the broker
	// exhausts its 800 messages in under 2ms. Wait 30ms so the first 100ms workers
	// are well underway but nowhere near finished.
	time.Sleep(30 * time.Millisecond)

	// Trigger revoke - blocks until drain completes (workers finish + offsets flush)
	partitions := make([]nexus.RebalanceInfo, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitions[i] = nexus.RebalanceInfo{Partition: int32(i)}
	}

	drainStart := time.Now()
	if err := consumer.TriggerRebalance(nexus.Revoke, partitions); err != nil {
		t.Fatalf("TriggerRebalance(Revoke) failed: %v", err)
	}
	drainDuration := time.Since(drainStart)

	// --- Timing ---
	// First messages are 100ms. We called revoke ~30ms in, so ~70ms remain.
	// Then 15 fast followers per key (0-10ms each). Total drain: ~100-180ms.
	if drainDuration < 50*time.Millisecond {
		t.Errorf("drain too fast (%v) - workers should still be processing 100ms messages", drainDuration)
	}
	if drainDuration > 500*time.Millisecond {
		t.Errorf("drain too slow (%v) - expected under 500ms", drainDuration)
	}

	// --- Offsets ---
	_, highest, hasDuplicate := broker.getCommitResults()

	for p := int32(0); p < int32(numPartitions); p++ {
		expected := partitionCounters[p] - 1
		if got, ok := highest[p]; !ok {
			t.Errorf("partition %d: no commits recorded", p)
		} else if got != expected {
			t.Errorf("partition %d: final committed offset = %d, want %d", p, got, expected)
		}
	}

	if hasDuplicate {
		t.Error("duplicate or regressing offset commit detected")
	}

	// --- Processing completeness ---
	if count := processed.Load(); count != int64(totalMessages) {
		t.Errorf("processed %d messages, want %d", count, totalMessages)
	}

	// Shutdown drains the metrics collector's async buffer before we assert
	// on metric counts (Drain only waits for workers + offsets, not metrics)
	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown: %v", err)
	}

	// --- Metrics completeness ---
	// Every message must have appeared in the metrics sink with the correct
	// partition and offset. This verifies the full pipeline path: poll → route →
	// worker → process → metrics → offset commit.
	if count := metricsCount.Load(); count != int64(totalMessages) {
		t.Errorf("metrics sink received %d callbacks, want %d", count, totalMessages)
	}

	metricsMu.Lock()
	for p := int32(0); p < int32(numPartitions); p++ {
		offsets := metricsByPartition[p]
		expectedCount := int(partitionCounters[p])
		if len(offsets) != expectedCount {
			t.Errorf("partition %d: metrics saw %d distinct offsets, want %d", p, len(offsets), expectedCount)
		}
		for o := int64(0); o < partitionCounters[p]; o++ {
			if !offsets[o] {
				t.Errorf("partition %d: offset %d missing from metrics", p, o)
				break // one per partition is enough
			}
		}
	}
	metricsMu.Unlock()

	t.Logf("drain lifecycle: %d messages (%d keys × %d/key), %d partitions, drained in %v",
		totalMessages, numKeys, msgsPerKey, numPartitions, drainDuration.Round(time.Millisecond))
}
