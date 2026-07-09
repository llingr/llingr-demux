// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// TestDrainTimeoutOrphan pins the documented drain-timeout orphan story
// through the real pipeline: a handler outlasts the revoke drain, the circuit
// breaker fires (expected), and the stranded work item's late completion after
// the partition is revoked must be discarded - never committed, never moving
// the broker's committed offset backwards, never panicking.
func TestDrainTimeoutOrphan(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newChurnBroker(t, 1, 30, 5)
	logger := mocklogger.NewRecordingLogger()

	const blockedKey = "key-0-1"
	releaseBlocked := make(chan struct{})
	var processed atomic.Int64
	processMessage := func(_ context.Context, message *nexus.Message[slamMessage]) error {
		if message.Key == blockedKey {
			<-releaseBlocked
		}
		processed.Add(1)
		return nil
	}
	writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
		return nil
	}
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }

	cfg := config.DemuxConfig{
		ConcurrentKeys:     4,
		WorkerShardsCount:  2,
		AutoCommitInterval: 50 * time.Millisecond,
		PollTimeout:        time.Millisecond,
		DrainTimeout:       200 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(metricsSink).
		WithExtractEnvelope(broker.ExtractEnvelope).
		WithOverflowGuard(make(chan struct{}, 2)).
		WithLogger(logger).
		Build(broker)

	// capture the expected emergency instead of the default os.Interrupt
	emergencyFired := make(chan error, 1)
	demuxConsumer := consumer.(*demux.Consumer[slamMessage]) //nolint:forcetypeassert // test: known type from builder
	demuxConsumer.RegisterShutdownCallback(func(_ context.Context, reason error) {
		if reason != nil {
			select {
			case emergencyFired <- reason:
			default:
			}
		}
	})

	initialAssignDone := make(chan struct{})
	broker.rebalanceFunc = func() {
		committed := broker.resumeDelivery(0)
		info := []nexus.RebalanceInfo{{
			RebalanceType: nexus.Assign, TopicName: topicName,
			Partition: 0, CommittedOffset: committed,
		}}
		if err := consumer.TriggerRebalance(nexus.Assign, info); err != nil {
			t.Errorf("initial TriggerRebalance failed: %v", err)
		}
		close(initialAssignDone)
	}

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	select {
	case <-initialAssignDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the initial assignment")
	}

	// let the unblocked keys process and the blocker take its worker hostage
	deadline := time.Now().Add(5 * time.Second)
	for processed.Load() < 20 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if processed.Load() < 20 {
		t.Fatalf("only %d messages processed before the revoke, want >= 20", processed.Load())
	}

	// revoke on a side goroutine while the blocked worker cannot drain
	broker.stopDelivery(0)
	revokeReturned := make(chan error, 1)
	go func() {
		revoke := []nexus.RebalanceInfo{{
			RebalanceType: nexus.Revoke, TopicName: topicName, Partition: 0, CommittedOffset: -1,
		}}
		revokeReturned <- consumer.TriggerRebalance(nexus.Revoke, revoke)
	}()

	// the drain must time out and escalate
	select {
	case reason := <-emergencyFired:
		t.Logf("expected emergency shutdown fired: %v", reason)
	case <-time.After(5 * time.Second):
		t.Fatal("drain timeout did not trigger the circuit breaker")
	}
	select {
	case err := <-revokeReturned:
		t.Logf("revoke returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("revoke did not return after the drain timeout")
	}

	broker.mu.Lock()
	committedBefore := broker.committedNext[0]
	broker.mu.Unlock()

	// the late orphan completion: the revoked partition's stranded work item
	// finishes long after the handoff
	close(releaseBlocked)

	// give the completion time to flow through the committer and several
	// commit ticks time to fire; the mock's monotonic guard and the
	// ownership discard are under test here
	time.Sleep(300 * time.Millisecond)

	broker.mu.Lock()
	committedAfter := broker.committedNext[0]
	broker.mu.Unlock()
	if committedAfter != committedBefore {
		t.Errorf("orphan completion moved the committed offset on a revoked partition: %d -> %d",
			committedBefore, committedAfter)
	}

	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown after emergency: %v", err)
	}
}
