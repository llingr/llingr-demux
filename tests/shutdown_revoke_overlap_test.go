// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// TestShutdownOverlappingRevokeDrain: a shutdown's drain (app goroutine) and a
// revoke's drain (a broker client's callback goroutine) can overlap, e.g.
// SIGTERM during a rebalance. Both must complete promptly; a lost drain wakeup
// turns a graceful stop into a drain timeout and an emergency shutdown.
func TestShutdownOverlappingRevokeDrain(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	iterations := 8
	if testing.Short() {
		iterations = 3
	}

	for iteration := 0; iteration < iterations; iteration++ {
		broker := newChurnBroker(t, 2, 400, 8)
		logger := mocklogger.NewRecordingLogger()

		processMessage := func(_ context.Context, _ *nexus.Message[slamMessage]) error {
			time.Sleep(500 * time.Microsecond) // keep workers busy at overlap time
			return nil
		}
		writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
			return nil
		}
		metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }

		cfg := config.DemuxConfig{
			ConcurrentKeys:     4,
			WorkerShardsCount:  2,
			AutoCommitInterval: 20 * time.Millisecond,
			PollTimeout:        time.Millisecond,
			DrainTimeout:       5 * time.Second,
		}

		ctx, cancel := context.WithCancel(context.Background())
		consumer := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
			WithContext(ctx).
			WithDemuxConfig(cfg).
			WithMetricsSink(metricsSink).
			WithExtractEnvelope(broker.ExtractEnvelope).
			WithOverflowGuard(make(chan struct{}, 2)).
			WithLogger(logger).
			Build(broker)

		// capture emergency shutdowns instead of the default os.Interrupt
		demuxConsumer := consumer.(*demux.Consumer[slamMessage]) //nolint:forcetypeassert // test: known type from builder
		demuxConsumer.RegisterShutdownCallback(func(_ context.Context, reason error) {
			if reason != nil {
				t.Errorf("iteration %d: emergency shutdown fired: %v", iteration, reason)
			}
		})

		initialAssignDone := make(chan struct{})
		broker.rebalanceFunc = func() {
			info := make([]nexus.RebalanceInfo, 2)
			for p := int32(0); p < 2; p++ {
				committed := broker.resumeDelivery(p)
				info[p] = nexus.RebalanceInfo{
					RebalanceType: nexus.Assign, TopicName: topicName,
					Partition: p, CommittedOffset: committed,
				}
			}
			if err := consumer.TriggerRebalance(nexus.Assign, info); err != nil {
				t.Errorf("iteration %d: initial TriggerRebalance failed: %v", iteration, err)
			}
			close(initialAssignDone)
		}

		if err := consumer.Subscribe(); err != nil {
			t.Fatalf("iteration %d: Subscribe failed: %v", iteration, err)
		}
		select {
		case <-initialAssignDone:
		case <-time.After(10 * time.Second):
			t.Fatalf("iteration %d: timed out waiting for the initial assignment", iteration)
		}

		time.Sleep(30 * time.Millisecond) // let traffic build so drains find live workers

		var overlap sync.WaitGroup
		overlap.Add(2)
		go func() {
			defer overlap.Done()
			broker.stopDelivery(0)
			revoke := []nexus.RebalanceInfo{{
				RebalanceType: nexus.Revoke, TopicName: topicName, Partition: 0, CommittedOffset: -1,
			}}
			if err := consumer.TriggerRebalance(nexus.Revoke, revoke); err != nil {
				// a revoke racing a shutdown may be rejected; it must not hang or panic
				t.Logf("iteration %d: revoke racing shutdown returned: %v", iteration, err)
			}
		}()
		var shutdownErr error
		go func() {
			defer overlap.Done()
			shutdownErr = consumer.Shutdown()
		}()

		overlapDone := make(chan struct{})
		go func() {
			overlap.Wait()
			close(overlapDone)
		}()
		select {
		case <-overlapDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: overlapped Shutdown and revoke did not complete within 5s", iteration)
		}

		if shutdownErr != nil {
			t.Errorf("iteration %d: Shutdown returned error: %v", iteration, shutdownErr)
		}
		if logger.HasErrors() {
			t.Errorf("iteration %d: engine logged errors: %v", iteration, logger.Errors())
		}
		cancel()
	}
}
