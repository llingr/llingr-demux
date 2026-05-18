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
	"github.com/llingr/llingr-nexus/nexus"
)

// TestShutdown_DrainsMetricsCollector proves that Shutdown() waits for the
// metrics collector to flush all buffered callbacks before returning.
//
// The metrics sink deliberately sleeps 100µs per callback, creating a backlog
// that cannot physically drain during the worker drain phase alone. If Shutdown()
// does not call metricsCollector.Stop() (which blocks until the collector goroutine
// drains its channel), this test will fail with a metrics count short of totalMessages.
func TestShutdown_DrainsMetricsCollector(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	const (
		numKeys        = 20
		messagesPerKey = 50
		totalMessages  = numKeys * messagesPerKey
		sinkDelay      = 100 * time.Microsecond
	)

	// generate messages
	messages := make([]slamMessage, 0, totalMessages)
	for m := 0; m < messagesPerKey; m++ {
		for k := 0; k < numKeys; k++ {
			messages = append(messages, slamMessage{
				ID:        len(messages),
				Key:       fmt.Sprintf("key-%d", k),
				Partition: 0,
				Offset:    int64(len(messages)),
			})
		}
	}

	broker := newSlamBroker(messages)

	var processed atomic.Int64
	processMessage := func(_ context.Context, _ *nexus.Message[slamMessage]) error {
		processed.Add(1)
		return nil
	}

	writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
		return nil
	}

	// slow metrics sink: 100µs per callback. 1000 messages × 100µs = 100ms minimum
	// to drain, far longer than workers take to finish processing (near-instant).
	var metricsCount atomic.Int64
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		time.Sleep(sinkDelay)
		metricsCount.Add(1)
		return nil
	}

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:     numKeys,
		WorkerShardsCount:  4,
		AutoCommitInterval: 50 * time.Millisecond,
		PollTimeout:        1 * time.Millisecond,
		DrainTimeout:       5 * time.Second,
	}

	builder := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
		WithContext(context.Background()).
		WithDemuxConfig(cfg).
		WithMetricsSink(metricsSink).
		WithExtractEnvelope(broker.ExtractEnvelope).
		WithOverflowGuard(make(chan struct{}, 2)).
		WithLogger(logger)

	consumer := builder.Build(broker)

	// assign partition 0
	rebalanceDone := make(chan struct{})
	broker.rebalanceFunc = func() {
		time.Sleep(5 * time.Millisecond)
		if err := consumer.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{
			{Partition: 0},
		}); err != nil {
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

	// wait for all messages to be processed (fast - no processing delay)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() >= int64(totalMessages) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if count := processed.Load(); count < int64(totalMessages) {
		t.Fatalf("processed only %d of %d messages", count, totalMessages)
	}

	// at this point all messages are processed but the slow metrics sink
	// is still working through its backlog. Shutdown must wait for it.
	if err := consumer.Shutdown(); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	// after Shutdown returns, every metrics callback must have completed.
	if count := metricsCount.Load(); count != int64(totalMessages) {
		t.Errorf("metrics sink received %d callbacks after Shutdown(), want %d - "+
			"metricsCollector not drained during shutdown", count, totalMessages)
	}

	t.Logf("metrics drain: %d messages, sink delay %v, all %d metrics callbacks completed",
		totalMessages, sinkDelay, metricsCount.Load())
}
