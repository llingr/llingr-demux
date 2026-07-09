// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// TestStutter is the slam test's hostile sibling. Two properties are inverted
// from the friendly case:
//
//   - EVERY offset jumps by five (0, 5, 10, ...): no record is predecessor+1,
//     so any "+1" offset arithmetic anywhere in the commit path breaks on the
//     very first record. Contiguity evidence can only come from predecessor
//     linkage and recorded commits.
//   - every 2000 messages the producer goes quiet for 4x the commit interval,
//     so commit ticks repeatedly consume Ready and fire against a drained,
//     silent pipeline. A saturated stream masks over-eager promotion and
//     re-initialisation stalls alike (holes fill within milliseconds); the
//     stutter windows expose both, at every one of the ~25 boundaries.
const (
	stutterMessages     = 50_000
	stutterKeys         = 50
	stutterEvery        = 2000
	stutterCommitEvery  = 100 * time.Millisecond // same interval as the slam test
	stutterPause        = 4 * stutterCommitEvery
	stutterWaitDeadline = 5 * time.Minute
	stutterOffsetStride = 5 // every message jumps four offsets
)

// stutterBroker clones slamBroker's delivery and commit capture, adding the
// producer stutter: after every stutterEvery deliveries, Poll goes quiet for
// stutterPause. Poll runs only on the engine's polling goroutine, so the pause
// state needs no lock.
type stutterBroker struct {
	*slamBroker
	pauseUntil time.Time
	delivered  int
}

func newStutterBroker(messages []slamMessage) *stutterBroker {
	return &stutterBroker{slamBroker: newSlamBroker(messages)}
}

func (b *stutterBroker) Poll(timeout time.Duration) (slamMessage, bool, error) {
	// no delivery before the assignment is processed (see slamBroker.Poll)
	select {
	case <-b.subscribeCalled:
	default:
		time.Sleep(timeout)
		return slamMessage{}, false, nil
	}

	if time.Now().Before(b.pauseUntil) {
		time.Sleep(timeout)
		return slamMessage{}, false, nil
	}

	idx := int(b.pollIndex.Add(1) - 1)
	if idx >= len(b.messages) {
		time.Sleep(timeout)
		return slamMessage{}, false, nil
	}

	b.delivered++
	if b.delivered%stutterEvery == 0 {
		b.pauseUntil = time.Now().Add(stutterPause)
	}
	return b.messages[idx], true, nil
}

// generateStutterMessages creates N messages with K round-robin keys on
// partition 0, every offset jumping by stutterOffsetStride.
func generateStutterMessages(n, k int) []slamMessage {
	msgs := make([]slamMessage, n)
	offset := int64(0)
	for i := 0; i < n; i++ {
		msgs[i] = slamMessage{
			ID:        i,
			Key:       fmt.Sprintf("key-%d", i%k),
			Partition: 0,
			Offset:    offset,
		}
		offset += stutterOffsetStride
	}
	return msgs
}

func TestStutter(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages := generateStutterMessages(stutterMessages, stutterKeys)
	broker := newStutterBroker(messages)

	var metricsCount atomic.Int64

	processMessage := func(_ context.Context, _ *nexus.Message[slamMessage]) error {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond) //nolint:gosec // G404: test jitter
		return nil
	}
	writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
		return nil
	}
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		metricsCount.Add(1)
		return nil
	}

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:     4,
		WorkerShardsCount:  4,
		AutoCommitInterval: stutterCommitEvery,
		PollTimeout:        5 * time.Millisecond,
	}

	consumer := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(metricsSink).
		WithExtractEnvelope(broker.ExtractEnvelope).
		WithOverflowGuard(make(chan struct{}, 2)).
		WithLogger(logger).
		Build(broker)

	rebalanceDone := make(chan struct{})
	broker.rebalanceFunc = func() {
		time.Sleep(10 * time.Millisecond)
		if err := consumer.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{
			{Partition: 0},
		}); err != nil {
			t.Errorf("TriggerRebalance failed: %v", err)
		}
		close(rebalanceDone)
	}

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	select {
	case <-rebalanceDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for rebalance")
	}

	startTime := time.Now()
	deadline := time.Now().Add(stutterWaitDeadline)
	for time.Now().Before(deadline) {
		if metricsCount.Load() >= int64(stutterMessages) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if count := metricsCount.Load(); count < int64(stutterMessages) {
		t.Fatalf("stalled: only %d of %d messages processed (a stutter window "+
			"likely stranded the commit chain)", count, stutterMessages)
	}

	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // final commit cycle

	elapsed := time.Since(startTime)

	// --- Assertions ---
	history, highest, hasDuplicate := broker.getCommitResults()

	// 1. only real record offsets may ever be committed: with every offset
	// gapped, any arithmetic-derived offset is by construction nonexistent
	realOffsets := make(map[int64]struct{}, len(messages))
	for _, m := range messages {
		realOffsets[m.Offset] = struct{}{}
	}
	for i, rec := range history {
		if _, ok := realOffsets[rec.offset]; !ok {
			t.Errorf("commit %d: offset %d does not exist on the topic (offset arithmetic leak)",
				i, rec.offset)
			break
		}
	}

	// 2. final committed offset reaches the end of the gapped stream
	expectedHighest := messages[len(messages)-1].Offset
	if got, ok := highest[0]; !ok {
		t.Error("partition 0: no commits recorded")
	} else if got != expectedHighest {
		t.Errorf("partition 0: final committed offset = %d, want %d", got, expectedHighest)
	}

	// 3. no duplicate or regressing commits
	if hasDuplicate {
		t.Error("duplicate or regressing commit detected")
	}

	// 4. strictly monotonic commit history
	previous := int64(-1)
	for i, rec := range history {
		if rec.offset <= previous {
			t.Errorf("commit %d: offset %d <= previous %d (not monotonic)", i, rec.offset, previous)
			break
		}
		previous = rec.offset
	}

	// 5. all messages accounted for
	if count := metricsCount.Load(); count != int64(stutterMessages) {
		t.Errorf("metrics count = %d, want %d", count, stutterMessages)
	}

	t.Logf("stutter_test.go: %d messages, stride %d, %d quiet windows of %v: completed in %v",
		stutterMessages, stutterOffsetStride, stutterMessages/stutterEvery, stutterPause,
		elapsed.Round(time.Millisecond))
}
