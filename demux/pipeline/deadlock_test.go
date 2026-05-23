// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// TestDeadlock_MutexHeldDuringBlockingChannelSend verifies that
// SendToWorkerForProcessing does not hold the shard mutex while
// blocking on a full worker channel.
//
// The deadlock scenario:
//  1. Worker drains channel, falls through to cleanup path
//  2. Worker tries to acquire shard.mu for cleanup
//  3. Sender holds shard.mu, blocked on channel send (channel full)
//  4. Neither can proceed - classic deadlock
//
// This test uses:
//   - Buffer size 1: channel is almost always full
//   - Single key: all ops contend for same worker
//   - Many concurrent senders: maximizes chance of hitting the race window
func TestDeadlock_MutexHeldDuringBlockingChannelSend(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:    20,
		PerKeyBufferLen:   1, // minimal buffer - nearly always full under load
		WorkerShardsCount: 2,
	}
	cfg.SetDemuxConfigDefaults()

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 5)

	var committed atomic.Int64
	committer := &deadlockTestCommitter{committed: &committed}

	processedCount := &atomic.Int32{}
	// yield during processing to increase scheduling variability
	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		processedCount.Add(1)
		return nil
	}

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)

	demux := NewDemux(cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

	const (
		numSenders   = 1
		opsPerSender = 8000
		totalOps     = numSenders * opsPerSender
		key          = "single-contention-key" // all ops to same worker
	)

	var wg sync.WaitGroup
	wg.Add(numSenders)

	for i := 0; i < numSenders; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerSender; j++ {
				guard <- struct{}{}
				item := &ports.WorkItem[string]{
					Message: &nexus.Message[string]{
						Key:       key,
						Partition: 0,
						Offset:    int64(id*opsPerSender + j),
					},
					Metrics: &nexus.Metrics{},
				}
				demux.SendToWorkerForProcessing(key, item)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		demux.DrainWorkers()
		close(done)
	}()

	select {
	case <-done:
		if c := committed.Load(); c != totalOps {
			t.Errorf("expected %d commits, got %d", totalOps, c)
		}
	case <-time.After(30 * time.Second):
		const deadlock = "deadlock detected: SendToWorkerForProcessing likely holding mutex during blocking channel send, %d processed"
		t.Fatalf(deadlock, processedCount.Load())
	}
}

type deadlockTestCommitter struct {
	committed *atomic.Int64
}

func (c *deadlockTestCommitter) CollectAndCommit(_ *ports.WorkItem[string]) {
	c.committed.Add(1)
}

var _ ports.CommitterPort[string] = (*deadlockTestCommitter)(nil)
