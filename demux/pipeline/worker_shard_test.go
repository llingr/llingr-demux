// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

func testDemuxConfig() config.DemuxConfig {
	return config.DemuxConfig{
		ConcurrentKeys:            16,
		PerKeyBufferLen:           4,
		WorkerShardsCount:         4,
		AutoCommitInterval:        time.Second,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}
}

func TestNewWorkerShard(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cfg := testDemuxConfig()

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)

	guard := make(chan struct{}, 10)
	overflowGuard := make(chan struct{}, 5)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}
	collectAndCommit := func(_ *ports.WorkItem[string]) {}

	shard := NewWorkerShard[string](
		processMessage,
		dl,
		collectAndCommit,
		cb,
		guard,
		overflowGuard,
		cfg,
		logger,
	)

	// verify shard is properly initialized
	if shard == nil {
		t.Fatal("expected non-nil worker shard")
	}
	if shard.workers == nil {
		t.Error("expected non-nil workers map")
	}
	if shard.borrowWorker == nil {
		t.Error("expected non-nil borrowWorker func")
	}
	if shard.pooledCount == nil {
		t.Error("expected non-nil pooledCount func")
	}
	// pooledCount returns the count of idle workers in the pool - must
	// be non-negative and bounded by the pool capacity (guard + overflow)
	if got := shard.pooledCount(); got < 0 || got > 15 {
		t.Errorf("pooledCount() = %d, want in [0, 15]", got)
	}
	if shard.done.Load() {
		t.Error("expected done to be false initially")
	}
}

func TestNewWorkerShard_WarmsWorkers(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cfg := testDemuxConfig()
	cfg.ConcurrentKeys = 32
	cfg.WorkerShardsCount = 4

	// minIdleWorkers = (4 + 32 - 1) / 4 = 8

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)

	guard := make(chan struct{}, 32)
	overflowGuard := make(chan struct{}, 8)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}
	collectAndCommit := func(_ *ports.WorkItem[string]) {}

	shard := NewWorkerShard[string](
		processMessage,
		dl,
		collectAndCommit,
		cb,
		guard,
		overflowGuard,
		cfg,
		logger,
	)

	// shard should exist and be functional
	if shard == nil {
		t.Fatal("expected non-nil worker shard")
	}

	// borrow a worker to verify pool is working
	worker := shard.BorrowWorker()
	if worker == nil {
		t.Error("expected to borrow a worker")
	}
}

func TestWorkerShard_BorrowWorker(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cfg := testDemuxConfig()

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)

	guard := make(chan struct{}, 10)
	overflowGuard := make(chan struct{}, 5)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}
	collectAndCommit := func(_ *ports.WorkItem[string]) {}

	shard := NewWorkerShard[string](
		processMessage,
		dl,
		collectAndCommit,
		cb,
		guard,
		overflowGuard,
		cfg,
		logger,
	)

	// borrow multiple workers
	worker1 := shard.BorrowWorker()
	worker2 := shard.BorrowWorker()

	if worker1 == nil || worker2 == nil {
		t.Fatal("expected to borrow workers")
	}
	if worker1 == worker2 {
		t.Error("expected different worker instances")
	}

	// workers should have proper configuration injected
	if worker1.processMessage == nil {
		t.Error("expected processMessage to be set")
	}
	if worker1.circuitBreaker == nil {
		t.Error("expected circuitBreaker to be set")
	}
	if worker1.collectAndCommit == nil {
		t.Error("expected collectAndCommit to be set")
	}
	if worker1.deadLetter == nil {
		t.Error("expected deadLetter to be set")
	}
	if worker1.returnWorker == nil {
		t.Error("expected returnWorker to be set")
	}
}

func TestWorkerShard_DetectMainCtxDone(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cfg := testDemuxConfig()

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)

	guard := make(chan struct{}, 10)
	overflowGuard := make(chan struct{}, 5)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}
	collectAndCommit := func(_ *ports.WorkItem[string]) {}

	shard := NewWorkerShard[string](
		processMessage,
		dl,
		collectAndCommit,
		cb,
		guard,
		overflowGuard,
		cfg,
		logger,
	)

	// initially done should be false
	if shard.done.Load() {
		t.Error("expected done to be false initially")
	}

	// trigger circuit breaker
	cb.TriggerEmergencyShutdown(nil)

	// wait for detectMainCtxDone to notice
	time.Sleep(50 * time.Millisecond)

	// done should now be true
	if !shard.done.Load() {
		t.Error("expected done to be true after circuit breaker triggered")
	}
}
