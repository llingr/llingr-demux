// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// benchPayload contains pre-computed key, partition and offset for routing.
// Ctx is not used by the pipeline for addressing.
type benchPayload struct {
	key       string
	partition int32
	offset    int64
}

// benchCommitter implements ports.CommitterPort, returning WorkItems to pool.
// No counting - this benchmark measures Process throughput.
type benchCommitter struct {
	pool *alloc.WorkItemsPool[benchPayload]
}

func (c *benchCommitter) CollectAndCommit(workItem *ports.WorkItem[benchPayload]) {
	c.pool.Return(workItem)
}

var _ ports.CommitterPort[benchPayload] = (*benchCommitter)(nil)

// Pre-allocated payloads: 100,000 with keys distributed across 24 partitions.
// Each partition gets ascending offsets to avoid duplicate detection errors.
// Generated once at package init to avoid allocation during benchmarks.
var benchPayloads = func() []benchPayload {
	const payloadCount = 100_000
	const partitionCount = 24

	// track offset per partition for ascending sequences
	partitionOffsets := make([]int64, partitionCount)

	payloads := make([]benchPayload, payloadCount)
	for i := 0; i < payloadCount; i++ {
		partition := int32(i % partitionCount)
		offset := partitionOffsets[partition]
		partitionOffsets[partition]++

		payloads[i] = benchPayload{
			key:       fmt.Sprintf("bench-key-%06d", i),
			partition: partition,
			offset:    offset,
		}
	}
	return payloads
}()

// Pre-allocated payloads for hot/cold path benchmark: 100,000 entries with
// keys grouped in 4s. Pattern: key-00000 x4, key-00001 x4, ...
// This creates 25,000 unique keys, each appearing 4 times consecutively.
// First occurrence exercises cold path (worker creation), subsequent 3
// exercise hot path (channel send to existing worker).
var benchPayloadsHotCold = func() []benchPayload {
	const payloadCount = 100_000
	const keyGroupSize = 4
	// 25,000 unique keys, each appearing 4 times consecutively
	const partitionCount = 24

	// track offset per partition for ascending sequences
	partitionOffsets := make([]int64, partitionCount)

	payloads := make([]benchPayload, payloadCount)
	for i := 0; i < payloadCount; i++ {
		keyIndex := i / keyGroupSize
		partition := int32(keyIndex % partitionCount)
		offset := partitionOffsets[partition]
		partitionOffsets[partition]++

		payloads[i] = benchPayload{
			key:       fmt.Sprintf("hc-key-%05d", keyIndex),
			partition: partition,
			offset:    offset,
		}
	}
	return payloads
}()

// BenchmarkPipeline_Throughput measures raw message throughput through the processor.
// Uses Processor.Process which handles guard acquisition internally.
// Includes worker startup cost - no warmup phase.
func BenchmarkPipeline_Throughput(b *testing.B) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:    250,
		PerKeyBufferLen:   16,
		WorkerShardsCount: 16,
	}
	cfg.SetDemuxConfigDefaults()

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 150)

	pool := alloc.NewWorkItemsPool[benchPayload](cfg)
	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[benchPayload](func(_ context.Context, _ *nexus.Message[benchPayload], _ error) error {
		return nil
	}, logger)

	committer := &benchCommitter{pool: pool}

	// no-op ProcessMessage
	processMessage := func(_ context.Context, _ *nexus.Message[benchPayload]) error {
		return nil
	}

	demux := NewDemux(cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[benchPayload]) {})

	// ExtractEnvelope returns pre-computed key, partition and offset.
	// Ctx not used in pipeline - ProcessMessage receives it but we no-op.
	extractEnvelope := func(p benchPayload) nexus.Envelope {
		return nexus.Envelope{
			Key:       p.key,
			Partition: p.partition,
			Offset:    p.offset,
		}
	}

	processor := NewProcessor(ctx, guard, overflowGuard, demux, cfg, extractEnvelope, pool, logger)

	// lift length to keep in stack frame
	payloadCount := len(benchPayloads)

	// pre-compute zero time to avoid any time ops in hot path
	var zeroTime time.Time

	// track per-partition offsets to ensure ascending sequences
	const partitionCount = 24
	partitionOffsets := make([]int64, partitionCount)

	b.ReportAllocs()
	b.ResetTimer()

	var errCount int
	for i := 0; i < b.N; i++ {
		payload := benchPayloads[i%payloadCount]
		// override offset with ascending value to avoid duplicate errors
		payload.offset = partitionOffsets[payload.partition]
		partitionOffsets[payload.partition]++
		if err := processor.Process(payload, zeroTime); err != nil {
			errCount++
		}
	}

	b.StopTimer()
	demux.DrainWorkers()

	if errCount > 0 {
		b.Errorf("got %d errors from Process", errCount)
	}

	// assert allocation invariant (1 alloc for envelope struct)
	allocs := testing.AllocsPerRun(100, func() {
		payload := benchPayloads[0]
		payload.offset = partitionOffsets[payload.partition]
		partitionOffsets[payload.partition]++
		_ = processor.Process(payload, zeroTime)
	})
	if allocs > 1 {
		b.Fatalf("alloc regression: expected <= 1 allocs/op, got %.2f", allocs)
	}
}

// BenchmarkWorkerShard_HotColdPaths measures worker coordination throughput
// with mixed hot/cold path execution. Uses single shard to isolate worker
// coordination from hash distribution.
//
// Keys are grouped in 4s (25% cold, 75% hot):
//   - 1st message: cold path (worker borrow, goroutine start, map insertion)
//   - 2nd-4th messages: hot path (channel send, guard release)
func BenchmarkWorkerShard_HotColdPaths(b *testing.B) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:    250,
		PerKeyBufferLen:   16,
		WorkerShardsCount: 2, // minimal shards to isolate worker coordination
	}
	cfg.SetDemuxConfigDefaults()

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 150)

	pool := alloc.NewWorkItemsPool[benchPayload](cfg)
	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[benchPayload](func(_ context.Context, _ *nexus.Message[benchPayload], _ error) error {
		return nil
	}, logger)

	committer := &benchCommitter{pool: pool}

	// no-op ProcessMessage
	processMessage := func(_ context.Context, _ *nexus.Message[benchPayload]) error {
		return nil
	}

	demux := NewDemux(cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[benchPayload]) {})

	extractEnvelope := func(p benchPayload) nexus.Envelope {
		return nexus.Envelope{
			Key:       p.key,
			Partition: p.partition,
			Offset:    p.offset,
		}
	}

	processor := NewProcessor(ctx, guard, overflowGuard, demux, cfg, extractEnvelope, pool, logger)

	payloadCount := len(benchPayloadsHotCold)

	var zeroTime time.Time

	// track per-partition offsets to ensure ascending sequences
	const partitionCount = 24
	partitionOffsets := make([]int64, partitionCount)

	b.ReportAllocs()
	b.ResetTimer()

	var errCount int
	for i := 0; i < b.N; i++ {
		payload := benchPayloadsHotCold[i%payloadCount]
		payload.offset = partitionOffsets[payload.partition]
		partitionOffsets[payload.partition]++
		if err := processor.Process(payload, zeroTime); err != nil {
			errCount++
		}
	}

	b.StopTimer()
	demux.DrainWorkers()

	if errCount > 0 {
		b.Errorf("got %d errors from Process", errCount)
	}

	// assert allocation invariant (1 alloc for envelope struct)
	allocs := testing.AllocsPerRun(100, func() {
		payload := benchPayloadsHotCold[0]
		payload.offset = partitionOffsets[payload.partition]
		partitionOffsets[payload.partition]++
		_ = processor.Process(payload, zeroTime)
	})
	if allocs > 1 {
		b.Fatalf("alloc regression: expected <= 1 allocs/op, got %.2f", allocs)
	}
}

// TestAllocs_Pipeline asserts at most 1 allocation per Process call.
// The single allocation (32 bytes) is from the envelope struct in the hot path.
func TestAllocs_Pipeline(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:    250,
		PerKeyBufferLen:   16,
		WorkerShardsCount: 16,
	}
	cfg.SetDemuxConfigDefaults()

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 150)

	pool := alloc.NewWorkItemsPool[benchPayload](cfg)
	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[benchPayload](func(_ context.Context, _ *nexus.Message[benchPayload], _ error) error {
		return nil
	}, logger)

	committer := &benchCommitter{pool: pool}

	processMessage := func(_ context.Context, _ *nexus.Message[benchPayload]) error {
		return nil
	}

	demux := NewDemux(cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[benchPayload]) {})
	defer demux.DrainWorkers()

	extractEnvelope := func(p benchPayload) nexus.Envelope {
		return nexus.Envelope{
			Key:       p.key,
			Partition: p.partition,
			Offset:    p.offset,
		}
	}

	processor := NewProcessor(ctx, guard, overflowGuard, demux, cfg, extractEnvelope, pool, logger)

	payloadCount := len(benchPayloads)
	var zeroTime time.Time

	const partitionCount = 24
	partitionOffsets := make([]int64, partitionCount)

	allocs := testing.AllocsPerRun(1000, func() {
		payload := benchPayloads[partitionOffsets[0]%int64(payloadCount)]
		payload.offset = partitionOffsets[payload.partition]
		partitionOffsets[payload.partition]++
		_ = processor.Process(payload, zeroTime)
	})

	if allocs > 1 {
		t.Errorf("expected at most 1 alloc/op, got %.2f", allocs)
	}
}

// TestAllocs_WorkerShard asserts at most 1 allocation per Process call
// for the hot/cold path benchmark scenario.
func TestAllocs_WorkerShard(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:    250,
		PerKeyBufferLen:   16,
		WorkerShardsCount: 2,
	}
	cfg.SetDemuxConfigDefaults()

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 150)

	pool := alloc.NewWorkItemsPool[benchPayload](cfg)
	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[benchPayload](func(_ context.Context, _ *nexus.Message[benchPayload], _ error) error {
		return nil
	}, logger)

	committer := &benchCommitter{pool: pool}

	processMessage := func(_ context.Context, _ *nexus.Message[benchPayload]) error {
		return nil
	}

	demux := NewDemux(cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[benchPayload]) {})
	defer demux.DrainWorkers()

	extractEnvelope := func(p benchPayload) nexus.Envelope {
		return nexus.Envelope{
			Key:       p.key,
			Partition: p.partition,
			Offset:    p.offset,
		}
	}

	processor := NewProcessor(ctx, guard, overflowGuard, demux, cfg, extractEnvelope, pool, logger)

	payloadCount := len(benchPayloadsHotCold)
	var zeroTime time.Time

	const partitionCount = 24
	partitionOffsets := make([]int64, partitionCount)

	allocs := testing.AllocsPerRun(1000, func() {
		payload := benchPayloadsHotCold[partitionOffsets[0]%int64(payloadCount)]
		payload.offset = partitionOffsets[payload.partition]
		partitionOffsets[payload.partition]++
		_ = processor.Process(payload, zeroTime)
	})

	if allocs > 1 {
		t.Errorf("expected at most 1 alloc/op, got %.2f", allocs)
	}
}
