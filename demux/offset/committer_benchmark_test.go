// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// benchMetricsPort mocks metrics collection for benchmarking.
// Counts collected items and returns them to the pool immediately,
// bypassing the real metrics collector's channel overhead.
type benchMetricsPort[T any] struct {
	pool  *alloc.WorkItemsPool[T]
	count *atomic.Int64
}

func (m *benchMetricsPort[T]) Collect(workItem *ports.WorkItem[T]) {
	m.count.Add(1)
	m.pool.Return(workItem)
}

var _ ports.MetricsPort[string] = (*benchMetricsPort[string])(nil)

// BenchmarkCommitter_Throughput measures committer throughput with 24 partitions.
//
// Uses contiguous offsets to exercise the fast path (immediate watermark advance).
// Mocks broker commit and metrics collection to isolate committer performance.
//
// The benchmark measures the complete CollectAndCommit → ingest → processCommit →
// watermark advance → metrics collect cycle.
func BenchmarkCommitter_Throughput(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond, // minimum allowed
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// no-op broker commit - just return success
	var commitCount atomic.Int64
	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitCount.Add(int64(len(messages)))
		return messages, nil
	}

	// minimal metrics mock - count and return to pool
	var collectedCount atomic.Int64
	metricsPort := &benchMetricsPort[string]{
		pool:  pool,
		count: &collectedCount,
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsPort, logger)

	// mark all 24 partitions as assigned (required for commits)
	const partitionCount = 24
	for p := int32(0); p < partitionCount; p++ {
		committer.MarkPartitionAssigned(p)
	}

	// track per-partition offsets for contiguous sequences
	partitionOffsets := make([]int64, partitionCount)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		partition := int32(i % partitionCount) //nolint:gosec // G115: partitionCount bounded
		offset := partitionOffsets[partition]
		partitionOffsets[partition]++

		workItem := pool.Borrow()
		workItem.Message.Partition = partition
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = partition
		workItem.Metrics.Offset = offset

		if offset == 0 {
			workItem.First = true
		} else {
			workItem.PreviousOffset = offset - 1
		}

		committer.CollectAndCommit(workItem)
	}

	b.StopTimer()

	// With contiguous offsets, all items except one per partition (Ready)
	// are collected immediately. Wait for those first.
	expectedMinCollected := int64(b.N)
	if b.N > partitionCount {
		expectedMinCollected = int64(b.N - partitionCount)
	}

	deadline := time.Now().Add(30 * time.Second)
	for collectedCount.Load() < expectedMinCollected {
		if time.Now().After(deadline) {
			b.Fatalf("timeout waiting for collection: got %d, expected at least %d",
				collectedCount.Load(), expectedMinCollected)
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}

	// trigger commit to collect remaining Ready items (one per partition)
	_ = committer.CommitOffsets()

	// wait for final collection
	for collectedCount.Load() < int64(b.N) {
		if time.Now().After(deadline) {
			// not fatal - Ready items may not all commit in edge cases
			b.Logf("final collection: %d/%d (some Ready items may be pending)",
				collectedCount.Load(), b.N)
			break
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}

	b.Logf("partitions=%d, collected=%d, broker_commits=%d",
		partitionCount, collectedCount.Load(), commitCount.Load())

	// assert allocation invariant
	allocs := testing.AllocsPerRun(100, func() {
		partition := int32(partitionOffsets[0] % partitionCount) //nolint:gosec // G115: partitionCount bounded
		offset := partitionOffsets[partition]
		partitionOffsets[partition]++

		workItem := pool.Borrow()
		workItem.Message.Partition = partition
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = partition
		workItem.Metrics.Offset = offset
		workItem.PreviousOffset = offset - 1

		committer.CollectAndCommit(workItem)
	})
	if allocs > 0 {
		b.Fatalf("alloc regression: expected 0 allocs/op, got %.2f", allocs)
	}
}

// TestAllocs_Committer asserts zero allocations for the CollectAndCommit hot path.
func TestAllocs_Committer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return messages, nil
	}

	var collectedCount atomic.Int64
	metricsPort := &benchMetricsPort[string]{
		pool:  pool,
		count: &collectedCount,
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsPort, logger)

	const partitionCount = 24
	for p := int32(0); p < partitionCount; p++ {
		committer.MarkPartitionAssigned(p)
	}

	partitionOffsets := make([]int64, partitionCount)

	// warm up
	for i := 0; i < 100; i++ {
		partition := int32(i % partitionCount) //nolint:gosec // G115: partitionCount bounded
		offset := partitionOffsets[partition]
		partitionOffsets[partition]++

		workItem := pool.Borrow()
		workItem.Message.Partition = partition
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = partition
		workItem.Metrics.Offset = offset
		if offset == 0 {
			workItem.First = true
		} else {
			workItem.PreviousOffset = offset - 1
		}
		committer.CollectAndCommit(workItem)
	}

	// wait for warmup to process
	time.Sleep(50 * time.Millisecond)

	allocs := testing.AllocsPerRun(1000, func() {
		partition := int32(partitionOffsets[0] % partitionCount) //nolint:gosec // G115: partitionCount bounded
		offset := partitionOffsets[partition]
		partitionOffsets[partition]++

		workItem := pool.Borrow()
		workItem.Message.Partition = partition
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = partition
		workItem.Metrics.Offset = offset
		workItem.PreviousOffset = offset - 1

		committer.CollectAndCommit(workItem)
	})

	if allocs > 0 {
		t.Errorf("expected 0 allocs/op, got %.2f", allocs)
	}
}
