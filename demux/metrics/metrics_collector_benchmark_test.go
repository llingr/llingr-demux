// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package metrics

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

// Pre-allocated work items for benchmarking.
// 100,000 items with partition/offset set.
// Generated once at package init to avoid allocation during benchmarks.
var benchWorkItems = func() []*ports.WorkItem[string] {
	const itemCount = 100_000
	const partitionCount = 24

	items := make([]*ports.WorkItem[string], itemCount)
	for i := 0; i < itemCount; i++ {
		partition := int32(i % partitionCount)
		items[i] = &ports.WorkItem[string]{
			Message: &nexus.Message[string]{
				Partition: partition,
				Offset:    int64(i),
			},
			Metrics: &nexus.Metrics{
				Partition: partition,
				Offset:    int64(i),
			},
		}
	}
	return items
}()

// BenchmarkCollector_Throughput measures the throughput of the metrics
// collection pipeline: Collect() -> channel -> sink.
//
// Uses a no-op sink to measure pure channel throughput without sink overhead.
// Pre-allocated work items are cycled through to avoid allocation in hot path.
func BenchmarkCollector_Throughput(b *testing.B) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:  250,
		PerKeyBufferLen: 16,
	}

	pool := alloc.NewWorkItemsPool[string](cfg)

	var sinkCount atomic.Int64
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		sinkCount.Add(1)
		return nil
	}

	collector := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	collector.StartCollectingMetrics()

	itemCount := len(benchWorkItems)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		item := benchWorkItems[i%itemCount]
		// borrow from pool to simulate real usage (Collect returns to pool)
		workItem := pool.Borrow()
		workItem.Metrics.Partition = item.Metrics.Partition
		workItem.Metrics.Offset = item.Metrics.Offset
		collector.Collect(workItem)
	}

	b.StopTimer()

	// wait for all items to be processed (collected + dropped = total sent)
	for sinkCount.Load()+collector.DroppedCount.Load() < int64(b.N) {
		runtime.Gosched()
	}

	collector.Stop()

	bufferSize := CalculateCollectBufferSize(cfg.ConcurrentKeys, cfg.PerKeyBufferLen)
	dropped := collector.DroppedCount.Load()
	b.Logf("channel buffer: %d, dropped: %d (%.2f%%)",
		bufferSize, dropped, float64(dropped)/float64(b.N)*100)

	// restart collector for allocation check
	collector2 := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	collector2.StartCollectingMetrics()

	allocs := testing.AllocsPerRun(100, func() {
		item := benchWorkItems[0]
		workItem := pool.Borrow()
		workItem.Metrics.Partition = item.Metrics.Partition
		workItem.Metrics.Offset = item.Metrics.Offset
		collector2.Collect(workItem)
	})

	collector2.Stop()

	if allocs > 0 {
		b.Fatalf("alloc regression: expected 0 allocs/op, got %.2f", allocs)
	}
}

// TestAllocs_Collector asserts zero allocations for the Collect hot path.
func TestAllocs_Collector(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:  250,
		PerKeyBufferLen: 16,
	}

	pool := alloc.NewWorkItemsPool[string](cfg)

	var sinkCount atomic.Int64
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		sinkCount.Add(1)
		return nil
	}

	collector := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	collector.StartCollectingMetrics()

	itemCount := len(benchWorkItems)

	// warm up
	for i := 0; i < 100; i++ {
		item := benchWorkItems[i%itemCount]
		workItem := pool.Borrow()
		workItem.Metrics.Partition = item.Metrics.Partition
		workItem.Metrics.Offset = item.Metrics.Offset
		collector.Collect(workItem)
	}

	// wait for warmup to process
	time.Sleep(50 * time.Millisecond)

	allocs := testing.AllocsPerRun(1000, func() {
		item := benchWorkItems[sinkCount.Load()%int64(itemCount)]
		workItem := pool.Borrow()
		workItem.Metrics.Partition = item.Metrics.Partition
		workItem.Metrics.Offset = item.Metrics.Offset
		collector.Collect(workItem)
	})

	collector.Stop()

	if allocs > 0 {
		t.Errorf("expected 0 allocs/op, got %.2f", allocs)
	}
}
