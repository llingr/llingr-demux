// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package alloc provides object pooling for WorkItems to reduce GC pressure at high throughput.
//
// The [WorkItemsPool] pre-allocates and recycles WorkItem wrappers (which contain Message
// and Metrics structs) across the pipeline lifecycle: borrow at poll, return after metrics
// collection. This eliminates per-message heap allocations, reducing GC pauses.
package alloc

import (
	"sync"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// WorkItemsPool avoids per-message heap allocations which reduces
// heap fragmentation, malloc/free syscall overhead and GC pressure
// that manifests as unpredictable latency at high message rates.
//
// Each WorkItem's lifecycle spans the full pipeline:
//
//	Poll (borrow) → Process → Commit → Metrics (return)
//
// Return happens at metrics collection because that's where the lifecycle
// naturally ends, and not because MetricsSink 'does pool management'.
type WorkItemsPool[T any] struct {
	workItemPool *sync.Pool
}

// NewWorkItemsPool creates a pre-warmed pool, sized
// for throughput based on the supplied demux config
func NewWorkItemsPool[T any](demuxConfig config.DemuxConfig) *WorkItemsPool[T] {
	workItemsPool := &WorkItemsPool[T]{
		workItemPool: &sync.Pool{
			New: func() any {
				workItem := new(ports.WorkItem[T])
				workItem.Message = new(nexus.Message[T])
				workItem.Metrics = new(nexus.Metrics)
				workItem.PreviousOffset = -1 // sentinel value indicating "no previous offset"
				return workItem
			},
		},
	}
	workItemsPool.warmPool(demuxConfig)
	return workItemsPool
}

// Borrow acquires a *nexus.WorkItem[T] from *sync.Pool
func (p *WorkItemsPool[T]) Borrow() *ports.WorkItem[T] {
	return p.workItemPool.Get().(*ports.WorkItem[T]) //nolint:forcetypeassert // workItemPool.New same type
}

// pre-allocated zero values to avoid
// allocations during field reset
var (
	zeroStr  = ""
	zeroTime time.Time
)

// Return *nexus.WorkItem[T] to *sync.Pool, zeroing all
// fields to avoid data leakage between reused instances
func (p *WorkItemsPool[T]) Return(w *ports.WorkItem[T]) {
	message := w.Message
	message.Traits = 0
	message.Partition = 0
	message.Offset = 0
	message.Key = zeroStr
	message.Payload = nil

	metrics := w.Metrics
	metrics.Traits = 0
	metrics.QueueDepth = 0
	metrics.Partition = 0
	metrics.Offset = 0
	metrics.ProcessDuration = 0
	metrics.WriteDeadLetterDuration = 0
	metrics.ReadTime = zeroTime
	metrics.ProcessStartTime = zeroTime
	metrics.WatermarkAdvanceTime = zeroTime

	w.Ctx = nil
	w.PreviousOffset = -1
	w.First = false
	w.WorkerPool = 0

	p.workItemPool.Put(w)
}

// warmPool pre-allocates *nexus.WorkItem[T] objects
// to move the cost of heap allocations to startup
func (p *WorkItemsPool[T]) warmPool(demuxConfig config.DemuxConfig) {
	messagesPoolSize := CalcWorkItemPoolSize(demuxConfig)
	warmWorkItems := make([]*ports.WorkItem[T], messagesPoolSize)

	workItemPool := p.workItemPool
	for i := 0; i < messagesPoolSize; i++ {
		warmWorkItems[i] = workItemPool.Get().(*ports.WorkItem[T]) //nolint:forcetypeassert // pool.New guarantees type
	}
	for j := 0; j < messagesPoolSize; j++ {
		workItemPool.Put(warmWorkItems[j])
	}
}

// CalcWorkItemPoolSize for *sync.Pool based
// on throughput and WorkItem buffering
func CalcWorkItemPoolSize(demuxConfig config.DemuxConfig) int {
	const (
		// assume ~50 out-of-order messages per broker partition (rare)
		gapBufferSize = 50
		// for end of pipeline:  POLL message → process → COMMIT offset → METRICS COLLECT
		metricsCollectDelayHeadroom = 25

		minPoolSize = 5000
		maxPoolSize = 100_000
	)
	messagesInGapBuffers := demuxConfig.ConcurrentKeys * (gapBufferSize + metricsCollectDelayHeadroom)

	// per-key buffer length rarely filled, so extra margin is implicit
	messagesInPipeline := demuxConfig.ConcurrentKeys * demuxConfig.PerKeyBufferLen

	poolSize := messagesInPipeline + messagesInGapBuffers

	if poolSize < minPoolSize {
		// minimal memory usage providing burst
		// capacity to small deployments
		return minPoolSize
	} else if poolSize > maxPoolSize {
		// limit excessive memory usage,
		// roughly proxying ~100k TPS
		return maxPoolSize
	}
	return poolSize
}
