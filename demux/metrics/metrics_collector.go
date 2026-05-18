// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Package metrics provides non-blocking metrics collection for delivery to a sink that
// captures per-message telemetry.
//
// The [Collector] receives completed work items after 'commit watermark' advancement
// (see: offset package), and processing uses a buffered channel to avoid back-pressure.
// If the sink can't keep up, metrics are dropped rather than blocking the commit path.
//
// Metrics include pipeline stage latencies and trait flags (ProcessError, DeadLetter,
// CommitBuffered, UsedOverflow) for filtering and aggregation. The collector deliberately
// excludes partition keys and payloads to prevent accidental data exposure to metrics
// backends.
package metrics

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Collector provides non-blocking metrics collection with drop tracking
// to prevent observability failures from impacting message processing.
//
// Sequences metrics using channel to support nexus.MetricsSink implementations
// that are not thread-safe, however - in order to ensure all metrics are
// captured quickly - the recommendation is to use thread-safe metrics components
// and fan out metrics collection to align with available system resources.
type Collector[T any] struct {
	workItems       chan *ports.WorkItem[T] // buffered channel for async metrics delivery
	metricsSink     nexus.MetricsSink       // pluggable metrics destination function, must be reliable
	sinkCtx         nexus.SinkContext       // instance-level identity for metrics routing, set once at construction
	CollectedCount  *atomic.Int64           // total metrics successfully collected
	workItemsPool   *alloc.WorkItemsPool[T] // WorkItem (borrow from pool) return/recycle
	DroppedCount    *atomic.Int64           // metrics lost due to Metrics channel backpressure
	SendFailedCount *atomic.Int64           // metrics lost due to nexus.MetricsSink errors
	quit            chan struct{}           // for orchestrated shutdown
	once            sync.Once               // quit only once
	ctx             context.Context         // app context for logging, control plane does not cancel
	logger          nexus.Logger            //
	wg              *sync.WaitGroup         // for orchestrated shutdown
}

// NewCollector creates a non-blocking, GC-friendly metrics collector.
// Buffers are auto-sized based on worker throughput config.
// Metrics are delivered serially to nexus.MetricsSink.
func NewCollector[T any](ctx context.Context, demuxConfig config.DemuxConfig,
	metricsSink nexus.MetricsSink, sinkCtx nexus.SinkContext,
	workItemsPool *alloc.WorkItemsPool[T],
	logger nexus.Logger) *Collector[T] {

	bufferSize := CalculateCollectBufferSize(demuxConfig.ConcurrentKeys, demuxConfig.PerKeyBufferLen)

	c := &Collector[T]{
		workItems:       make(chan *ports.WorkItem[T], bufferSize),
		metricsSink:     metricsSink,
		sinkCtx:         sinkCtx,
		workItemsPool:   workItemsPool,
		CollectedCount:  &atomic.Int64{},
		DroppedCount:    &atomic.Int64{},
		SendFailedCount: &atomic.Int64{},
		quit:            make(chan struct{}, 1),
		once:            sync.Once{},
		wg:              &sync.WaitGroup{},
		ctx:             ctx,
		logger:          logger,
	}
	return c
}

// Stats is a point-in-time view of metrics collection counters.
type Stats struct {
	Collected  int64 `json:"collected"`
	Dropped    int64 `json:"dropped"`
	SendFailed int64 `json:"sendFailed"`
}

// Stats returns a point-in-time view of metrics collection counters.
func (c *Collector[T]) Stats() Stats {
	return Stats{
		Collected:  c.CollectedCount.Load(),
		Dropped:    c.DroppedCount.Load(),
		SendFailed: c.SendFailedCount.Load(),
	}
}

// Collect captures metrics without blocking:
// safe for hot paths, drops on overflow.
func (c *Collector[T]) Collect(workItem *ports.WorkItem[T]) {
	select {
	case c.workItems <- workItem:

	default:
		// Attempt to minimize drops by yielding to give the
		// collect loop the opportunity to lift a message.
		runtime.Gosched()
		select {
		case c.workItems <- workItem:
		default:
			c.workItemsPool.Return(workItem)
			c.DroppedCount.Add(1)
		}
	}
}

// StartCollectingMetrics spawns the metrics collection goroutine
func (c *Collector[T]) StartCollectingMetrics() {
	c.wg.Add(1)
	go c.startCollectLoop()
}

// startCollectLoop delivers metrics to the
// registered nexus.MetricsSink (in order)
func (c *Collector[T]) startCollectLoop() {
	defer c.wg.Done()
	quit := false
	for {
		select {
		case workItem := <-c.workItems:
			// hot path pointers from cache to stack
			c.sendToMetricsSink(workItem, c.metricsSink, c.CollectedCount)

			// relies on upstream message-workers drain correctness
			if quit && len(c.workItems) == 0 {
				return
			}

		case <-c.quit:
			if !quit {
				const quitMessage = "metrics collector shutting down, final workItem will be drained"
				c.logger.Info(c.ctx, quitMessage)
				quit = true
			}
			if len(c.workItems) == 0 {
				return
			}
		}
	}
}

const sendFailed = "send metrics to nexus.MetricsSink failed - %s"

// sendToMetricsSink delivers metrics to the configured nexus.MetricsSink,
// converting panics to errors to protect processing
func (c *Collector[T]) sendToMetricsSink(workItem *ports.WorkItem[T], metricsSink nexus.MetricsSink,
	collectedCount *atomic.Int64) {

	message := workItem.Message
	metrics := workItem.Metrics
	metrics.WorkerPool = workItem.WorkerPool
	metrics.Partition = message.Partition
	metrics.Offset = message.Offset
	metrics.AddCustomTraits(message.Traits)

	defer func() {
		c.workItemsPool.Return(workItem)
		if r := recover(); r != nil {
			const sendMetricsPanic = "send metrics to nexus.MetricsSink panic - %v"
			err := fmt.Errorf(sendMetricsPanic, r)
			c.logger.Error(c.ctx, fmt.Sprintf(sendFailed, err))
			c.SendFailedCount.Add(1)
		}
	}()

	err := metricsSink(c.sinkCtx, *workItem.Metrics) // must be fast, reliable call
	if err != nil {
		c.logger.Warn(c.ctx, fmt.Sprintf(sendFailed, err)) // for operational visibility
		c.SendFailedCount.Add(1)
	} else {
		collectedCount.Add(1)
	}
}

// Stop signals metrics collector shutdown without blocking.
func (c *Collector[T]) Stop() {
	c.once.Do(func() {
		close(c.quit)
	})
	// Stop called after committer workers drain, so this won't block
	// indefinitely, only until the last committed messages
	// have made their way through the process pipeline
	c.wg.Wait()
}

// CalculateCollectBufferSize returns 5x message
// processing capacity, bounded 5K-250K (1Mb-40Mb)
func CalculateCollectBufferSize(concurrentKeys, perKeyBufferLength int) int {
	const (
		bufferSizeMultiplier = 5      // 5x max message processing capacity, subject to review
		minBufferSize        = 20000  // 40Mb with 2kb message size
		maxBufferSize        = 100000 // 200Mb with 2kb message size
	)
	bufferSize := bufferSizeMultiplier * concurrentKeys * perKeyBufferLength
	if bufferSize < minBufferSize {
		bufferSize = minBufferSize
	} else if bufferSize > maxBufferSize {
		bufferSize = maxBufferSize
	}
	return bufferSize
}
