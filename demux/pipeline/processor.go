// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Package pipeline implements the fan-out stage that transforms a single ordered message
// stream into concurrent per-key worker streams.
//
// The [Processor] receives messages from subscription polling, extracts envelope metadata,
// acquires a concurrency guard, and dispatches to [Demux]. The [Demux] routes each message
// to a [WorkerShard] where a [Worker] processes it by invoking the application's ProcessMessage
// callback.
//
// Sharding reduces mutex contention: 16 shards means 16 independent locks, allowing near-linear
// scaling on multi-core systems.
//
// Concurrency is bounded by guard channels (default: 250 concurrent keys). An optional overflow
// guard can be included to share burst capacity across multiple consumer instances.
package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/pipeline/prev"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

const (
	processorPrefix = "pipeline-processor: "
	acquireTimeout  = processorPrefix + "acquire worker timeout waiting to process partition %d, offset %d"
)

// Processor pipelines broker message payloads, by Key, to fan out
// partitions concurrently, mitigating head-of-line blocking to reduce
// per-key latency and increase throughput without breaking ordering
// semantics.
//
//	subscription.PollAndForward
//	  → Process
//	    → SendToWorkerForProcessing - fan out
//	      → offset.CollectAndCommit - fan in
type Processor[T any] struct {
	workItemsPool        *alloc.WorkItemsPool[T]  // pooled Message, Metrics, WorkItem
	extractEnvelope      nexus.ExtractEnvelope[T] // adapter addressing info, context etc.
	partitionOffsets     prev.PartitionOffsets
	timer                *time.Timer
	acquireWorkerTimeout time.Duration
	demux                *Demux[T]
	ctx                  context.Context
	logger               nexus.Logger
	guard                chan struct{} // concurrent Worker limit, heavily contended
	overflowGuard        chan struct{} // cross-consumer overflow capacity, distributes bursts
}

// NewProcessor pipeline to send messages to a host application concurrently while maintaining
// ordering within each partition key. Processed messages are sent to offset.Committer
func NewProcessor[T any](ctx context.Context, guard chan struct{}, overflowGuard chan struct{}, pipelineDemux *Demux[T],
	demuxConfig config.DemuxConfig, extractEnvelope nexus.ExtractEnvelope[T],
	workItemsPool *alloc.WorkItemsPool[T], logger nexus.Logger) *Processor[T] {

	// create timer in stopped state - only
	// activates on first Reset() in Process()
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

	processor := &Processor[T]{
		workItemsPool:        workItemsPool,
		extractEnvelope:      extractEnvelope,
		timer:                timer,
		acquireWorkerTimeout: demuxConfig.AcquireWorkerTimeoutCircuitBreaker,
		demux:                pipelineDemux,
		ctx:                  ctx,
		logger:               logger,
		guard:                guard,
		overflowGuard:        overflowGuard,
	}
	processor.partitionOffsets.Init()
	return processor
}

// Process called from the subscription polling loop to route each
// message payload to concurrent, per-partition-key workers.
//
// The circuit breaker prevents new messages from being processed
// when irrecoverable failures occur - application deadlocks,
// infrastructure failures - avoiding message loss.
//
// The guard channel is used to mitigate resource exhaustion, for example
// the channel length might align with a database connection pool size.
// This is configured using config.DemuxConfig and has default: 250
//
// Blocking call: which waits until a worker is active and has the message.
func (p *Processor[T]) Process(payload T, readTime time.Time) (err error) {
	var workItem *ports.WorkItem[T]
	workItem, err = p.toWorkItem(payload)
	if err != nil {
		return err
	}
	workItem.Metrics.ReadTime = readTime

	select {
	case p.guard <- struct{}{}:
		p.demux.SendToWorkerForProcessing(workItem.Message.Key, workItem)

	default:
		// amortized timer reset with optional overflow capacity
		// for applications consuming from multiple message streams
		p.timer.Reset(p.acquireWorkerTimeout)

		select {
		case p.guard <- struct{}{}:
			p.demux.SendToWorkerForProcessing(workItem.Message.Key, workItem)

		case p.overflowGuard <- struct{}{}:
			nexus.SetUsedOverflow(&workItem.Metrics.Traits)
			p.demux.SendToWorkerForProcessing(workItem.Message.Key, workItem)

		case <-p.timer.C:
			// deadlocked or overwhelmed infrastructure
			envelope := p.extractEnvelope(payload)
			return fmt.Errorf(acquireTimeout, envelope.Partition, envelope.Offset)
		}
	}

	return nil
}

// toWorkItem from payload
func (p *Processor[T]) toWorkItem(payload T) (workItem *ports.WorkItem[T], err error) {
	workItem = p.workItemsPool.Borrow()
	envelope := p.extractEnvelope(payload)

	workItem.PreviousOffset, workItem.First, err =
		p.partitionOffsets.GetPrevious(envelope.Partition, envelope.Offset)
	workItem.Ctx = envelope.Ctx
	message := workItem.Message
	message.Key = envelope.Key
	message.Partition = envelope.Partition
	message.Offset = envelope.Offset
	message.Payload = &payload

	return workItem, err
}

// ResetPrevOffsets after assignment so that commit high-watermark processing
// knows the first know offset on each partition
func (p *Processor[T]) ResetPrevOffsets(partitions []int32) {
	p.partitionOffsets.Reset(partitions...)
}
