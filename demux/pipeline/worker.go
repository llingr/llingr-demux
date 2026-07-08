// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package pipeline

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

const (
	workerPrefix     = "pipeline-worker: "
	errorProcessing  = workerPrefix + "error processing partition: %d, offset: %d - %v"
	deadLetterFailed = workerPrefix + "circuit-breaker triggered processing partition %d, offset %d - %w"
)

// Worker processes messages - in order - for a single partition key.
//
// Multiple workers run concurrently to avoid head-of-line blocking
// and increase throughput.
//
// When messages have drained it returns itself to a worker pool to be
// used on a different, future partition key; it does not shut down
// when it is not receiving work items as the invariant 'partitionKey'
// can be relied upon by the higher-level Demux coordination.
type Worker[T any] struct {
	workItems        chan *ports.WorkItem[T]  // deliver by the ctrl, per-shard highly contention
	processMessage   nexus.ProcessMessage[T]  // in host app, **ALWAYS** a blocking call
	collectAndCommit func(*ports.WorkItem[T]) // to commit offset for processed messages
	guard            <-chan struct{}          // global Worker limit, heavily contended
	mu               *sync.Mutex              // protects workers, per-shard highly contended
	returnWorker     func(*Worker[T])         // return 'self' to WorkerShard's pool to be reused
	overflowGuard    <-chan struct{}          // cross-consumer overflow capacity, distributes bursts
	IsActive         bool                     // set to true when created or (re-)borrowed
	drained          *sync.Cond               // broadcast on empty, wakes every concurrent Drain()

	deadLetter     *deadletter.DeadLetter[T]      // called on work error or panic
	circuitBreaker *circuitbreaker.CircuitBreaker // worker detecting infra issue can trigger
	shutdown       chan struct{}
	workerShard    *WorkerShard[T]
	logger         nexus.Logger
}

// NewWorker creates a worker that processes messages for a single partition key.
func NewWorker[T any](workerShard *WorkerShard[T], bufferLen int,
	guard, overflowGuard <-chan struct{}, logger nexus.Logger) *Worker[T] {

	worker := &Worker[T]{
		workItems:     make(chan *ports.WorkItem[T], bufferLen),
		workerShard:   workerShard,
		mu:            &workerShard.mu,
		IsActive:      true, // set true on create (new worker) and borrow (existing from pool)
		logger:        logger,
		guard:         guard,
		overflowGuard: overflowGuard,
		drained:       sync.NewCond(&workerShard.mu),
		shutdown:      make(chan struct{}),
	}
	return worker
}

// startProcessingWorkItems processing messages delivered via the workItems channel.
// This will be parked by the scheduler when there are no work-items.
func (w *Worker[T]) startProcessingWorkItems() {
	defer func() {
		close(w.workItems)
	}()

	var (
		workItems          = w.workItems
		workItem           *ports.WorkItem[T]
		usedOverflow       = false
		workers            = w.workerShard.workers // map[string]*Worker[T]
		partitionKey       = ""
		commit             = w.collectAndCommit
		workerShardDone    = w.workerShard.done.Load
		returnWorkerToPool = false
	)

	processWorkItem := func(workItem *ports.WorkItem[T]) {
		if workerShardDone() {
			return
		}
		if errP := w.process(workItem); errP != nil {
			if errDL := w.writeDeadLetter(workItem, errP); errDL != nil {
				w.triggerCircuitBreaker(workItem, errDL)
				return // don't commit or message can be lost
			}
		}
		commit(workItem)
	}

	for {
		// workItem variable serves as both data and state:
		//  - nil means "idle worker" with no CPU spinning     - cold path
		//  - non-nil means "active worker draining partition" - hot path
		//
		// So workers either sleep efficiently or work at max
		// throughput with no middle ground wasting resources.
		//nolint:nestif // intentional state machine - nil/non-nil drives cold/hot path branching
		if workItem == nil {
			// Cold Start: blocking wait, worker sleeps until there is a message
			select {
			case workItem = <-w.workItems:
				// cache key before processor returns workItem to pool
				partitionKey = workItem.Message.Key
				usedOverflow = workItem.Metrics.Traits&nexus.UsedOverflow != 0
				processWorkItem(workItem)

			case <-w.shutdown:
				return
			}

		} else {
			// Hot Path: non-blocking workers drain with select fallthrough when empty.
			// Processes all available messages for partition key before stopping
			select {
			case workItem = <-workItems:
				processWorkItem(workItem)

			default:
				w.mu.Lock()              // synchronize with Demux.SendToWorkerForProcessing()
				if len(workItems) == 0 { // ensure no message arrived during fallthrough
					delete(workers, partitionKey) // cached key
					w.workerShard.activeCount = len(workers)
					returnWorkerToPool = true
					w.IsActive = false // boolean is set to true when borrowed (inside same mutex)
					w.drained.Broadcast() // wakes every Drain() waiter; no waiters is a no-op
				}
				w.mu.Unlock()

				if returnWorkerToPool {
					if usedOverflow {
						<-w.overflowGuard
					} else {
						<-w.guard
					}
					workItem = nil
					partitionKey = ""
					returnWorkerToPool = false
					w.returnWorker(w)
				}
			}
		}
	}
}

// process calls registered nexus.ProcessMessage function in host app
func (w *Worker[T]) process(workItem *ports.WorkItem[T]) (err error) {
	metrics := workItem.Metrics
	readTime := metrics.ReadTime
	processStartTime := readTime.Add(time.Since(readTime))

	defer func() {
		if r := recover(); r != nil {
			nexus.SetProcessPanic(&workItem.Message.Traits)
			err = fmt.Errorf("panic: %v, stack: %s", r, string(debug.Stack()))
		}
		processDuration := time.Since(processStartTime)
		metrics.ProcessDuration = processDuration
		metrics.ProcessStartTime = processStartTime
		if err != nil {
			nexus.SetProcessError(&workItem.Message.Traits)
			partition, partitionOffset := workItem.PartitionOffset()
			processError := fmt.Sprintf(errorProcessing, partition, partitionOffset, err)
			w.logger.Error(workItem.Ctx, processError)
		}
	}()

	err = w.processMessage(workItem.Ctx, workItem.Message)
	return err
}

// writeDeadLetter routes failed messages to registered nexus.WriteDeadLetter
// function to avoid total message loss; it should be possible to review and
// potentially replay dead-letters.
func (w *Worker[T]) writeDeadLetter(workItem *ports.WorkItem[T], errP error) (errDL error) {
	defer func() {
		if r := recover(); r != nil {
			errDL = fmt.Errorf("panic recovered: %v, stack: %s", r, string(debug.Stack()))
		}
		metrics := workItem.Metrics
		processStartTime := metrics.ProcessStartTime
		processDuration := metrics.ProcessDuration
		writeDuration := time.Since(processStartTime) - processDuration
		metrics.WriteDeadLetterDuration = writeDuration
		if errDL != nil {
			// writes must be simple/reliable, failures indicate a serious
			// infrastructure issue that is considered unrecoverable.
			errDL = fmt.Errorf("error writing dead-letter: %w", errDL)
		}
	}()
	nexus.SetDeadLetter(&workItem.Message.Traits)
	errDL = w.deadLetter.Write(workItem, errP)
	return
}

// triggerCircuitBreaker because processor could neither process message
// nor write a dead-letter. Treat this as an infrastructure issue: trigger
// circuit-breaker to avoid message loss (partitions will rebalance).
func (w *Worker[T]) triggerCircuitBreaker(workItem *ports.WorkItem[T], errDL error) {
	partition, partitionOffset := workItem.PartitionOffset()
	reason := fmt.Errorf(deadLetterFailed, partition, partitionOffset, errDL)
	w.circuitBreaker.TriggerEmergencyShutdown(reason)
}

// Drain waits for worker to complete processing all messages.
//
// ⏱️ Blocks indefinitely: rebalance time is the slowest message processing time.
//
// For slow (typically external) systems, architect nexus.ProcessMessage[T] handler
// to offload work before returning: for example a deep-copy or blocking/sequential
// persist to a database, then (after copy) invoke async processing with managed retries.
//
// Safe for concurrent waiters: Broadcast wakes them all. A waiter racing a pool
// re-borrow (IsActive true again) waits for the next empty; the drain
// coordinator's timeout bounds that, same as before.
func (w *Worker[T]) Drain() {
	w.mu.Lock()
	for w.IsActive {
		w.drained.Wait()
	}
	w.mu.Unlock()
}
