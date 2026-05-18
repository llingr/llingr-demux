// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package pipeline

import (
	"sort"
	"time"

	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-demux/demux/pipeline/fnv"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Demux fans out from single-threaded polling, delegating to concurrent workers to process
// messages in parallel. This approach solves the fundamental tension between throughput and
// ordering in streaming systems, eliminates head-of-line blocking, delivers predictable
// latency and high throughput while preserving the strict ordering within each key.
//
// Workers are sharded, primarily to reduce mutex contention: each shard has its own mutex
// rather than a single, global mutex.
//
// WorkItems are consistently distributed across WorkerShards, deriving a shard index from
// fnv.HashIndex(partitionKey). Each shard has its own map of per-partition-key workers,
// ensuring the same keys always route to the same worker shard and worker. Workers are pooled
// and reused to reduce allocation/GC pressure:
//
//	  Message Broker
//	    → Subscription.PollAndForward
//	    → Processor.Process
//		→ Demux.SendToWorkerForProcessing
//		→ WorkerShard / mutex
//		→ Worker / channel
//		→ nexus.ProcessMessage[T]
type Demux[T any] struct {
	workerShards   []*WorkerShard[T]
	concurrentKeys int
	awaitRateLimit func(*nexus.Message[T]) // always non-nil, default no-op
}

// NewDemux creates worker shards for by-partition-key nexus.WorkItem fan-out
func NewDemux[T any](demuxConfig config.DemuxConfig, processMessage nexus.ProcessMessage[T],
	deadLetter *deadletter.DeadLetter[T], committer ports.CommitterPort[T],
	circuitBreaker *circuitbreaker.CircuitBreaker, guard chan struct{}, overflowGuard chan struct{},
	logger nexus.Logger, awaitRateLimit func(*nexus.Message[T])) *Demux[T] {

	// lift from interface once - keeps function pointer on stack frame
	collectAndCommit := committer.CollectAndCommit

	// Performance benchmarking has established 16 shards are optimal for default settings
	shardsCount := demuxConfig.WorkerShardsCount
	workerShards := make([]*WorkerShard[T], shardsCount)
	for i := 0; i < shardsCount; i++ {
		workerShards[i] = NewWorkerShard(processMessage, deadLetter, collectAndCommit, circuitBreaker,
			guard, overflowGuard, demuxConfig, logger)
	}

	return &Demux[T]{
		workerShards:   workerShards,
		concurrentKeys: demuxConfig.ConcurrentKeys,
		awaitRateLimit: awaitRateLimit,
	}
}

const retrySpinDelay = 100 * time.Microsecond

// SendToWorkerForProcessing delegates processing to a concurrent worker.
// This is the 'fan-out' point for the demux processor.
func (c *Demux[T]) SendToWorkerForProcessing(partitionKey string, workItem *ports.WorkItem[T]) {
	c.awaitRateLimit(workItem.Message)

	// find processor shard
	workerShards := c.workerShards
	bitMask := uint32(len(workerShards) - 1) //nolint - shards count bounded by validated config
	hashIndex := fnv.HashIndex(partitionKey, bitMask)
	workerShard := workerShards[hashIndex]
	workItem.WorkerPool = hashIndex

retrySend: // goto preferred over wrapping loop; retry is rare and label makes intent explicit
	// fan out by key, with slightly different behaviours to reduce coordination pressure:
	//  - existing: has to lock so worker doesn't self-terminate while a message is being added
	//  - new: won't self-terminate because new worker has empty partitionKey
	workerShard.mu.Lock()
	workers := workerShard.workers
	worker, exists := workers[partitionKey]
	if exists {
		metrics := workItem.Metrics
		// cache before send: workItem may be recycled after worker receives it
		usedOverflow := metrics.Traits&nexus.UsedOverflow != 0

		// useful for understanding slow drain times
		metrics.QueueDepth = int32(len(worker.workItems))

		// send within mutex to prevent cleanup race, otherwise a worker's processUntilEmpty()
		// may fall through, see an empty channel and remove itself from workerShards map
		select {
		case worker.workItems <- workItem:
			workerShard.mu.Unlock()
		default:
			// worker channel is full, but holding mutex means that if the worker is
			// in the fall-through/return, so it won't be able to acquire the lock to
			// do this: yield to let this run (rare edge case)
			workerShard.mu.Unlock()
			time.Sleep(retrySpinDelay)
			goto retrySend
		}

		// don't need guard because item is pipelined in existing worker
		if !usedOverflow {
			<-worker.guard
		} else {
			<-worker.overflowGuard
		}

	} else {
		worker = workerShard.BorrowWorker()
		worker.IsActive = true
		workers[partitionKey] = worker
		workerShard.activeCount = len(workers)
		workerShard.mu.Unlock()
		// send AFTER unlock, worker will not self terminate until the first
		// message because 'cold path' workItem will be nil.
		//
		// This mitigates any convoy effect on (other) workers in the shard
		// waiting to clean up: those workers can obtain the lock and clean up sooner.
		worker.workItems <- workItem
	}
}

// ShardSnapshots returns a point-in-time view of each worker shard.
// Uses lock-free atomic reads for active counts and channel len for pooled counts.
func (c *Demux[T]) ShardSnapshots() []snapshot.ShardSnapshot {
	shards := make([]snapshot.ShardSnapshot, len(c.workerShards))
	for i, ws := range c.workerShards {
		ws.mu.Lock()
		activeWorkers := ws.activeCount
		ws.mu.Unlock()
		shards[i] = snapshot.ShardSnapshot{
			Shard:         i,
			ActiveWorkers: activeWorkers,
			PooledWorkers: ws.pooledCount(),
		}
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].Shard < shards[j].Shard
	})
	return shards
}

// DrainWorkers waits for in-flight processing
// in each Worker to complete
func (c *Demux[T]) DrainWorkers() {
	for _, s := range c.workerShards {
		// each shard has its own collection of active workers
		s.mu.Lock()
		workersCount := len(s.workers)
		if workersCount == 0 {
			s.mu.Unlock()
			continue
		}
		workers := make([]*Worker[T], 0, workersCount)
		for _, w := range s.workers {
			workers = append(workers, w)
		}
		s.mu.Unlock()

		for _, w := range workers {
			// drain loop aligns to the slowest worker
			w.Drain()
		}
	}
}
