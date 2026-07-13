// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package pipeline

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// WorkerShard coordinates workers for a (hashed) range of partition keys.
//
// Each shard has a dedicated mutex, *sync.Pool and workers map, reducing
// lock contention, allocations and GC pressure - this improves performance
// and delivers more predictable latency.
type WorkerShard[T any] struct {
	mu           sync.Mutex            // sharded mutex, accessed by each Worker
	workers      map[string]*Worker[T] // partition key vs Worker
	borrowWorker func() *Worker[T]
	pooledCount  func() int // idle workers in pool (lock-free channel len)
	activeCount  int
	done         atomic.Bool
	_            [20]byte // pad shard to 64 bytes
}

// NewWorkerShard creates a shard with self-managing worker pool. Workers are
// pre-started with running goroutines and injected with shared coordination
// state (mutex, workers map, and shard reference). This enables workers to
// autonomously manage their lifecycle - returning themselves to the pool when
// idle without external coordination overhead.
func NewWorkerShard[T any](processMessage nexus.ProcessMessage[T],
	deadLetter *deadletter.DeadLetter[T],
	collectAndCommit func(*ports.WorkItem[T]),
	circuitBreaker *circuitbreaker.CircuitBreaker,
	guard chan struct{},
	overflowGuard chan struct{},
	demuxConfig config.DemuxConfig,
	logger nexus.Logger) *WorkerShard[T] {

	perKeyBufferLen := demuxConfig.PerKeyBufferLen
	workerChannelsMapSize := demuxConfig.CalcWorkerChannelsMapSize()

	workerPoolSize := cap(guard) + cap(overflowGuard)
	minIdleWorkers := demuxConfig.CalcMinimumIdleWorkers()
	workerPool := NewWorkerPool[T](workerPoolSize, minIdleWorkers, time.Second)

	workerShard := new(WorkerShard[T])
	workerShard.workers = make(map[string]*Worker[T], workerChannelsMapSize)
	workerShard.borrowWorker = workerPool.BorrowWorker
	workerShard.pooledCount = func() int { return len(workerPool.workerPool) }

	workerPool.createWorker = func() *Worker[T] {
		worker := NewWorker(workerShard, perKeyBufferLen, guard, overflowGuard, logger)
		worker.processMessage = processMessage
		worker.circuitBreaker = circuitBreaker
		worker.collectAndCommit = collectAndCommit
		worker.deadLetter = deadLetter
		worker.returnWorker = workerPool.ReturnWorker
		go worker.startProcessingWorkItems()
		return worker
	}

	go workerShard.detectMainCtxDone(circuitBreaker)

	warmedWorkers := make([]*Worker[T], minIdleWorkers)
	for i := 0; i < minIdleWorkers; i++ {
		warmedWorkers[i] = workerPool.BorrowWorker()
	}
	for j := 0; j < minIdleWorkers; j++ {
		workerPool.ReturnWorker(warmedWorkers[j])
	}

	return workerShard
}

// BorrowWorker from sync.Pool, called
// by the demux coordinator
func (ws *WorkerShard[T]) BorrowWorker() *Worker[T] {
	return ws.borrowWorker()
}

func (ws *WorkerShard[T]) detectMainCtxDone(circuitBreaker *circuitbreaker.CircuitBreaker) {
	go func() {
		<-circuitBreaker.MainCtxDone()
		ws.done.Store(true)
	}()
}
