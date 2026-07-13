// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package pipeline

import (
	"math"
	"time"
)

// WorkerPool maintains pre-started workers for re-use,
// and gradually reduces unused instances.  This allows the system
// to rapidly scale but then slowly fade back to reduce the amount
// of information the scheduler needs to maintain.
type WorkerPool[T any] struct {
	workerPool   chan *Worker[T]
	createWorker func() *Worker[T] // set in NewWorkerShard
	tickDuration time.Duration
}

// NewWorkerPool of (lazy-created) running workers.
func NewWorkerPool[T any](size, minIdleWorkers int, tickDuration time.Duration) *WorkerPool[T] {
	wp := &WorkerPool[T]{
		workerPool:   make(chan *Worker[T], size),
		tickDuration: tickDuration,
	}
	go wp.pruneIdleWorkers(minIdleWorkers)
	return wp
}

// BorrowWorker either accesses an existing/running Worker, or creates
// a new one (creating a worker starts it)
func (wp *WorkerPool[T]) BorrowWorker() *Worker[T] {
	select {
	case worker := <-wp.workerPool:
		return worker
	default:
		return wp.createWorker()
	}
}

// ReturnWorker to pool for re-use assuming sufficient
// capacity; if the pipeline is completely saturated
// (rare) then the worker is stopped and destroyed.
func (wp *WorkerPool[T]) ReturnWorker(worker *Worker[T]) {
	select {
	case wp.workerPool <- worker:
	default:
		// return from worker loop (exits go-routine)
		close(worker.shutdown)
	}
}

// pruneIdleWorkers gradually shrink
// the workers pool after bursts
func (wp *WorkerPool[T]) pruneIdleWorkers(minIdleWorkers int) {
	ticker := time.NewTicker(wp.tickDuration)
	defer ticker.Stop()
	prevLen := math.MaxInt

	for range ticker.C {
		poolSize := len(wp.workerPool)
		if poolSize > minIdleWorkers && poolSize <= prevLen {
			select {
			case worker := <-wp.workerPool:
				close(worker.shutdown)
			default:
				// burst traffic stole all workers between len check and select start
			}
		}
		prevLen = poolSize
	}
}
