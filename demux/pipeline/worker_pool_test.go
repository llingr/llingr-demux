// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package pipeline

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/tests/mocklogger"
)

func TestNewWorkerPool(t *testing.T) {
	pool := NewWorkerPool[string](10, 2, 50*time.Millisecond)

	if cap(pool.workerPool) != 10 {
		t.Errorf("expected pool capacity 10, got %d", cap(pool.workerPool))
	}
	if pool.tickDuration != 50*time.Millisecond {
		t.Errorf("expected tick duration 50ms, got %v", pool.tickDuration)
	}
}

func TestBorrowWorker_FromPool(t *testing.T) {
	pool := NewWorkerPool[string](10, 0, time.Second)

	// create and add a worker to pool
	workerShard := testWorkerShard[string]()
	worker := NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
	pool.workerPool <- worker

	// borrow should return the pooled worker
	borrowed := pool.BorrowWorker()
	if borrowed != worker {
		t.Error("expected to borrow the pooled worker")
	}
	if len(pool.workerPool) != 0 {
		t.Errorf("expected empty pool after borrow, got %d", len(pool.workerPool))
	}
}

func TestBorrowWorker_CreateNew(t *testing.T) {
	pool := NewWorkerPool[string](10, 0, time.Second)

	var createCalled atomic.Bool
	pool.createWorker = func() *Worker[string] {
		createCalled.Store(true)
		return &Worker[string]{}
	}

	// pool is empty, should call createWorker
	_ = pool.BorrowWorker()

	if !createCalled.Load() {
		t.Error("expected createWorker to be called when pool is empty")
	}
}

func TestReturnWorker_ToPool(t *testing.T) {
	pool := NewWorkerPool[string](10, 0, time.Second)

	workerShard := testWorkerShard[string]()
	worker := NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())

	pool.ReturnWorker(worker)

	if len(pool.workerPool) != 1 {
		t.Errorf("expected 1 worker in pool, got %d", len(pool.workerPool))
	}
}

func TestReturnWorker_PoolFull_ShutdownWorker(t *testing.T) {
	// pool with capacity 1
	pool := NewWorkerPool[string](1, 0, time.Second)

	workerShard := testWorkerShard[string]()
	worker1 := NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
	worker2 := NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())

	// fill the pool
	pool.ReturnWorker(worker1)

	// return second worker - should be shut down
	pool.ReturnWorker(worker2)

	// verify worker2 was shut down
	select {
	case <-worker2.shutdown:
		// expected
	default:
		t.Error("expected worker2.shutdown to be closed when pool is full")
	}

	// pool should still have only 1 worker
	if len(pool.workerPool) != 1 {
		t.Errorf("expected 1 worker in pool, got %d", len(pool.workerPool))
	}
}

func TestPruneIdleWorkers_PrunesAboveMin(t *testing.T) {
	// minIdleWorkers=1, so pool with 3 workers should prune down to 1
	pool := NewWorkerPool[string](10, 1, 20*time.Millisecond)

	workerShard := testWorkerShard[string]()

	// add 3 workers to pool
	workers := make([]*Worker[string], 3)
	for i := 0; i < 3; i++ {
		workers[i] = NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
		pool.workerPool <- workers[i]
	}

	// wait for pruning (needs multiple ticks: 3->2->1)
	time.Sleep(150 * time.Millisecond)

	// should have pruned down to minIdleWorkers (1)
	poolLen := len(pool.workerPool)
	if poolLen != 1 {
		t.Errorf("expected pool to prune to 1, got %d", poolLen)
	}

	// verify pruned workers were shut down
	shutdownCount := 0
	for _, w := range workers {
		select {
		case <-w.shutdown:
			shutdownCount++
		default:
		}
	}
	if shutdownCount != 2 {
		t.Errorf("expected 2 workers shut down, got %d", shutdownCount)
	}
}

func TestPruneIdleWorkers_DoesNotPruneBelowMin(t *testing.T) {
	// minIdleWorkers=2, pool with 2 workers should not prune
	pool := NewWorkerPool[string](10, 2, 20*time.Millisecond)

	workerShard := testWorkerShard[string]()

	// add exactly minIdleWorkers
	workers := make([]*Worker[string], 2)
	for i := 0; i < 2; i++ {
		workers[i] = NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
		pool.workerPool <- workers[i]
	}

	// wait for potential pruning
	time.Sleep(100 * time.Millisecond)

	// should still have 2 workers
	if len(pool.workerPool) != 2 {
		t.Errorf("expected pool to remain at 2, got %d", len(pool.workerPool))
	}

	// verify no workers shut down
	for i, w := range workers {
		select {
		case <-w.shutdown:
			t.Errorf("worker %d should not be shut down", i)
		default:
		}
	}
}

func TestPruneIdleWorkers_SkipsPruningWhenGrowing(t *testing.T) {
	// when pool is growing (poolSize > prevLen), pruning should be skipped
	// Timeline:
	//   1. Add 3 workers -> pool=3
	//   2. Tick 1: pool=3 > minIdle=0, prune one -> prevLen=3, pool=2
	//   3. Add 2 more workers -> pool=4
	//   4. Tick 2: pool=4 > prevLen=3 -> skip prune (pool is growing)
	pool := NewWorkerPool[string](10, 0, 30*time.Millisecond)

	workerShard := testWorkerShard[string]()

	// add 3 workers initially
	workers := make([]*Worker[string], 5)
	for i := 0; i < 3; i++ {
		workers[i] = NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
		pool.workerPool <- workers[i]
	}

	// wait for first tick - prunes one worker, sets prevLen=3
	time.Sleep(40 * time.Millisecond)

	// pool should be 2 now (one pruned)
	if len(pool.workerPool) != 2 {
		t.Fatalf("expected 2 workers after first prune, got %d", len(pool.workerPool))
	}

	// add 2 more workers (pool grows from 2 to 4)
	workers[3] = NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
	workers[4] = NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
	pool.workerPool <- workers[3]
	pool.workerPool <- workers[4]

	if len(pool.workerPool) != 4 {
		t.Fatalf("expected 4 workers after growth, got %d", len(pool.workerPool))
	}

	// wait for second tick - should skip because pool=4 > prevLen=3
	time.Sleep(40 * time.Millisecond)

	// pool should still be 4 (prune skipped due to growth)
	if len(pool.workerPool) != 4 {
		t.Errorf("expected 4 workers (prune skipped), got %d", len(pool.workerPool))
	}
}

func TestPruneIdleWorkers_BurstTrafficStealsWorkers(_ *testing.T) {
	// tests the inner default case: workers stolen between len check and select
	//
	// The race window is between pruner's len() check and its inner select.
	// We use capacity=1 pool with rapid add/steal to maximise race probability.
	// Multiple goroutines compete for the single slot while ticks occur.
	pool := NewWorkerPool[string](1, 0, 2*time.Millisecond)

	workerShard := testWorkerShard[string]()

	stop := make(chan struct{})

	// multiple goroutines racing to add/steal from single-slot pool
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					worker := NewWorker[string](workerShard, 16, nil, nil, mocklogger.NewRecordingLogger())
					select {
					case pool.workerPool <- worker:
					default:
					}
					select {
					case <-pool.workerPool:
					default:
					}
				}
			}
		}()
	}

	// run long enough to hit the race many times
	time.Sleep(200 * time.Millisecond)
	close(stop)

	// test passes if no panic - the race path is exercised probabilistically
}
