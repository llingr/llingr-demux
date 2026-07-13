// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_Committer_OrphanedFirstFlagOverridesCommittedPlusOne is a regression test for a
// bug where an orphaned worker's First=true flag incorrectly overrode CommittedPlusOne
// that was set by ResetCommittedOffsets during rebalance assign. The fix (offset >=
// CommittedPlusOne guard in processCommit) prevents stale orphaned items from corrupting
// the broker position. This test ensures the bug does not resurface.
//
// Original bug scenario:
//  1. Consumer A polls offset 100, First=true (first after original assignment)
//  2. Worker blocks on slow external system
//  3. Rebalance, drain timeout, worker abandoned (becomes orphaned)
//  4. Partition assigned to Consumer B, processes 100-150, commits to 150
//  5. Rebalance returns partition to Consumer A
//  6. ResetCommittedOffsets sets CommittedPlusOne=150
//  7. Orphaned worker finally completes, sends to commitsIn with First=true, offset=100
//  8. Without fix: First=true caused CommittedPlusOne to be overwritten to 100
//  9. checkAndAdvance saw CommittedPlusOne==offset, orphaned item became Ready
//  10. Orphaned offset 100 got committed, moving broker position backwards!
//
// This test simulates steps 6-9 without race timing complexity and asserts the orphaned
// item is rejected, verifying the >= guard in processCommit prevents the regression.
func Test_Committer_OrphanedFirstFlagOverridesCommittedPlusOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // ensure goroutines are cleaned up

	logger := nexus.NewDefaultLogger(slog.LevelError)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond, // minimum allowed for fast tests
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// Track if orphaned item becomes Ready (the bug)
	var metricsMu sync.Mutex
	var metricsCollected []int64

	metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
		metricsMu.Lock()
		defer metricsMu.Unlock()
		metricsCollected = append(metricsCollected, m.Offset)
		return nil
	}

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	partition := int32(0)
	brokerCommittedPlusOne := int64(150) // broker has committed up to 149, expects 150 next
	orphanedOffset := int64(100)         // orphaned from before partition was revoked

	// Step 6: Simulate ResetCommittedOffsets having run during Assign
	// This sets CommittedPlusOne to the broker's position (150)
	committer.ResetCommittedOffsets(map[int32]int64{
		partition: brokerCommittedPlusOne,
	})
	committer.MarkPartitionAssigned(partition) // required for commit guard

	// Verify CommittedPlusOne is set correctly
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if tracker.CommittedPlusOne != brokerCommittedPlusOne {
		committer.mu.Unlock()
		t.Fatalf("setup failed: CommittedPlusOne should be %d, got %d",
			brokerCommittedPlusOne, tracker.CommittedPlusOne)
	}
	committer.mu.Unlock()

	// Step 7: Create orphaned work item
	// The orphaned worker has First=true because it was the first message after the ORIGINAL
	// assignment (before the partition was revoked). This flag is stale but still set.
	orphaned := pool.Borrow()
	orphaned.Message.Partition = partition
	orphaned.Message.Offset = orphanedOffset
	orphaned.Metrics.Partition = partition
	orphaned.Metrics.Offset = orphanedOffset
	orphaned.First = true // This is the problematic flag!

	// Step 8-9: Send orphaned item through the committer
	committer.CollectAndCommit(orphaned)

	// Wait for ingest loop to process
	time.Sleep(100 * time.Millisecond)

	// Check the result
	committer.mu.Lock()
	defer committer.mu.Unlock()

	tracker = committer.offsetsByPartition.PartitionMap[partition]

	// Assertion 1: First=true must not override CommittedPlusOne
	if tracker.CommittedPlusOne == orphanedOffset {
		t.Errorf("REGRESSION: First=true flag caused CommittedPlusOne to be overwritten "+
			"from %d to %d (orphaned offset)", brokerCommittedPlusOne, orphanedOffset)
	}

	// Assertion 2: orphaned item must not become Ready
	if tracker.Ready != nil && tracker.Ready.Message.Offset == orphanedOffset {
		t.Errorf("REGRESSION: Orphaned offset %d became Ready when CommittedPlusOne "+
			"should have been %d - this would cause backward commit!",
			orphanedOffset, brokerCommittedPlusOne)
	}

	// What SHOULD happen: orphaned item should be rejected because offset < CommittedPlusOne
	// and CommittedPlusOne should remain at brokerCommittedPlusOne (150)
	if tracker.CommittedPlusOne != brokerCommittedPlusOne {
		t.Errorf("CommittedPlusOne was corrupted: expected %d, got %d",
			brokerCommittedPlusOne, tracker.CommittedPlusOne)
	}

	if tracker.Ready != nil {
		t.Errorf("Ready should be nil (orphaned rejected), but Ready.Offset=%d",
			tracker.Ready.Message.Offset)
	}
}

// Test_Committer_OrphanedRaceCondition verifies that no race condition exists between
// the ingest loop processing an orphaned item and ResetCommittedOffsets updating
// CommittedPlusOne.
//
// The fix uses mutex-based synchronisation in ResetCommittedOffsets, eliminating any
// race window:
//   - If ingest loop holds mutex → ResetCommittedOffsets blocks until released
//   - If ResetCommittedOffsets holds mutex → ingest loop can't process commits
//
// This test runs many iterations to verify orphaned items are ALWAYS rejected after reset.
func Test_Committer_OrphanedRaceCondition(t *testing.T) {
	iterations := 5000
	if testing.Short() {
		iterations = 100
	}
	racesDetected := atomic.Int32{}

	// Run iterations in parallel to stress test the mutex coordination
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, runtime.GOMAXPROCS(0)*2) // limit concurrency

	for i := 0; i < iterations; i++ {
		semaphore <- struct{}{}
		wg.Add(1)

		go func(_ int) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// cancellable context prevents goroutine accumulation across iterations
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // cleanup committer goroutines when iteration completes

			logger := nexus.NewDefaultLogger(slog.LevelError)

			cfg := config.DemuxConfig{
				AutoCommitInterval: 250 * time.Millisecond,
			}
			cfg.SetDemuxConfigDefaults()

			pool := alloc.NewWorkItemsPool[string](cfg)
			metricsCollector := metrics.NewCollector[string](ctx, cfg,
				func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)
			metricsCollector.StartCollectingMetrics()

			committer := NewCommitter[string](ctx, cfg,
				func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
					return msgs, nil
				}, metricsCollector, logger)

			partition := int32(0)
			staleCommittedPlusOne := int64(100) // from original assignment
			newCommittedPlusOne := int64(150)   // from broker after Consumer B processed
			orphanedOffset := int64(100)

			// Set up stale state (simulating state from before partition was revoked)
			committer.mu.Lock()
			committer.offsetsByPartition.PartitionMap[partition] = &OffsetsTracker[string]{
				CommittedPlusOne: staleCommittedPlusOne,
				Assignment:       nexus.Assign,
				GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
			}
			committer.mu.Unlock()
			committer.MarkPartitionAssigned(partition) // required for commit guard

			// Create orphaned item - NOTE: First=false here to isolate the race condition
			// from the First flag override bug (tested separately above)
			orphaned := pool.Borrow()
			orphaned.Message.Partition = partition
			orphaned.Message.Offset = orphanedOffset
			orphaned.Metrics.Partition = partition
			orphaned.Metrics.Offset = orphanedOffset
			orphaned.First = false // Testing pure race, not First flag issue
			orphaned.PreviousOffset = orphanedOffset - 1

			// Race setup: hold mutex while setting up race conditions
			committer.mu.Lock()

			// Put orphaned item in channel while we hold the mutex
			// The ingest loop is blocked waiting for mu
			committer.commitsIn <- orphaned

			// Start ResetCommittedOffsets in goroutine - it will block waiting for mutex
			resetDone := make(chan struct{})
			go func() {
				// This now acquires mutex directly - no race window possible
				committer.ResetCommittedOffsets(map[int32]int64{partition: newCommittedPlusOne})
				close(resetDone)
			}()

			// Yield to give ResetCommittedOffsets goroutine a chance to start and block
			runtime.Gosched()

			// Release mutex - now there's a race between:
			// 1. ResetCommittedOffsets acquiring mutex and updating CommittedPlusOne
			// 2. Ingest loop acquiring mutex and processing orphaned item
			// With mutex-based reset, whichever wins first will hold the mutex,
			// and the other must wait. If reset wins, orphaned is rejected.
			// If ingest wins, it processes with stale state, THEN reset runs.
			// But since orphaned offset == stale CommittedPlusOne, orphaned becomes Ready.
			// The key insight: we need reset to ALWAYS win, which the mutex guarantees
			// because ResetCommittedOffsets is called BEFORE polling resumes in production.
			committer.mu.Unlock()

			// Wait for ResetCommittedOffsets to complete
			<-resetDone

			// Allow ingest loop to process. 1ms suffices: orphaned item is already on
			// commitsIn and ingest loop was blocked on mutex (not its 10ms idle ticker).
			time.Sleep(1 * time.Millisecond)

			// Check result
			committer.mu.Lock()
			tracker := committer.offsetsByPartition.PartitionMap[partition]

			// If orphaned item became Ready with the stale offset, the race was lost
			if tracker.Ready != nil && tracker.Ready.Message.Offset == orphanedOffset {
				// Only count as race if CommittedPlusOne is now correct (150)
				// This proves ResetCommittedOffsets did run, but orphaned won the race
				if tracker.CommittedPlusOne == newCommittedPlusOne {
					racesDetected.Add(1)
				}
			}
			committer.mu.Unlock()
		}(i)
	}

	wg.Wait()

	if racesDetected.Load() > 0 {
		t.Errorf("RACE CONDITION DETECTED: %d/%d iterations had orphaned item become Ready "+
			"despite ResetCommittedOffsets having run. This indicates the mutex-based "+
			"fix is not working correctly.", racesDetected.Load(), iterations)
	} else {
		t.Logf("No race detected in %d iterations - mutex-based synchronisation is working.",
			iterations)
	}
}

// Test_Committer_OrphanedWithFirstFlagAfterResetSequential verifies the exact sequence
// of events that leads to the bug, without any race conditions - purely sequential
// to prove the logic error.
func Test_Committer_OrphanedWithFirstFlagAfterResetSequential(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // ensure goroutines are cleaned up

	logger := nexus.NewDefaultLogger(slog.LevelError)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	committer := NewCommitter[string](ctx, cfg,
		func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
			return msgs, nil
		}, metricsCollector, logger)

	// We'll call processCommit directly to test the logic without timing issues
	partition := int32(0)

	// Step 1: Simulate ResetCommittedOffsets having run (Assign phase)
	committer.ResetCommittedOffsets(map[int32]int64{partition: 150})
	committer.MarkPartitionAssigned(partition) // required for commit guard

	// Verify initial state
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	initialCommittedPlusOne := tracker.CommittedPlusOne
	t.Logf("After ResetCommittedOffsets: CommittedPlusOne=%d", initialCommittedPlusOne)

	if initialCommittedPlusOne != 150 {
		committer.mu.Unlock()
		t.Fatalf("Setup failed: expected CommittedPlusOne=150, got %d", initialCommittedPlusOne)
	}

	// Step 2: Create orphaned item with First=true
	orphaned := pool.Borrow()
	orphaned.Message.Partition = partition
	orphaned.Message.Offset = 100 // behind broker's position
	orphaned.Metrics.Partition = partition
	orphaned.Metrics.Offset = 100
	orphaned.First = true // stale flag from original assignment

	// Step 3: Call processCommit directly (bypassing channel/ingest loop)
	now := time.Now()
	committer.processCommit(orphaned, now)

	// Step 4: Check results
	t.Logf("After processCommit: CommittedPlusOne=%d, Ready=%v",
		tracker.CommittedPlusOne,
		tracker.Ready != nil)

	// The bug: First=true causes CommittedPlusOne to be overwritten
	if tracker.CommittedPlusOne == 100 {
		t.Errorf("BUG: First=true overwrote CommittedPlusOne from 150 to 100")
	}

	if tracker.Ready != nil {
		t.Errorf("BUG: Orphaned item became Ready at offset %d when it should have been "+
			"rejected (offset %d < CommittedPlusOne %d)",
			tracker.Ready.Message.Offset, 100, 150)
	}

	committer.mu.Unlock()
}
