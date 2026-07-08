// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Package offset implements the fan-in stage that (re-)multiplexes concurrent worker streams
// back to per-partition commit tracking.
//
// The [Committer] receives completed work items from workers - potentially arriving out of
// offset order due to processing jitter - and maintains a high watermark per partition. Only
// contiguous offset ranges are committed to the broker. Out-of-order completions wait in
// a gap buffer until earlier offsets fill in.
//
// Commits run on a timer (default: 5s) in batches.
package offset

import (
	"context"
	"sync"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Committer receives processed messages from pipeline.Processor, and
// (re)-multiplexes concurrent streams back to per-Partition buffers,
// committing contiguous Offset blocks in an async loop.
//
// mu is a pointer to isolate its cache line from commitsIn; this avoids
// read-write contention where mutex locks would stall workers reading
// the channel pointer.
type Committer[T any] struct {
	commitsIn                 chan *ports.WorkItem[T] // all processed work items, highly contended
	mu                        *sync.Mutex             // pointer to avoid contention with frequently-read commitsIn
	offsetsByPartition        *OffsetsByPartition[T]
	collectMetrics            func(*ports.WorkItem[T])
	commitOffsets             nexus.CommitOffsets[T]
	ctx                       context.Context
	logger                    nexus.Logger
	autoCommitInterval        time.Duration
	acquireCommitGuardTimeout time.Duration
	autoCommitGuard           chan struct{}
	drained                   chan struct{}
	gapBufferSize             int
	gapBufferSize70Pct        int
	assigned                  map[int32]struct{} // tracks partition ownership for orphaned work item protection
	metricsBuffer             *metricsRingBuffer // sliding window metrics accumulation (accessed under mu)
	totalProcessed            int64              // grand total of all messages through returnMessageAndCollectMetrics (accessed under mu)
	lastRebalanceTime         time.Time          // last partition assign/revoke time (accessed under mu)
}

// NewCommitter to receive processed messages from pipeline.Processor,
// (re)-multiplexing concurrent streams back to per-Partition buffers,
// committing contiguous offset blocks in an async loop.
func NewCommitter[T any](ctx context.Context, demuxConfig config.DemuxConfig,
	commitOffsets nexus.CommitOffsets[T], metricsCollector ports.MetricsPort[T],
	logger nexus.Logger) *Committer[T] {

	// lift from interface once - keeps function pointer on stack frame
	collectMetrics := metricsCollector.Collect

	// reasonable (high-end) default avoids most re-hashing for typical deployments
	// (12-100 partitions) with negligible ~2KB memory overhead, but can still grow
	const partitionsInitMapSize = 128

	oc := &Committer[T]{
		commitsIn:                 make(chan *ports.WorkItem[T], demuxConfig.CommitIngestChannelLen),
		offsetsByPartition:        New[T](partitionsInitMapSize),
		mu:                        new(sync.Mutex), // synchronize data for async commit
		commitOffsets:             commitOffsets,
		collectMetrics:            collectMetrics,
		ctx:                       ctx,
		logger:                    logger,
		autoCommitInterval:        demuxConfig.AutoCommitInterval,
		acquireCommitGuardTimeout: demuxConfig.AcquireCommitGuardTimeout,
		autoCommitGuard:           make(chan struct{}, 1),
		drained:                   make(chan struct{}, 1),
		gapBufferSize:             demuxConfig.ConcurrentKeys * 3 / 2, //nolint:mnd // 50% headroom
		assigned:                  make(map[int32]struct{}, partitionsInitMapSize),
		metricsBuffer:             newRingBuffer(snapshot.DefaultBucketDuration, snapshot.DefaultBucketCount),
	}

	oc.gapBufferSize70Pct = oc.gapBufferSize * 70 / 100

	go oc.startAsyncCommits()
	go oc.startIngestLoop()
	return oc
}

// CommitIngestChannelLen returns the current and maximum length of the commit ingest channel.
func (oc *Committer[T]) CommitIngestChannelLen() (int, int) {
	return len(oc.commitsIn), cap(oc.commitsIn)
}

// ResetCommittedOffsets updates CommittedPlusOne for partitions during rebalance assign.
// This closes the race window where an orphaned WorkItem (from drain timeout) could arrive
// before the first new message updates CommittedPlusOne via the First flag.
// See DESIGN.md for full explanation of the orphaned WorkItem scenario.
//
// Acquiring the mutex ensures no commits are being processed during the update. Additionally,
// any Ready item that would now be behind CommittedPlusOne is rejected - this handles the case
// where an orphaned WorkItem was processed just before this reset was called.
func (oc *Committer[T]) ResetCommittedOffsets(partitionOffsets map[int32]int64) {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	for partition, committedOffset := range partitionOffsets {
		tracker, ok := oc.offsetsByPartition.PartitionMap[partition]
		if !ok {
			tracker = &OffsetsTracker[T]{
				CommittedPlusOne:    committedOffset,
				LastCommittedOffset: -1, // a baseline is a position, not a record
				Assignment:          nexus.Assign,
				GapBuffer:           make([]*ports.WorkItem[T], 0, oc.gapBufferSize),
			}
			oc.offsetsByPartition.PartitionMap[partition] = tracker
		} else {
			// reject any Ready orphaned WorkItem that would now be behind CommittedPlusOne - this
			// handles the race where an orphaned WorkItem was processed before reset was called
			if tracker.Ready != nil && tracker.Ready.Message.Offset < committedOffset {
				nexus.SetOrphaned(&tracker.Ready.Metrics.Traits)
				oc.returnMessageAndCollectMetrics(tracker.Ready)
				tracker.Ready = nil
			}

			// A re-assign AFTER A REVOKE starts a fresh ownership epoch: anything still in
			// the tracker is from the previous epoch (the revoke drain already committed
			// everything committable) and is discarded unconditionally. The baseline check
			// above is inert when the adapter cannot supply the real broker position at
			// assign (an unknown-baseline sentinel is below every real offset): a stale
			// Ready then survived the reset and was committed BACKWARDS on the next tick,
			// and a stale gap-buffer item below the new epoch's watermark permanently
			// stalled the flush walk permanently, silently stopping commits. Gated on the revoke marker
			// so pre-assign deliveries from permissive test brokers are not discarded.
			// See the reset-to-zero orphan regression tests.
			if tracker.Assignment == nexus.Revoke {
				if tracker.Ready != nil {
					nexus.SetOrphaned(&tracker.Ready.Metrics.Traits)
					oc.returnMessageAndCollectMetrics(tracker.Ready)
					tracker.Ready = nil
				}
				for _, stale := range tracker.GapBuffer {
					nexus.SetOrphaned(&stale.Metrics.Traits)
					oc.returnMessageAndCollectMetrics(stale)
				}
				tracker.GapBuffer = tracker.GapBuffer[:0]
			}

			tracker.CommittedPlusOne = committedOffset
			// this epoch has committed nothing yet: linkage promotion stays
			// disabled until it does (the baseline is a position, not a record)
			tracker.LastCommittedOffset = -1
			tracker.Assignment = nexus.Assign
			// items buffered before this reset (against the old or an unknown
			// baseline) may satisfy a Ready-initialisation rule against the new
			// one with no further traffic ever arriving; ask the next flush to
			// attempt to re-initialise Ready from the buffer
			tracker.NeedsGapAdvance = len(tracker.GapBuffer) > 0
		}
	}
}

// MarkPartitionAssigned records that a partition is assigned to this consumer.
// Called during rebalance Assign, after ResetCommittedOffsets.
//
// This is necessary for orphaned WorkItem protection. When CommitOffsets() runs,
// it checks this map and discards any Ready items for partitions not in the map. This
// prevents commits for partitions that were revoked after a drain timeout. WorkItems
// that complete after their partition was revoked are "orphaned" (called "zombies" in the
// TLA+ model for brevity). See COMMIT_GUARD_ANALYSIS.md for full explanation.
func (oc *Committer[T]) MarkPartitionAssigned(partition int32) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.assigned[partition] = struct{}{}
	oc.lastRebalanceTime = time.Now()
}

// MarkPartitionRevoked records that a partition is no longer assigned.
// Called during rebalance Revoke, after ackRebalance completes.
//
// This is part of the orphaned work item protection mechanism.
// See MarkPartitionAssigned for full explanation.
func (oc *Committer[T]) MarkPartitionRevoked(partition int32) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	delete(oc.assigned, partition)
	// Stamp the tracker so a later re-assign knows this partition went through a
	// revoke: any state still present then is from the closed epoch and is
	// discarded by ResetCommittedOffsets. (Closes the model-implementation gap
	// noted in COMMIT_GUARD_ANALYSIS.md: Assignment was only ever set to Assign.)
	if tracker, ok := oc.offsetsByPartition.PartitionMap[partition]; ok {
		tracker.Assignment = nexus.Revoke
	}
	oc.lastRebalanceTime = time.Now()
}
