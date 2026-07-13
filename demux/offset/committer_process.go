// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"fmt"
	"slices"
	"time"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// processCommit ensures the WorkItem is set as 'ready' to commit, or added
// to a 'gap buffer' (slice) if it is non-contiguous so that future work items
// can fill this gap. The gap buffer supports at-least-once semantics, since an
// outage after a non-contiguous offset advancement/commit would risk losing
// unfinished messages behind it.
//
// The Offset high-watermarks are 'advanced' in this algorithm, at which point
// each WorkItem is immediately collected for metrics capture and returned to
// the object pool for recycling (alleviates GC pressure).
func (oc *Committer[T]) processCommit(workItem *ports.WorkItem[T], now time.Time) {
	partition, offset := workItem.PartitionOffset()

	offsetTracker, ok := oc.offsetsByPartition.PartitionMap[partition]
	if !ok {
		offsetTracker = &OffsetsTracker[T]{
			CommittedPlusOne:    -1,
			LastCommittedOffset: -1,
			Assignment:          nexus.Assign,
			MinOffsetSeen:       offset,
			MaxOffsetSeen:       offset,
			GapBuffer:           make([]*ports.WorkItem[T], 0, oc.gapBufferSize),
		}
		oc.offsetsByPartition.PartitionMap[partition] = offsetTracker
	} else {
		if offset < offsetTracker.MinOffsetSeen {
			offsetTracker.MinOffsetSeen = offset
		} else if offset > offsetTracker.MaxOffsetSeen {
			offsetTracker.MaxOffsetSeen = offset
		}
	}

	// only update CommittedPlusOne from First if offset >= current value, preventing
	// stale orphaned WorkItems (from drain timeout) from corrupting the broker position
	// set by ResetCommittedOffsets during rebalance assign. See DESIGN.md.
	if workItem.First {
		nexus.SetFirstAfterRebalance(&workItem.Metrics.Traits)
		if offset >= offsetTracker.CommittedPlusOne {
			offsetTracker.CommittedPlusOne = offset
		}
	}

	oc.checkAndAdvance(offsetTracker, workItem, offset, now)
}

// checkAndAdvance the most recent WorkItem 'ready' to commit, so that
// when the committer runs, all it requires is each ready item in the
// offsetsByPartition map.
//
// If the workItem is contiguous (previous + 1), this high-watermark
// advances immediately, otherwise it is added to the 'gap buffer' for
// future re-org and advancement.
//
// When a WorkItem advances, it is sent to the metrics collector to
// complete its lifecycle, after which it is returned to the object pool.
func (oc *Committer[T]) checkAndAdvance(offsetsTracker *OffsetsTracker[T],
	workItem *ports.WorkItem[T], offset int64, now time.Time) {

	// wall time from monotonic time delta calc
	// avoiding significantly slower syscall
	now = now.Add(time.Since(now))

	if offsetsTracker.Ready == nil {
		switch {
		case offsetsTracker.CommittedPlusOne == offset:
			// next expected broker offset, make this ready for next commit
			offsetsTracker.Ready = workItem
			workItem.Metrics.WatermarkAdvanceTime = now
			// and advance through any buffered commits
			oc.advanceThroughGapBuffer(offsetsTracker, workItem.Message.Offset, now)
		case offsetsTracker.CommittedPlusOne > offset:
			// orphaned WorkItem: completed after partition reassigned with advanced offset
			nexus.SetOrphaned(&workItem.Metrics.Traits)
			// stamp detection time - item never reached Ready
			workItem.Metrics.WatermarkAdvanceTime = now
			oc.returnMessageAndCollectMetrics(workItem)
		default:
			// pend in gap buffer
			oc.addToGapBuffer(offsetsTracker, workItem)
			// With Ready empty, an arrival that satisfies a Ready-initialization
			// rule (its offset is the baseline, or its predecessor is the record
			// committed this epoch - the broker-gap case where the baseline
			// offset does not exist) must ask the batch-end flush to
			// re-initialize Ready from the buffer, or the partition stalls
			// permanently: commits stop and the buffer grows without bound.
			// Other arrivals never flag: they cannot unlock the walk, and
			// flagging them made every batch-end flush sort the whole buffer
			// while the baseline was still unknown. Baseline CHANGES that could
			// qualify already-buffered items set the flag at their own sites:
			// the commit success path, the stale-Ready discard, and
			// ResetCommittedOffsets.
			if offset == offsetsTracker.CommittedPlusOne ||
				(offsetsTracker.LastCommittedOffset >= 0 &&
					workItem.PreviousOffset == offsetsTracker.LastCommittedOffset) {
				offsetsTracker.NeedsGapAdvance = true
			}
		}

	} else {
		readyOffset := offsetsTracker.Ready.Message.Offset
		switch {
		case readyOffset > offset:
			// orphaned WorkItem: completed after watermark already advanced past this offset
			nexus.SetOrphaned(&workItem.Metrics.Traits)
			// stamp detection time - item never reached Ready
			workItem.Metrics.WatermarkAdvanceTime = now
			oc.returnMessageAndCollectMetrics(workItem)
		case readyOffset == workItem.PreviousOffset || readyOffset == offset:
			if readyOffset == offset {
				// the 'should never happen' case, indicates a broker client or infra issue
				const duplicateMessage = "duplicate 'should never happen' on partition: %d, offset: %d"
				partition := workItem.Message.Partition
				oc.logger.Error(oc.ctx, fmt.Sprintf(duplicateMessage, partition, readyOffset))
			}
			// swap existing to make workItem the new 'ready' to commit
			oc.returnMessageAndCollectMetrics(offsetsTracker.Ready)
			offsetsTracker.Ready = workItem
			workItem.Metrics.WatermarkAdvanceTime = now
			readyOffset = workItem.Message.Offset
			oc.advanceThroughGapBuffer(offsetsTracker, readyOffset, now)
		default:
			// offset too far ahead, pend for future advance
			oc.addToGapBuffer(offsetsTracker, workItem)
		}
	}
}

// addToGapBuffer queues out-of-order messages that can't advance immediately
func (oc *Committer[T]) addToGapBuffer(offsetsTracker *OffsetsTracker[T], workItem *ports.WorkItem[T]) {
	// work items that advance immediately (fast path) are not
	// assigned this trait, explaining potentially slow advance
	nexus.SetCommitBuffered(&workItem.Metrics.Traits)
	offsetsTracker.GapBuffer = append(offsetsTracker.GapBuffer, workItem)
}

// advanceThroughGapBuffer quick-scans the gap buffer for the next contiguous offset.
// If found, it sets a deferred flag so the expensive sort+walk runs once at batch end
// via flushGapBuffers, amortizing the O(n log n) sort across the whole batch.
func (oc *Committer[T]) advanceThroughGapBuffer(offsetsTracker *OffsetsTracker[T], readyOffset int64,
	now time.Time) {

	if len(offsetsTracker.GapBuffer) == 0 {
		return
	}
	for _, g := range offsetsTracker.GapBuffer {
		if readyOffset == g.PreviousOffset {
			offsetsTracker.NeedsGapAdvance = true
			return
		}
	}
}

// flushGapBuffers sorts and walks gap buffers for all partitions flagged during the batch.
// Called once per batch before releasing the mutex, amortizing the O(n log n) sort.
func (oc *Committer[T]) flushGapBuffers(now time.Time) {
	now = now.Add(time.Since(now))
	for _, tracker := range oc.offsetsByPartition.PartitionMap {
		if !tracker.NeedsGapAdvance {
			continue
		}
		tracker.NeedsGapAdvance = false

		if len(tracker.GapBuffer) == 0 {
			continue
		}

		oc.sortGapBuffer(tracker)
		advancedOffsetIndex := -1
	walk:
		for i, g := range tracker.GapBuffer {
			switch {
			case tracker.Ready == nil && g.Message.Offset < tracker.CommittedPlusOne:
				// below the baseline with Ready empty: a leftover that can never
				// commit; prune it as orphaned so it cannot block the
				// re-initialization below
				nexus.SetOrphaned(&g.Metrics.Traits)
				g.Metrics.WatermarkAdvanceTime = now
				oc.returnMessageAndCollectMetrics(g)
				advancedOffsetIndex = i
			case tracker.Ready == nil && (g.Message.Offset == tracker.CommittedPlusOne ||
				(tracker.LastCommittedOffset >= 0 &&
					g.PreviousOffset == tracker.LastCommittedOffset)):
				// Re-initialize Ready from the buffer. With Ready consumed (by a
				// commit or a stale-Ready discard), an arriving completion can no
				// longer initialize it when the expected next offset was skipped on
				// the broker or has already arrived and is in the buffer.
				// Without this, the commits for the partition would stall.
				// Initialization uses the arrival rule (offset == CommittedPlusOne)
				// with the same predecessor linkage the live swap path uses - predecessor
				// is the last committed offset - so ordering invariants are unchanged.
				tracker.Ready = g
				g.Metrics.WatermarkAdvanceTime = now
				advancedOffsetIndex = i
			case tracker.Ready == nil:
				// nothing qualifies yet: the true successor has not completed.
				// Linkage broken by log compaction across ownership epochs is
				// repaired at commit cadence instead (see repairSuccessor),
				// never on this batch path.
				break walk
			case g.Message.Offset <= tracker.Ready.Message.Offset:
				// at or below the watermark: an orphan buffered while the baseline was
				// unknown (the adapter could not supply the broker position at assign).
				// It can never link and, sorted to the front, would break this walk on
				// every flush, silently stalling commits for the rest of the epoch.
				// Prune it as orphaned instead.
				nexus.SetOrphaned(&g.Metrics.Traits)
				g.Metrics.WatermarkAdvanceTime = now
				oc.returnMessageAndCollectMetrics(g)
				advancedOffsetIndex = i
			case tracker.Ready.Message.Offset == g.PreviousOffset:
				oc.returnMessageAndCollectMetrics(tracker.Ready)
				tracker.Ready = g
				g.Metrics.WatermarkAdvanceTime = now
				advancedOffsetIndex = i
			default:
				break walk
			}
		}

		if advancedOffsetIndex < 0 {
			continue
		}

		if len(tracker.GapBuffer) == advancedOffsetIndex+1 {
			tracker.GapBuffer = make([]*ports.WorkItem[T], 0, oc.gapBufferSize)
		} else {
			tracker.GapBuffer = tracker.GapBuffer[advancedOffsetIndex+1:]
			oc.resizeGapBuffer(tracker)
		}
	}
}

// sortGapBuffer also removes duplicates (rare)
func (oc *Committer[T]) sortGapBuffer(offsetsTracker *OffsetsTracker[T]) {
	foundDuplicate := false
	slices.SortFunc(offsetsTracker.GapBuffer, func(a, b *ports.WorkItem[T]) int {
		offsetA := a.Message.Offset
		offsetB := b.Message.Offset
		if offsetA < offsetB {
			return -1
		}
		if offsetA > offsetB {
			return 1
		}
		foundDuplicate = true
		// equal offsets are cross-epoch twins (impossible within one fetch
		// stream) and their predecessor stamps are not interchangeable: order
		// the later-read twin first so the compact keeps the current epoch's
		// stamp. SortFunc is not stable, so equal elements must never be left
		// for it to order; CompactFunc keeps the first of each equal run.
		return b.Metrics.ReadTime.Compare(a.Metrics.ReadTime)
	})

	// amortized de-dupe
	if foundDuplicate {
		offsetsTracker.GapBuffer = slices.CompactFunc(offsetsTracker.GapBuffer, func(a, b *ports.WorkItem[T]) bool {
			return a.Message.Offset == b.Message.Offset
		})
	}
}

// resizeGapBuffer applies 70% / 300% hysteresis thresholds around
// steady-state capacity to amortize gap buffer reallocations.
func (oc *Committer[T]) resizeGapBuffer(offsetsTracker *OffsetsTracker[T]) {
	if cap(offsetsTracker.GapBuffer) < oc.gapBufferSize70Pct {
		// only grow when < 70% steady-state capacity
		offsetsTracker.GapBuffer = slices.Grow(offsetsTracker.GapBuffer, oc.gapBufferSize)
	} else if cap(offsetsTracker.GapBuffer) > oc.gapBufferSize*3 {
		// re-allocate when > 300% capacity to prevent memory leaks in unbounded underlying
		// array when steady-state is more than expected, increase to 400%, 500%, ...
		//
		// Iterator needed for extreme throughput where out-of-order messages arrive
		// faster than gap resolution can occur and buffer grows beyond 4x bounds.
		// This only manifests at near-zero processing latency.
		for i := 4; ; i++ {
			if len(offsetsTracker.GapBuffer) < oc.gapBufferSize*i {
				newBuffer := make([]*ports.WorkItem[T], len(offsetsTracker.GapBuffer), oc.gapBufferSize*i)
				copy(newBuffer, offsetsTracker.GapBuffer)
				offsetsTracker.GapBuffer = newBuffer
				return
			}
		}
	}
}

// returnMessageAndCollectMetrics captures watermark advance time
// and sends to metrics sink for observability. This completes the
// WorkItem lifecycle, including pooled objects return.
func (oc *Committer[T]) returnMessageAndCollectMetrics(workItem *ports.WorkItem[T]) {
	oc.totalProcessed++
	metrics := workItem.Metrics
	processedAt := metrics.ProcessStartTime.Add(metrics.ProcessDuration)
	oc.metricsBuffer.record(processedAt, metrics.ProcessDuration, metrics.WatermarkAdvanceTime.Sub(metrics.ReadTime), metrics.Traits&nexus.DeadLetter != 0)
	oc.collectMetrics(workItem) // must be last - collector may return WorkItem to pool
}
