// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"errors"
	"fmt"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// ErrBrokerCommitFailed marks a CommitOffsets failure that came from the broker
// commit itself (as opposed to e.g. the commit-guard timeout). The drain treats
// it as surfaced-but-not-fatal: the uncommitted tail is an at-least-once
// re-read for the partition's next owner, not an availability failure.
var ErrBrokerCommitFailed = errors.New("broker offset commit failed")

// startAsyncCommits periodically sends high-watermark offsets to message broker.
func (oc *Committer[T]) startAsyncCommits() {
	brokerCommitTicker := time.NewTicker(oc.autoCommitInterval)
	defer brokerCommitTicker.Stop()

	for {
		select {
		case <-oc.ctx.Done():
			return
		case <-brokerCommitTicker.C:
			err := oc.CommitOffsets()
			if err != nil {
				oc.logger.Error(oc.ctx, fmt.Sprintf("failed to commit offset(s) - %v", err))
			}
		}
	}
}

// CommitOffsets in Broker
func (oc *Committer[T]) CommitOffsets() error {
	mutexAcquired := false
	guardAcquired := false
	defer func() {
		if mutexAcquired {
			oc.mu.Unlock()
		}
		if guardAcquired {
			<-oc.autoCommitGuard
		}
	}()

	select {
	case oc.autoCommitGuard <- struct{}{}:
		guardAcquired = true
		oc.mu.Lock()
		mutexAcquired = true

		// consume any pending gap-advance flags (flag-gated, cheap when none):
		// a reset that flagged items buffered before its assign has no ingest
		// batch to run the flush when no further traffic arrives, so the tick
		// must attempt it, or a Ready initialisable from the buffer strands
		oc.flushGapBuffers(time.Now())

		commits := make([]*nexus.Message[T], 0, len(oc.offsetsByPartition.PartitionMap))
		for partition, offsetTracker := range oc.offsetsByPartition.PartitionMap {
			// guard: only commit for assigned partitions (orphaned work item protection).
			// Checked before the Ready-nil test so gap-buffer leftovers are also cleaned
			// up when Ready is nil; a stale gap item surviving into a later re-assignment
			// of the partition would stall the flush walk permanently.
			if _, ok := oc.assigned[partition]; !ok {
				oc.discardOrphanedTracker(partition, offsetTracker)
				continue
			}

			workItem := offsetTracker.Ready
			if workItem == nil {
				// stall repair of last resort: Ready empty with items buffered
				// is the blockage signature, and the flag-gated flush above has
				// already had its chance. A successful repair commits this tick.
				oc.repairSuccessor(partition, offsetTracker)
				if workItem = offsetTracker.Ready; workItem == nil {
					continue
				}
			}

			// A Ready below the baseline is stale (an orphaned work item from a previous ownership
			// epoch that slipped past an unknown-baseline reset). Committing it would
			// move the broker's committed offset backwards; discard it instead. The
			// buffer may already hold the baseline's completion (a First record that
			// could not displace the stale Ready), so ask the next flush to
			// re-initialise Ready from the buffer.
			if workItem.Message.Offset < offsetTracker.CommittedPlusOne {
				nexus.SetOrphaned(&workItem.Metrics.Traits)
				oc.returnMessageAndCollectMetrics(workItem)
				offsetTracker.Ready = nil
				if len(offsetTracker.GapBuffer) > 0 {
					offsetTracker.NeedsGapAdvance = true
				}
				continue
			}

			commits = append(commits, workItem.Message)
		}
		if len(commits) > 0 {
			_, err := oc.commitOffsets(commits)
			if err != nil {
				oc.logger.Error(oc.ctx, fmt.Sprintf("failed to commit offset(s) - %v", err))
				return fmt.Errorf("%w: %v", ErrBrokerCommitFailed, err)
			}
			for _, message := range commits {
				offsetTracker := oc.offsetsByPartition.PartitionMap[message.Partition]
				// never move the baseline backwards: a commit of an older offset must
				// not undo a higher position already established for the current epoch
				if next := message.Offset + 1; next > offsetTracker.CommittedPlusOne {
					offsetTracker.CommittedPlusOne = next
				}
				// record the committed RECORD: the only anchor the flush walk's
				// predecessor linkage may promote against
				if message.Offset > offsetTracker.LastCommittedOffset {
					offsetTracker.LastCommittedOffset = message.Offset
				}
				oc.returnMessageAndCollectMetrics(offsetTracker.Ready)
				offsetTracker.Ready = nil
				// Ready consumed with items still buffered: ask the next flush to
				// attempt to re-initialise Ready (see flushGapBuffers; prevents the
				// commit-boundary broker-gap stall when the successor is already buffered)
				if len(offsetTracker.GapBuffer) > 0 {
					offsetTracker.NeedsGapAdvance = true
				}
			}
		}
		return nil

	case <-time.After(oc.acquireCommitGuardTimeout):
		acquireGuardFailed := "failed to acquire commit guard after %s"
		return fmt.Errorf(acquireGuardFailed, oc.acquireCommitGuardTimeout)
	}
}

// discardOrphanedTracker drops a no-longer-assigned partition's pending state:
// the Ready item and any gap-buffer leftovers. Runs under oc.mu (called from
// CommitOffsets).
func (oc *Committer[T]) discardOrphanedTracker(partition int32, offsetTracker *OffsetsTracker[T]) {
	if offsetTracker.Ready == nil && len(offsetTracker.GapBuffer) == 0 {
		return
	}
	if offsetTracker.Ready != nil {
		oc.logger.Warn(oc.ctx, fmt.Sprintf(
			"discarding orphaned work item for partition %d offset %d (partition no longer assigned)",
			partition, offsetTracker.Ready.Message.Offset))
		nexus.SetOrphaned(&offsetTracker.Ready.Metrics.Traits)
		oc.returnMessageAndCollectMetrics(offsetTracker.Ready)
		offsetTracker.Ready = nil
	}
	if len(offsetTracker.GapBuffer) > 0 {
		oc.logger.Warn(oc.ctx, fmt.Sprintf(
			"discarding %d orphaned gap-buffer item(s) for partition %d (partition no longer assigned)",
			len(offsetTracker.GapBuffer), partition))
		for _, stale := range offsetTracker.GapBuffer {
			nexus.SetOrphaned(&stale.Metrics.Traits)
			oc.returnMessageAndCollectMetrics(stale)
		}
		offsetTracker.GapBuffer = offsetTracker.GapBuffer[:0] // clear, keep capacity
	}
	offsetTracker.NeedsGapAdvance = false
}
