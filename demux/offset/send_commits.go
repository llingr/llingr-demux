// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"fmt"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

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

		commits := make([]*nexus.Message[T], 0, len(oc.offsetsByPartition.PartitionMap))
		for partition, offsetTracker := range oc.offsetsByPartition.PartitionMap {
			workItem := offsetTracker.Ready
			if workItem == nil {
				continue
			}

			// guard: only commit for assigned partitions (orphaned work item protection)
			if _, ok := oc.assigned[partition]; !ok {
				oc.logger.Warn(oc.ctx, fmt.Sprintf(
					"discarding orphaned work item for partition %d offset %d (partition no longer assigned)",
					partition, workItem.Message.Offset))
				oc.returnMessageAndCollectMetrics(workItem)
				offsetTracker.Ready = nil
				offsetTracker.GapBuffer = offsetTracker.GapBuffer[:0] // clear, keep capacity
				offsetTracker.NeedsGapAdvance = false
				continue
			}

			commits = append(commits, workItem.Message)
		}
		if len(commits) > 0 {
			_, err := oc.commitOffsets(commits)
			if err != nil {
				oc.logger.Error(oc.ctx, fmt.Sprintf("failed to commit offset(s) - %v", err))
			} else {
				for _, message := range commits {
					offsetTracker := oc.offsetsByPartition.PartitionMap[message.Partition]
					offsetTracker.CommittedPlusOne = message.Offset + 1
					oc.returnMessageAndCollectMetrics(offsetTracker.Ready)
					offsetTracker.Ready = nil
				}
			}
		}
		return nil

	case <-time.After(oc.acquireCommitGuardTimeout):
		acquireGuardFailed := "failed to acquire commit guard after %s"
		return fmt.Errorf(acquireGuardFailed, oc.acquireCommitGuardTimeout)
	}
}
