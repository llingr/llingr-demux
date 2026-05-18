// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package subscription

import (
	"errors"
	"fmt"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// pausePollingTimeout is the maximum time to wait for the polling loop to pause
// during async drain. Package-level var allows tests to override with shorter duration.
const defaultPausePollingTimeout = 10 * time.Second

var pausePollingTimeout = defaultPausePollingTimeout

// HandleRebalance processes partition assignment and revocation events from the broker.
func (s *Subscription[T]) HandleRebalance(rebalanceType nexus.RebalanceType,
	rebalanceInfo []nexus.RebalanceInfo) (err error) {

	switch rebalanceType {
	case nexus.Assign:
		select {
		case s.signalAssigned <- struct{}{}:
		default:
		}

		// reset BEFORE ackRebalance to minimise race window where orphaned WorkItems
		// (from drain timeout) could be processed with stale CommittedPlusOne.
		// ackRebalance involves a network round-trip - doing resets first ensures
		// CommittedPlusOne is updated before any orphaned WorkItem can win the race.
		s.resetFirstSeen(rebalanceInfo)
		s.resetCommittedOffsetsFromRebalanceInfo(rebalanceInfo)

		// mark partitions as assigned for orphaned WorkItem protection (see offset/COMMIT_GUARD_ANALYSIS.md)
		for _, r := range rebalanceInfo {
			s.markPartitionAssigned(r.Partition)
		}

		if err = s.ackRebalance(rebalanceType, rebalanceInfo); err != nil {
			s.logger.Error(s.ctx, fmt.Sprintf("assign ack error - %v", err.Error()))
		}

	case nexus.Revoke:
		select {
		case <-s.signalAssigned:
		default:
		}

		s.drain(SyncPollingAlreadyStopped, rebalanceInfo...)

		// mark partitions as revoked BEFORE ack to close race window
		// TLA+ verification (OffsetCommitterP3r.tla) proved that doing this AFTER ack
		// leaves a window where CommitTick can fire with committerAssigned=TRUE,
		// allowing orphaned WorkItem commits that violate monotonicity.
		// See offset/COMMIT_GUARD_ANALYSIS.md for the full counterexample trace.
		for _, r := range rebalanceInfo {
			s.markPartitionRevoked(r.Partition)
		}

		if err = s.ackRebalance(rebalanceType, rebalanceInfo); err != nil {
			s.logger.Error(s.ctx, fmt.Sprintf("revoke ack error - %v", err.Error()))
		}

	default:
		const unsupportedRebalanceType = "unsupported rebalance type: %v"
		err = fmt.Errorf(unsupportedRebalanceType, rebalanceType)
	}

	return err
}

// DrainBehaviour controls how the subscription coordinates with the polling loop during drain.
type DrainBehaviour int

// DrainBehaviour constants control polling coordination during drain.
const (
	// SyncPollingAlreadyStopped - polling loop is blocked in the same execution
	// context (e.g., Kafka RebalanceCb or poll-returned event). No pause needed.
	SyncPollingAlreadyStopped DrainBehaviour = iota + 1 // inside polling context, synchronous with it

	// AsyncStopPollingBeforeDrain - rebalance arrived on separate thread
	// from polling loop so must pause polling loop before draining.
	AsyncStopPollingBeforeDrain

	// ShutdownStopPollingBeforeDrain is used during graceful shutdown.
	ShutdownStopPollingBeforeDrain
)

func (s *Subscription[T]) drain(drainBehaviour DrainBehaviour, _ ...nexus.RebalanceInfo) {
	paused := false
	defer func() {
		if paused {
			select {
			case s.resumePolling <- struct{}{}:
			case <-time.After(time.Second):
				s.logger.Error(s.ctx, "resume polling timeout")
			}
		}
	}()

	switch drainBehaviour {
	case SyncPollingAlreadyStopped:
		// no-op, doing rebalance and currently inside
		// Poll() thread, no new messages can arrive

	case ShutdownStopPollingBeforeDrain:
		stopPollingTimeout := s.drainTimeout / 2 //nolint:mnd // half timeout for stop, half for drain
		select {
		case s.stopPolling <- struct{}{}:
			// polling loop stopped
		case <-time.After(stopPollingTimeout):
			const timeoutMessage = "timeout in %s waiting for to stop polling loop, proceeding with drain"
			s.logger.Error(s.ctx, fmt.Sprintf(timeoutMessage, stopPollingTimeout))
		}

	case AsyncStopPollingBeforeDrain:
		select {
		case s.pausePolling <- struct{}{}:
			// polling loop paused, will resume in defer
			paused = true

		case <-time.After(pausePollingTimeout):
			const timeoutMessage = "timeout waiting for polling to pause"
			s.logger.Error(s.ctx, timeoutMessage)
			s.circuitBreaker.TriggerEmergencyShutdown(errors.New(timeoutMessage))
			return
		}
	}

	if err := s.drainCoordinator.Drain(); err != nil {
		const drainFailed = "error draining pipeline.Processor for topic: %s"
		s.logger.Error(s.ctx, fmt.Sprintf(drainFailed, s.topicName), err)
	}
}

func (s *Subscription[T]) resetFirstSeen(rebalanceInfo []nexus.RebalanceInfo) {
	partitions := make([]int32, len(rebalanceInfo))
	for i, r := range rebalanceInfo {
		partitions[i] = r.Partition
	}
	s.processor.ResetPrevOffsets(partitions)
}

// resetCommittedOffsetsFromRebalanceInfo updates the committer's CommittedPlusOne for each
// partition from the broker's committed offset. This closes the race window where an orphaned
// WorkItem (from drain timeout) could arrive before the first new message updates
// CommittedPlusOne via the First flag. See offset/DESIGN.md for full explanation.
func (s *Subscription[T]) resetCommittedOffsetsFromRebalanceInfo(rebalanceInfo []nexus.RebalanceInfo) {
	partitionOffsets := make(map[int32]int64, len(rebalanceInfo))
	for _, r := range rebalanceInfo {
		partitionOffsets[r.Partition] = r.CommittedOffset
	}
	s.resetCommittedOffsets(partitionOffsets)
}
