// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

// DrainCommitter waits for pending commits to complete before shutdown.
//
// NOT safe for concurrent callers: drained carries a single token, so a second
// concurrent drain would miss it and burn the full timer. The drain Coordinator
// serializes all drains (revoke and shutdown paths both route through it).
func (oc *Committer[T]) DrainCommitter(timer *time.Timer) error {
	// Clear any stale signal first
	select {
	case <-oc.drained:
	default:
	}

	// yield: give ingest goroutine a chance to make progress - and possibly
	// signal drained - before we evaluate channel state and enter a wait.
	// This is a micro-optimization to reduce saturated pipeline tail-latency.
	runtime.Gosched()

	if len(oc.commitsIn) == 0 {
		// liveness bound: don't wait indefinitely if `drained` never arrives.

		const coldDrainGracePeriod = 10 * time.Millisecond

		select {
		case <-oc.drained:
			// drained signal arrived during the grace period
		case <-time.After(coldDrainGracePeriod):
			// either the pipeline is cold, or in-flight batch
			// processing outlasted the grace

			// mutex barrier synchronizes with ingest's batch completion
			// and handles the slow-batch case beyond grace-period timeout
			oc.mu.Lock()
			oc.mu.Unlock()
		}
		return oc.surfaceFinalCommit()
	}

	select {
	case <-oc.drained:
		// drain completed for last messages, immediately proceed to commit
	case <-timer.C:
		// timeout, commit what we can and log error
		err := oc.CommitOffsets()
		if err != nil {
			oc.logger.Error(oc.ctx, fmt.Sprintf("failed to commit offsets in timeout - %v", err))
		}
		return errors.New("timeout")
	}
	return oc.surfaceFinalCommit()
}

// surfaceFinalCommit runs the final drain commit. A broker rejection is
// surfaced loudly with its consequence but not escalated: the pipeline has
// drained, the partitions are leaving this consumer anyway, and the
// uncommitted tail is an at-least-once re-read for the next owner. Escalating
// would turn expected fencing (e.g. a lost-partitions callback, where group
// membership is already gone) into an emergency shutdown. Non-broker errors
// (e.g. the commit-guard timeout) still propagate.
func (oc *Committer[T]) surfaceFinalCommit() error {
	oc.logger.Debug(oc.ctx, "drain: sending final commit")
	err := oc.CommitOffsets()
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrBrokerCommitFailed) {
		oc.logger.Error(oc.ctx, fmt.Sprintf(
			"final drain commit failed - the uncommitted tail will be re-read by the partition's next owner: %v", err))
		return nil
	}
	return err
}
