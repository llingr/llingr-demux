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
		return oc.CommitOffsets()
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
	return oc.CommitOffsets()
}
