// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"time"

	"github.com/llingr/llingr-demux/ports"
)

// CollectAndCommit receives processed containers for streaming watermark processing.
// Timestamps arrival and forwards to streaming algorithm (container lifecycle extended).
func (oc *Committer[T]) CollectAndCommit(workItem *ports.WorkItem[T]) {
	oc.commitsIn <- workItem
}

// amortizes mutex lock access
const readBatchSize = 1000

// startIngestLoop launches the ingest goroutine moving processed messages
// from the commitsIn channel into buffers.OffsetsByPartition.
func (oc *Committer[T]) startIngestLoop() {
	const ingestIdleTickInterval = 100 * time.Microsecond
	ingestGuardTicker := time.NewTicker(ingestIdleTickInterval)
	defer ingestGuardTicker.Stop()

	wallTime := time.Now()
	lastUpdate := wallTime

	updateNowTime := func() {
		// amortize wall time read to once per-second,
		// otherwise use the offset vs monotonic time
		if delta := time.Since(lastUpdate); delta > time.Second {
			wallTime = time.Now()
			lastUpdate = wallTime
		} else {
			wallTime = lastUpdate.Add(delta)
		}
	}

	for {
		select {
		case <-oc.ctx.Done():
			return

		case workItem := <-oc.commitsIn:
			<-ingestGuardTicker.C

			// ingest in batches to limit contention on mutex
			func(w *ports.WorkItem[T]) {
				updateNowTime()
				oc.mu.Lock()
				defer func() {
					defer oc.mu.Unlock()
					// sort and walk gap buffers, still under the batch/mutex
					oc.flushGapBuffers(wallTime)
					if len(oc.commitsIn) == 0 {
						select {
						case oc.drained <- struct{}{}:
						default:
							// no-op
						}
					}
				}()

				oc.processCommit(workItem, wallTime)

				// drain remaining messages in the channel up to batch size
				for messageCount := 1; messageCount < readBatchSize; messageCount++ {
					select {
					case workItem = <-oc.commitsIn:
						oc.processCommit(workItem, wallTime)
					default:
						return
					}
				}
			}(workItem)
		}
	}
}
