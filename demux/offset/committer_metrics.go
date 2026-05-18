// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"time"

	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-nexus/nexus"
)

// metricsRingBuffer accumulates per-message stats in a sliding window.
//
// All mutating methods are called under oc.mu - no internal synchronization
// needed. This eliminates a separate mutex on the hot path entirely: the
// synchronization cost of metrics accumulation is zero (already paid for).
//
// Bucket duration and count are configurable: for example, 240 buckets of
// 15 seconds each covers one hour of history.
type metricsRingBuffer struct {
	buckets        []bucket
	currentIndex   int
	bucketCount    int   // number of buckets in the ring
	lastEpoch      int64 // now.Unix() / bucketSeconds
	totalProcessed int64
	bucketSeconds  int64 // time span of each bucket in seconds
}

// bucket accumulates stats for a single time slot.
type bucket struct {
	count              int64
	deadLetterCount    int64
	totalProcessNanos  int64
	totalEndToEndNanos int64
	maxProcessNanos    int64
	maxEndToEndNanos   int64
	minEndToEndNanos   int64
}

// newRingBuffer creates a ring buffer with the given bucket duration and count.
// Bucket duration is truncated to whole seconds (minimum 1 second).
func newRingBuffer(bucketDuration time.Duration, bucketCount int) *metricsRingBuffer {
	bucketSeconds := max(int64(bucketDuration/time.Second), 1)
	return &metricsRingBuffer{
		buckets:       make([]bucket, bucketCount),
		lastEpoch:     time.Now().Unix() / bucketSeconds,
		bucketSeconds: bucketSeconds,
		bucketCount:   bucketCount,
	}
}

// record accumulates a processed message into the current bucket.
// Called from returnMessageAndCollectMetrics under oc.mu - no lock needed.
func (rb *metricsRingBuffer) record(now time.Time, processDuration, endToEndDuration time.Duration, deadLetter bool) {
	nowEpoch := now.Unix() / rb.bucketSeconds
	if last := rb.lastEpoch; nowEpoch != last && nowEpoch > last {
		rb.advance(last, nowEpoch)
	}

	b := &rb.buckets[rb.currentIndex]

	processNanos := processDuration.Nanoseconds()
	endToEndNanos := endToEndDuration.Nanoseconds()

	b.count++
	if deadLetter {
		b.deadLetterCount++
	}
	b.totalProcessNanos += processNanos
	b.totalEndToEndNanos += endToEndNanos
	if processNanos > b.maxProcessNanos {
		b.maxProcessNanos = processNanos
	}
	if endToEndNanos > b.maxEndToEndNanos {
		b.maxEndToEndNanos = endToEndNanos
	}
	if b.minEndToEndNanos == 0 || endToEndNanos < b.minEndToEndNanos {
		b.minEndToEndNanos = endToEndNanos
	}

	rb.totalProcessed++
}

// advance moves the ring buffer forward, zeroing skipped buckets.
func (rb *metricsRingBuffer) advance(lastEpoch, nowEpoch int64) {
	gap := int(nowEpoch - lastEpoch)
	if gap > rb.bucketCount {
		gap = rb.bucketCount
	}

	oldIndex := rb.currentIndex
	for i := 1; i <= gap; i++ {
		idx := (oldIndex + i) % rb.bucketCount
		rb.buckets[idx] = bucket{}
	}

	rb.currentIndex = (oldIndex + gap) % rb.bucketCount
	rb.lastEpoch = nowEpoch
}

// windowData iterates ring buffer buckets and returns raw statistics.
// Called under oc.mu. Accumulates totals but does NOT divide - the
// caller computes averages after the lock is released.
func (rb *metricsRingBuffer) windowData() snapshot.WindowData {
	wd := snapshot.WindowData{
		TotalProcessed:      rb.totalProcessed,
		ThroughputPerBucket: make([]uint32, rb.bucketCount),
		DeadLetterPerBucket: make([]uint32, rb.bucketCount),
		BucketDuration:      time.Duration(rb.bucketSeconds) * time.Second,
	}

	idx := rb.currentIndex
	for i := 0; i < rb.bucketCount; i++ {
		bIdx := (idx + 1 + i) % rb.bucketCount
		b := &rb.buckets[bIdx]

		wd.ThroughputPerBucket[i] = uint32(b.count)
		wd.DeadLetterPerBucket[i] = uint32(b.deadLetterCount)
		wd.TotalCount += b.count
		wd.TotalProcessNanos += b.totalProcessNanos
		wd.TotalEndToEndNanos += b.totalEndToEndNanos
		if b.maxProcessNanos > wd.MaxProcessNanos {
			wd.MaxProcessNanos = b.maxProcessNanos
		}
		if b.maxEndToEndNanos > wd.MaxEndToEndNanos {
			wd.MaxEndToEndNanos = b.maxEndToEndNanos
		}
		if b.count > 0 && (wd.MinEndToEndNanos == 0 || b.minEndToEndNanos < wd.MinEndToEndNanos) {
			wd.MinEndToEndNanos = b.minEndToEndNanos
		}
	}

	return wd
}

// WindowData returns ring buffer statistics for snapshot capture.
// Acquires oc.mu to ensure a consistent read of the sliding window.
func (oc *Committer[T]) WindowData() snapshot.WindowData {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	wd := oc.metricsBuffer.windowData()
	wd.TotalProcessed = oc.totalProcessed
	return wd
}

// PreCommitsSnapshot returns a point-in-time view of the pre-commit state.
func (oc *Committer[T]) PreCommitsSnapshot() snapshot.PreCommitsSnapshot {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	ps := snapshot.PreCommitsSnapshot{
		LastRebalanceTime: oc.lastRebalanceTime,
		Partitions:        make([]snapshot.PartitionSnapshot, 0, len(oc.offsetsByPartition.PartitionMap)),
	}
	for partition, tracker := range oc.offsetsByPartition.PartitionMap {
		committedOffset := tracker.CommittedPlusOne - 1
		readyOffset := committedOffset // no advancement yet
		if tracker.Ready != nil {
			readyOffset = tracker.Ready.Message.Offset
		}
		// When CommittedPlusOne holds a sentinel (e.g. -1 initial value or
		// -1001 from kafka.OffsetInvalid passed via ResetCommittedOffsets),
		// no real commit has occurred yet. Clamp to -1 so that pending
		// reflects actual offsets (0-based) rather than inflating against
		// a large negative sentinel.
		if committedOffset < -1 {
			committedOffset = -1
		}
		ps.Partitions = append(ps.Partitions, snapshot.PartitionSnapshot{
			Partition:          partition,
			CommittedOffset:    committedOffset,
			HighestReadyOffset: readyOffset,
			MaxOffsetSeen:      tracker.MaxOffsetSeen,
			GapBufferDepth:     len(tracker.GapBuffer),
			Assigned:           tracker.Assignment == nexus.Assign,
		})
	}
	return ps
}
