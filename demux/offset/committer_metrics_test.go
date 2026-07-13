// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

func timeAt(second int64) time.Time {
	return time.Unix(second, 0)
}

// testRingBuffer creates a 1-second-per-bucket ring buffer with default count,
// seeded at the given start second - for deterministic tests with synthetic time.
func testRingBuffer(startSecond int64) *metricsRingBuffer {
	return testRingBufferWith(startSecond, time.Second, snapshot.DefaultBucketCount)
}

func testRingBufferWith(startSecond int64, bucketDuration time.Duration, bucketCount int) *metricsRingBuffer {
	bucketSeconds := max(int64(bucketDuration/time.Second), 1)
	return &metricsRingBuffer{
		buckets:       make([]bucket, bucketCount),
		lastEpoch:     startSecond / bucketSeconds,
		bucketSeconds: bucketSeconds,
		bucketCount:   bucketCount,
	}
}

// --- 1-second bucket tests (default configuration) ---

func Test_ringBuffer_SingleSecond_Accumulates(t *testing.T) {
	rb := testRingBuffer(1000)

	rb.record(timeAt(1000), 10*time.Millisecond, 50*time.Millisecond, false)
	rb.record(timeAt(1000), 30*time.Millisecond, 100*time.Millisecond, false)
	rb.record(timeAt(1000), 20*time.Millisecond, 75*time.Millisecond, false)

	b := rb.buckets[rb.currentIndex]

	if b.count != 3 {
		t.Errorf("count: expected 3, got %d", b.count)
	}

	expectProcess := int64(10+30+20) * int64(time.Millisecond)
	if b.totalProcessNanos != expectProcess {
		t.Errorf("totalProcessNanos: expected %d, got %d", expectProcess, b.totalProcessNanos)
	}

	expectE2E := int64(50+100+75) * int64(time.Millisecond)
	if b.totalEndToEndNanos != expectE2E {
		t.Errorf("totalEndToEndNanos: expected %d, got %d", expectE2E, b.totalEndToEndNanos)
	}

	if b.maxProcessNanos != (30 * time.Millisecond).Nanoseconds() {
		t.Errorf("maxProcessNanos: expected 30ms, got %d ns", b.maxProcessNanos)
	}
	if b.maxEndToEndNanos != (100 * time.Millisecond).Nanoseconds() {
		t.Errorf("maxEndToEndNanos: expected 100ms, got %d ns", b.maxEndToEndNanos)
	}
	if rb.totalProcessed != 3 {
		t.Errorf("totalProcessed: expected 3, got %d", rb.totalProcessed)
	}
}

func Test_ringBuffer_ConsecutiveSeconds_SeparateBuckets(t *testing.T) {
	rb := testRingBuffer(1000)

	rb.record(timeAt(1000), 10*time.Millisecond, 50*time.Millisecond, false)
	rb.record(timeAt(1001), 20*time.Millisecond, 60*time.Millisecond, false)
	rb.record(timeAt(1002), 30*time.Millisecond, 70*time.Millisecond, false)

	if rb.currentIndex != 2 {
		t.Fatalf("currentIndex: expected 2, got %d", rb.currentIndex)
	}

	for i := 0; i < 3; i++ {
		if rb.buckets[i].count != 1 {
			t.Errorf("bucket %d: expected count 1, got %d", i, rb.buckets[i].count)
		}
	}

	if rb.totalProcessed != 3 {
		t.Errorf("totalProcessed: expected 3, got %d", rb.totalProcessed)
	}
}

func Test_ringBuffer_GapSkip_ZeroesIntermediateBuckets(t *testing.T) {
	rb := testRingBuffer(1000)

	rb.record(timeAt(1000), 10*time.Millisecond, 50*time.Millisecond, false)
	// skip to second 1005 (5-second gap)
	rb.record(timeAt(1005), 20*time.Millisecond, 60*time.Millisecond, false)

	if rb.currentIndex != 5 {
		t.Fatalf("currentIndex: expected 5, got %d", rb.currentIndex)
	}

	// bucket 0: original data preserved (not in zeroed range 1..5)
	if rb.buckets[0].count != 1 {
		t.Errorf("bucket 0: expected count 1, got %d", rb.buckets[0].count)
	}

	// buckets 1-4: should be zeroed (gap)
	for i := 1; i <= 4; i++ {
		if rb.buckets[i].count != 0 {
			t.Errorf("bucket %d: expected count 0 (gap), got %d", i, rb.buckets[i].count)
		}
	}

	// bucket 5: new data
	if rb.buckets[5].count != 1 {
		t.Errorf("bucket 5: expected count 1, got %d", rb.buckets[5].count)
	}
}

func Test_ringBuffer_LargeGap_ZeroesEntireWindow(t *testing.T) {
	rb := testRingBuffer(1000)

	// fill several buckets
	for i := int64(0); i < 5; i++ {
		rb.record(timeAt(1000+i), 10*time.Millisecond, 50*time.Millisecond, false)
	}

	// jump beyond bucket count - all old data should be zeroed
	rb.record(timeAt(1000+int64(rb.bucketCount)+10), 20*time.Millisecond, 60*time.Millisecond, false)

	wd := rb.windowData()

	nonZero := 0
	for i := 0; i < rb.bucketCount; i++ {
		if wd.ThroughputPerBucket[i] > 0 {
			nonZero++
		}
	}
	if nonZero != 1 {
		t.Errorf("expected 1 non-zero bucket after large gap, got %d", nonZero)
	}

	// TotalProcessed is cumulative - never resets
	if wd.TotalProcessed != 6 {
		t.Errorf("totalProcessed: expected 6, got %d", wd.TotalProcessed)
	}
}

func Test_ringBuffer_windowData_OldestFirst(t *testing.T) {
	rb := testRingBuffer(1000)

	// record increasing counts: second 0 = 1 msg, second 1 = 2 msgs, ..., second 4 = 5 msgs
	for sec := int64(0); sec < 5; sec++ {
		for msg := int64(0); msg <= sec; msg++ {
			rb.record(timeAt(1000+sec), 10*time.Millisecond, 50*time.Millisecond, false)
		}
	}
	// currentIndex = 4
	// windowData iterates from (4+1)%15 = 5 → oldest
	// slots 5..14 are empty (10 zeros), then slots 0..4 have data

	wd := rb.windowData()

	// first 10 slots: empty (oldest seconds with no data)
	for i := 0; i < 10; i++ {
		if wd.ThroughputPerBucket[i] != 0 {
			t.Errorf("slot %d: expected 0, got %d", i, wd.ThroughputPerBucket[i])
		}
	}

	// last 5 slots: 1, 2, 3, 4, 5 (oldest data first)
	expected := []uint32{1, 2, 3, 4, 5}
	for i, exp := range expected {
		slot := 10 + i
		if wd.ThroughputPerBucket[slot] != exp {
			t.Errorf("slot %d: expected %d, got %d", slot, exp, wd.ThroughputPerBucket[slot])
		}
	}
}

func Test_ringBuffer_windowData_MaxAcrossWindow(t *testing.T) {
	rb := testRingBuffer(1000)

	// scatter max durations across different seconds
	rb.record(timeAt(1000), 10*time.Millisecond, 100*time.Millisecond, false) // low process, medium e2e
	rb.record(timeAt(1001), 50*time.Millisecond, 30*time.Millisecond, false)  // HIGH process, low e2e
	rb.record(timeAt(1002), 5*time.Millisecond, 200*time.Millisecond, false)  // low process, HIGH e2e

	wd := rb.windowData()

	if wd.MaxProcessNanos != (50 * time.Millisecond).Nanoseconds() {
		t.Errorf("maxProcessNanos: expected 50ms, got %d ns", wd.MaxProcessNanos)
	}
	if wd.MaxEndToEndNanos != (200 * time.Millisecond).Nanoseconds() {
		t.Errorf("maxEndToEndNanos: expected 200ms, got %d ns", wd.MaxEndToEndNanos)
	}
}

func Test_ringBuffer_windowData_TotalsForAverageComputation(t *testing.T) {
	rb := testRingBuffer(1000)

	// 4 messages across 2 seconds
	rb.record(timeAt(1000), 10*time.Millisecond, 100*time.Millisecond, false)
	rb.record(timeAt(1000), 30*time.Millisecond, 200*time.Millisecond, false)
	rb.record(timeAt(1001), 20*time.Millisecond, 150*time.Millisecond, false)
	rb.record(timeAt(1001), 40*time.Millisecond, 250*time.Millisecond, false)

	wd := rb.windowData()

	if wd.TotalCount != 4 {
		t.Errorf("totalCount: expected 4, got %d", wd.TotalCount)
	}

	expectProcess := int64(10+30+20+40) * int64(time.Millisecond)
	if wd.TotalProcessNanos != expectProcess {
		t.Errorf("totalProcessNanos: expected %d, got %d", expectProcess, wd.TotalProcessNanos)
	}

	expectE2E := int64(100+200+150+250) * int64(time.Millisecond)
	if wd.TotalEndToEndNanos != expectE2E {
		t.Errorf("totalEndToEndNanos: expected %d, got %d", expectE2E, wd.TotalEndToEndNanos)
	}

	// verify caller can compute correct average from raw totals
	avgProcess := time.Duration(wd.TotalProcessNanos / wd.TotalCount)
	if avgProcess != 25*time.Millisecond {
		t.Errorf("avg process: expected 25ms, got %v", avgProcess)
	}
	avgE2E := time.Duration(wd.TotalEndToEndNanos / wd.TotalCount)
	if avgE2E != 175*time.Millisecond {
		t.Errorf("avg e2e: expected 175ms, got %v", avgE2E)
	}
}

func Test_ringBuffer_Wraparound_EvictsOldest(t *testing.T) {
	rb := testRingBuffer(1000)

	// fill entire window: 1 message per second for bucketCount seconds
	for i := int64(0); i < int64(rb.bucketCount); i++ {
		rb.record(timeAt(1000+i), time.Duration(i+1)*time.Millisecond, 50*time.Millisecond, false)
	}

	// all buckets should have data
	wd := rb.windowData()
	for i := 0; i < rb.bucketCount; i++ {
		if wd.ThroughputPerBucket[i] != 1 {
			t.Errorf("slot %d: expected 1 before wraparound, got %d", i, wd.ThroughputPerBucket[i])
		}
	}

	// record 1 more second - oldest (second 1000) should be evicted
	rb.record(timeAt(1000+int64(rb.bucketCount)), 99*time.Millisecond, 99*time.Millisecond, false)

	wd = rb.windowData()

	// window should still show bucketCount entries, each with 1 message
	if wd.TotalCount != int64(rb.bucketCount) {
		t.Errorf("window count: expected %d, got %d", rb.bucketCount, wd.TotalCount)
	}

	// newest bucket (last slot) should have the 99ms record
	if wd.ThroughputPerBucket[rb.bucketCount-1] != 1 {
		t.Errorf("newest slot: expected 1, got %d", wd.ThroughputPerBucket[rb.bucketCount-1])
	}

	// the evicted second (1000) had process duration 1ms
	// the new second has 99ms - window max should be 99ms
	if wd.MaxProcessNanos != (99 * time.Millisecond).Nanoseconds() {
		t.Errorf("max process after wraparound: expected 99ms, got %d ns", wd.MaxProcessNanos)
	}

	// TotalProcessed spans all time
	if wd.TotalProcessed != int64(rb.bucketCount)+1 {
		t.Errorf("totalProcessed: expected %d, got %d", rb.bucketCount+1, wd.TotalProcessed)
	}
}

func Test_ringBuffer_TotalProcessed_NeverResets(t *testing.T) {
	rb := testRingBuffer(1000)

	// 100 messages across 20 seconds (exceeds default bucket count)
	for i := int64(0); i < 20; i++ {
		for j := 0; j < 5; j++ {
			rb.record(timeAt(1000+i), 10*time.Millisecond, 50*time.Millisecond, false)
		}
	}

	wd := rb.windowData()

	if wd.TotalProcessed != 100 {
		t.Errorf("totalProcessed: expected 100, got %d", wd.TotalProcessed)
	}

	// window should only contain last bucketCount seconds (15 × 5 = 75)
	if wd.TotalCount != int64(rb.bucketCount)*5 {
		t.Errorf("window count: expected %d, got %d", rb.bucketCount*5, wd.TotalCount)
	}
}

func Test_ringBuffer_EmptyWindow(t *testing.T) {
	rb := testRingBuffer(1000)

	wd := rb.windowData()

	if wd.TotalProcessed != 0 {
		t.Errorf("totalProcessed: expected 0, got %d", wd.TotalProcessed)
	}
	if wd.TotalCount != 0 {
		t.Errorf("totalCount: expected 0, got %d", wd.TotalCount)
	}
	if wd.MaxProcessNanos != 0 {
		t.Errorf("maxProcessNanos: expected 0, got %d", wd.MaxProcessNanos)
	}
	for i := 0; i < rb.bucketCount; i++ {
		if wd.ThroughputPerBucket[i] != 0 {
			t.Errorf("slot %d: expected 0, got %d", i, wd.ThroughputPerBucket[i])
		}
	}
}

func Test_ringBuffer_PerBucketMax_NotWindowMax(t *testing.T) {
	rb := testRingBuffer(1000)

	// second 1000: max is 10ms
	rb.record(timeAt(1000), 10*time.Millisecond, 50*time.Millisecond, false)
	rb.record(timeAt(1000), 5*time.Millisecond, 20*time.Millisecond, false)

	// second 1001: max is 3ms (lower than bucket 0's max)
	rb.record(timeAt(1001), 3*time.Millisecond, 15*time.Millisecond, false)
	rb.record(timeAt(1001), 1*time.Millisecond, 10*time.Millisecond, false)

	// verify per-bucket max is independent
	if rb.buckets[0].maxProcessNanos != (10 * time.Millisecond).Nanoseconds() {
		t.Errorf("bucket 0 max: expected 10ms, got %d ns", rb.buckets[0].maxProcessNanos)
	}
	if rb.buckets[1].maxProcessNanos != (3 * time.Millisecond).Nanoseconds() {
		t.Errorf("bucket 1 max: expected 3ms, got %d ns", rb.buckets[1].maxProcessNanos)
	}

	// window max should be the global max across all buckets
	wd := rb.windowData()
	if wd.MaxProcessNanos != (10 * time.Millisecond).Nanoseconds() {
		t.Errorf("window max: expected 10ms, got %d ns", wd.MaxProcessNanos)
	}
}

func Test_ringBuffer_DeadLetterCount(t *testing.T) {
	rb := testRingBuffer(1000)

	rb.record(timeAt(1000), 10*time.Millisecond, 50*time.Millisecond, false)
	rb.record(timeAt(1000), 20*time.Millisecond, 60*time.Millisecond, true) // dead letter
	rb.record(timeAt(1000), 15*time.Millisecond, 55*time.Millisecond, false)
	rb.record(timeAt(1001), 30*time.Millisecond, 70*time.Millisecond, true) // dead letter
	rb.record(timeAt(1001), 25*time.Millisecond, 65*time.Millisecond, true) // dead letter

	b0 := rb.buckets[0]
	if b0.count != 3 {
		t.Errorf("bucket 0 count: expected 3, got %d", b0.count)
	}
	if b0.deadLetterCount != 1 {
		t.Errorf("bucket 0 deadLetterCount: expected 1, got %d", b0.deadLetterCount)
	}

	b1 := rb.buckets[1]
	if b1.count != 2 {
		t.Errorf("bucket 1 count: expected 2, got %d", b1.count)
	}
	if b1.deadLetterCount != 2 {
		t.Errorf("bucket 1 deadLetterCount: expected 2, got %d", b1.deadLetterCount)
	}

	wd := rb.windowData()

	// verify DeadLetterPerBucket matches (oldest first in windowData)
	// currentIndex=1, so iteration starts at (1+1)%15=2, wraps: slots 2..14 are 0, then 0, then 1
	// slot 13 = bucket 0 (count=3, dl=1), slot 14 = bucket 1 (count=2, dl=2)
	lastSlot := len(wd.DeadLetterPerBucket) - 1
	if wd.DeadLetterPerBucket[lastSlot] != 2 {
		t.Errorf("newest dead letter slot: expected 2, got %d", wd.DeadLetterPerBucket[lastSlot])
	}
	if wd.DeadLetterPerBucket[lastSlot-1] != 1 {
		t.Errorf("second newest dead letter slot: expected 1, got %d", wd.DeadLetterPerBucket[lastSlot-1])
	}

	if rb.totalProcessed != 5 {
		t.Errorf("totalProcessed: expected 5, got %d", rb.totalProcessed)
	}
}

func Test_ringBuffer_windowData_BucketDuration(t *testing.T) {
	rb := testRingBuffer(1000)
	wd := rb.windowData()
	if wd.BucketDuration != time.Second {
		t.Errorf("expected BucketDuration 1s, got %v", wd.BucketDuration)
	}

	rb15 := testRingBufferWith(1000, 15*time.Second, 240)
	wd15 := rb15.windowData()
	if wd15.BucketDuration != 15*time.Second {
		t.Errorf("expected BucketDuration 15s, got %v", wd15.BucketDuration)
	}
}

// --- configurable bucket duration tests ---

func Test_ringBuffer_15SecondBuckets_GroupsCorrectly(t *testing.T) {
	// 4 buckets * 15 seconds = 60 second window
	rb := testRingBufferWith(1500, 15*time.Second, 4)

	// epoch at second 1500 = 1500/15 = 100
	// messages within the same 15-second window share a bucket
	rb.record(timeAt(1500), 10*time.Millisecond, 50*time.Millisecond, false) // epoch 100
	rb.record(timeAt(1507), 20*time.Millisecond, 60*time.Millisecond, false) // epoch 100 (same)
	rb.record(timeAt(1514), 30*time.Millisecond, 70*time.Millisecond, false) // epoch 100 (same)

	// all three should land in the same bucket
	b := rb.buckets[rb.currentIndex]
	if b.count != 3 {
		t.Errorf("expected 3 messages in same bucket, got %d", b.count)
	}

	// second 1515 = epoch 101, should advance
	rb.record(timeAt(1515), 5*time.Millisecond, 25*time.Millisecond, false)

	if rb.buckets[rb.currentIndex].count != 1 {
		t.Errorf("expected 1 message in new bucket, got %d", rb.buckets[rb.currentIndex].count)
	}

	if rb.totalProcessed != 4 {
		t.Errorf("totalProcessed: expected 4, got %d", rb.totalProcessed)
	}
}

func Test_ringBuffer_15SecondBuckets_Wraparound(t *testing.T) {
	// 4 buckets * 15s = 60 second window
	rb := testRingBufferWith(1500, 15*time.Second, 4)

	// fill all 4 buckets (epochs 100, 101, 102, 103)
	for i := int64(0); i < 4; i++ {
		sec := 1500 + i*15
		rb.record(timeAt(sec), time.Duration(i+1)*time.Millisecond, 50*time.Millisecond, false)
	}

	wd := rb.windowData()
	if wd.TotalCount != 4 {
		t.Errorf("expected 4 messages in window, got %d", wd.TotalCount)
	}
	for i := 0; i < 4; i++ {
		if wd.ThroughputPerBucket[i] != 1 {
			t.Errorf("slot %d: expected 1, got %d", i, wd.ThroughputPerBucket[i])
		}
	}

	// epoch 104 evicts epoch 100
	rb.record(timeAt(1500+4*15), 99*time.Millisecond, 99*time.Millisecond, false)

	wd = rb.windowData()
	if wd.TotalCount != 4 {
		t.Errorf("expected 4 after wraparound, got %d", wd.TotalCount)
	}
	if wd.TotalProcessed != 5 {
		t.Errorf("totalProcessed: expected 5, got %d", wd.TotalProcessed)
	}
}

func Test_ringBuffer_15SecondBuckets_GapSkip(t *testing.T) {
	// 4 buckets * 15s = 60 second window
	rb := testRingBufferWith(1500, 15*time.Second, 4)

	rb.record(timeAt(1500), 10*time.Millisecond, 50*time.Millisecond, false) // epoch 100

	// skip 2 epochs (30 seconds of silence)
	rb.record(timeAt(1545), 20*time.Millisecond, 60*time.Millisecond, false) // epoch 103

	wd := rb.windowData()
	if wd.TotalProcessed != 2 {
		t.Errorf("totalProcessed: expected 2, got %d", wd.TotalProcessed)
	}

	// only 2 buckets should have data (epoch 100 and 103)
	nonZero := 0
	for i := 0; i < 4; i++ {
		if wd.ThroughputPerBucket[i] > 0 {
			nonZero++
		}
	}
	if nonZero != 2 {
		t.Errorf("expected 2 non-zero buckets, got %d: %v", nonZero, wd.ThroughputPerBucket)
	}
}

func Test_ringBuffer_240Buckets_15Seconds_OneHourWindow(t *testing.T) {
	// David's example: 240 * 15s = 3600s = 1 hour
	const bucketCount = 240
	rb := testRingBufferWith(0, 15*time.Second, bucketCount)

	// record 1 message per 15-second epoch for 1 hour
	for epoch := int64(0); epoch < int64(bucketCount); epoch++ {
		sec := epoch * 15
		rb.record(timeAt(sec), time.Duration(epoch+1)*time.Microsecond, 50*time.Millisecond, false)
	}

	wd := rb.windowData()

	if len(wd.ThroughputPerBucket) != bucketCount {
		t.Fatalf("expected %d slots, got %d", bucketCount, len(wd.ThroughputPerBucket))
	}
	if wd.BucketDuration != 15*time.Second {
		t.Errorf("expected BucketDuration 15s, got %v", wd.BucketDuration)
	}
	if wd.TotalProcessed != bucketCount {
		t.Errorf("totalProcessed: expected %d, got %d", bucketCount, wd.TotalProcessed)
	}
	if wd.TotalCount != bucketCount {
		t.Errorf("window count: expected %d, got %d", bucketCount, wd.TotalCount)
	}

	// all slots should be populated
	for i := 0; i < bucketCount; i++ {
		if wd.ThroughputPerBucket[i] != 1 {
			t.Errorf("slot %d: expected 1, got %d", i, wd.ThroughputPerBucket[i])
		}
	}

	// max should be from the last epoch (240µs)
	if wd.MaxProcessNanos != (240 * time.Microsecond).Nanoseconds() {
		t.Errorf("maxProcessNanos: expected 240µs, got %d ns", wd.MaxProcessNanos)
	}

	// now record 1 more epoch - oldest should be evicted
	rb.record(timeAt(int64(bucketCount)*15), 1*time.Millisecond, 1*time.Millisecond, false)

	wd = rb.windowData()
	if wd.TotalProcessed != bucketCount+1 {
		t.Errorf("totalProcessed after eviction: expected %d, got %d", bucketCount+1, wd.TotalProcessed)
	}
	if wd.TotalCount != bucketCount {
		t.Errorf("window count after eviction: expected %d, got %d", bucketCount, wd.TotalCount)
	}
}

func Test_ringBuffer_SubSecondDuration_ClampsToOneSecond(t *testing.T) {
	rb := testRingBufferWith(1000, 500*time.Millisecond, 10)

	// 500ms should clamp to 1 second
	if rb.bucketSeconds != 1 {
		t.Errorf("expected bucketSeconds clamped to 1, got %d", rb.bucketSeconds)
	}

	wd := rb.windowData()
	if wd.BucketDuration != time.Second {
		t.Errorf("expected BucketDuration 1s (clamped), got %v", wd.BucketDuration)
	}
}

// --- real-time test: proves the sliding window works with wall clock ---

func Test_Committer_WindowData_ReturnsCorrectData(t *testing.T) {
	rb := testRingBuffer(1000)

	// populate ring buffer with known data
	rb.record(timeAt(1000), 10*time.Millisecond, 50*time.Millisecond, false)
	rb.record(timeAt(1001), 20*time.Millisecond, 60*time.Millisecond, false)
	rb.record(timeAt(1002), 30*time.Millisecond, 70*time.Millisecond, false)

	oc := &Committer[string]{
		mu:             &sync.Mutex{},
		metricsBuffer:  rb,
		totalProcessed: 3,
	}

	wd := oc.WindowData()

	if wd.TotalProcessed != 3 {
		t.Errorf("TotalProcessed: expected 3, got %d", wd.TotalProcessed)
	}
	if wd.TotalCount != 3 {
		t.Errorf("TotalCount: expected 3, got %d", wd.TotalCount)
	}
	if wd.BucketDuration != time.Second {
		t.Errorf("BucketDuration: expected 1s, got %v", wd.BucketDuration)
	}
	if wd.MaxProcessNanos != (30 * time.Millisecond).Nanoseconds() {
		t.Errorf("MaxProcessNanos: expected 30ms, got %d ns", wd.MaxProcessNanos)
	}
	if wd.MaxEndToEndNanos != (70 * time.Millisecond).Nanoseconds() {
		t.Errorf("MaxEndToEndNanos: expected 70ms, got %d ns", wd.MaxEndToEndNanos)
	}

	expectProcess := int64(10+20+30) * int64(time.Millisecond)
	if wd.TotalProcessNanos != expectProcess {
		t.Errorf("TotalProcessNanos: expected %d, got %d", expectProcess, wd.TotalProcessNanos)
	}
	expectE2E := int64(50+60+70) * int64(time.Millisecond)
	if wd.TotalEndToEndNanos != expectE2E {
		t.Errorf("TotalEndToEndNanos: expected %d, got %d", expectE2E, wd.TotalEndToEndNanos)
	}
	if len(wd.ThroughputPerBucket) != snapshot.DefaultBucketCount {
		t.Errorf("ThroughputPerBucket length: expected %d, got %d", snapshot.DefaultBucketCount, len(wd.ThroughputPerBucket))
	}
}

func Test_Committer_PreCommitsSnapshot_IncludesOffsets(t *testing.T) {
	rebalanceTime := time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC)

	oc := &Committer[string]{
		mu:                &sync.Mutex{},
		lastRebalanceTime: rebalanceTime,
		offsetsByPartition: &OffsetsByPartition[string]{
			PartitionMap: map[int32]*OffsetsTracker[string]{
				0: {
					CommittedPlusOne: 1050,
					Ready: &ports.WorkItem[string]{
						Message: &nexus.Message[string]{Partition: 0, Offset: 1052},
					},
					GapBuffer:  make([]*ports.WorkItem[string], 2),
					Assignment: nexus.Assign,
				},
				1: {
					CommittedPlusOne: 200,
					Ready:            nil, // no advancement
					Assignment:       nexus.Revoke,
				},
			},
		},
	}

	ps := oc.PreCommitsSnapshot()

	if !ps.LastRebalanceTime.Equal(rebalanceTime) {
		t.Errorf("LastRebalanceTime: expected %v, got %v", rebalanceTime, ps.LastRebalanceTime)
	}
	if len(ps.Partitions) != 2 {
		t.Fatalf("Partitions: expected 2, got %d", len(ps.Partitions))
	}

	// map iteration order is nondeterministic - find by partition ID
	partitionByID := make(map[int32]snapshot.PartitionSnapshot)
	for _, p := range ps.Partitions {
		partitionByID[p.Partition] = p
	}

	p0 := partitionByID[0]
	if p0.CommittedOffset != 1049 {
		t.Errorf("partition 0 CommittedOffset: expected 1049, got %d", p0.CommittedOffset)
	}
	if p0.HighestReadyOffset != 1052 {
		t.Errorf("partition 0 HighestReadyOffset: expected 1052, got %d", p0.HighestReadyOffset)
	}
	if p0.GapBufferDepth != 2 {
		t.Errorf("partition 0 GapBufferDepth: expected 2, got %d", p0.GapBufferDepth)
	}
	if !p0.Assigned {
		t.Error("partition 0 Assigned: expected true")
	}

	p1 := partitionByID[1]
	if p1.CommittedOffset != 199 {
		t.Errorf("partition 1 CommittedOffset: expected 199, got %d", p1.CommittedOffset)
	}
	if p1.HighestReadyOffset != 199 {
		t.Errorf("partition 1 HighestReadyOffset: expected 199 (same as committed, no Ready), got %d", p1.HighestReadyOffset)
	}
	if p1.Assigned {
		t.Error("partition 1 Assigned: expected false")
	}
}

// Test_returnMessageAndCollectMetrics_DeadLetterTrait verifies that the
// DeadLetter trait on a WorkItem's Metrics flows through to the ring buffer's
// dead letter count. This is the glue between trait flags and ring buffer stats.
func Test_returnMessageAndCollectMetrics_DeadLetterTrait(t *testing.T) {
	rb := testRingBuffer(1000)
	now := timeAt(1000)

	var collected []*ports.WorkItem[string]
	oc := &Committer[string]{
		metricsBuffer:  rb,
		collectMetrics: func(wi *ports.WorkItem[string]) { collected = append(collected, wi) },
	}

	// normal message - no DeadLetter trait
	// ProcessStartTime + ProcessDuration must land at epoch 1000 for correct bucketing
	normalItem := &ports.WorkItem[string]{
		Message: &nexus.Message[string]{Partition: 0, Offset: 1},
		Metrics: &nexus.Metrics{
			ReadTime:             now.Add(-50 * time.Millisecond),
			ProcessStartTime:     now.Add(-10 * time.Millisecond),
			ProcessDuration:      10 * time.Millisecond,
			WatermarkAdvanceTime: now,
		},
	}
	oc.returnMessageAndCollectMetrics(normalItem)

	// dead letter message - DeadLetter trait set
	// processedAt = ProcessStartTime + ProcessDuration (dead letter write time excluded)
	dlItem := &ports.WorkItem[string]{
		Message: &nexus.Message[string]{Partition: 0, Offset: 2},
		Metrics: &nexus.Metrics{
			ReadTime:                now.Add(-60 * time.Millisecond),
			ProcessStartTime:        now.Add(-20 * time.Millisecond),
			ProcessDuration:         20 * time.Millisecond,
			WriteDeadLetterDuration: 5 * time.Millisecond, // present but not used for bucketing
			WatermarkAdvanceTime:    now,
		},
	}
	nexus.SetDeadLetter(&dlItem.Metrics.Traits)
	oc.returnMessageAndCollectMetrics(dlItem)

	// verify ring buffer captured the dead letter
	wd := rb.windowData()
	if wd.TotalProcessed != 2 {
		t.Errorf("TotalProcessed: expected 2, got %d", wd.TotalProcessed)
	}

	// sum dead letter counts across all buckets
	var totalDeadLetters uint32
	for _, dl := range wd.DeadLetterPerBucket {
		totalDeadLetters += dl
	}
	if totalDeadLetters != 1 {
		t.Errorf("dead letter count: expected 1, got %d", totalDeadLetters)
	}

	// verify collectMetrics was called for both
	if len(collected) != 2 {
		t.Errorf("collectMetrics called %d times, expected 2", len(collected))
	}

	// verify WatermarkAdvanceTime was set
	if normalItem.Metrics.WatermarkAdvanceTime != now {
		t.Error("normal item WatermarkAdvanceTime not set")
	}
	if dlItem.Metrics.WatermarkAdvanceTime != now {
		t.Error("dead letter item WatermarkAdvanceTime not set")
	}
}

// Test_returnMessageAndCollectMetrics_E2E_UsesWatermarkAdvanceTime verifies that
// end-to-end duration is derived from WatermarkAdvanceTime (when item became Ready)
// rather than a later collection time. This prevents the commit timer from inflating
// e2e latency for the last message on each partition.
func Test_returnMessageAndCollectMetrics_E2E_UsesWatermarkAdvanceTime(t *testing.T) {
	rb := testRingBuffer(1000)

	oc := &Committer[string]{
		metricsBuffer:  rb,
		collectMetrics: func(_ *ports.WorkItem[string]) {},
	}

	readTime := timeAt(1000)
	readyTime := readTime.Add(50 * time.Millisecond) // item became Ready 50ms after read

	workItem := &ports.WorkItem[string]{
		Message: &nexus.Message[string]{Partition: 0, Offset: 1},
		Metrics: &nexus.Metrics{
			ReadTime:             readTime,
			ProcessStartTime:     readTime.Add(5 * time.Millisecond),
			ProcessDuration:      10 * time.Millisecond,
			WatermarkAdvanceTime: readyTime, // stamped when item became Ready
		},
	}

	// call returnMessageAndCollectMetrics - e2e should be 50ms (readyTime - readTime),
	// NOT inflated by any later "now" time
	oc.returnMessageAndCollectMetrics(workItem)

	wd := rb.windowData()

	expectedE2E := (50 * time.Millisecond).Nanoseconds()
	actualAvgE2E := wd.TotalEndToEndNanos / wd.TotalCount
	if actualAvgE2E != expectedE2E {
		t.Errorf("e2e duration: expected %v, got %v (should use WatermarkAdvanceTime, not collection time)",
			time.Duration(expectedE2E), time.Duration(actualAvgE2E))
	}
}

// Test_returnMessageAndCollectMetrics_BucketsAcross15Seconds sends 30 messages
// (1 normal + 1 dead letter per second) across 15 seconds with varying
// ProcessDuration and end-to-end latency in 3 tiers and verifies:
//   - each bucket has 2 messages with 1 dead letter
//   - avg/max ProcessDuration: avg=20ms, max=30ms
//   - avg/max EndToEndDuration: avg=200ms, max=300ms
//   - correct total counts
func Test_returnMessageAndCollectMetrics_BucketsAcross15Seconds(t *testing.T) {
	const bucketCount = snapshot.DefaultBucketCount // 15
	startEpoch := int64(985)
	rb := testRingBuffer(startEpoch)
	watermarkNow := timeAt(1000) // watermark advance time

	oc := &Committer[string]{
		metricsBuffer:  rb,
		collectMetrics: func(_ *ports.WorkItem[string]) {}, // no-op
	}

	// 3 tiers: seconds 0-4, 5-9, 10-14
	// ProcessDuration: 10ms, 20ms, 30ms → avg=20ms, max=30ms
	// EndToEnd (watermarkNow - ReadTime): 100ms, 200ms, 300ms → avg=200ms, max=300ms
	tierProcess := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
	tierE2E := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}

	for i := 0; i < bucketCount; i++ {
		epoch := startEpoch + int64(i)
		tier := i / 5
		processDuration := tierProcess[tier]
		readTime := watermarkNow.Add(-tierE2E[tier]) // e2e = watermarkNow - readTime
		processedAt := timeAt(epoch)

		// normal message: ProcessStartTime + ProcessDuration = processedAt
		normal := &ports.WorkItem[string]{
			Message: &nexus.Message[string]{Partition: 0, Offset: int64(i * 2)},
			Metrics: &nexus.Metrics{
				ReadTime:             readTime,
				ProcessStartTime:     processedAt.Add(-processDuration),
				ProcessDuration:      processDuration,
				WatermarkAdvanceTime: watermarkNow,
			},
		}
		oc.returnMessageAndCollectMetrics(normal)

		// dead letter message: same tier durations, DeadLetter trait set
		dl := &ports.WorkItem[string]{
			Message: &nexus.Message[string]{Partition: 0, Offset: int64(i*2 + 1)},
			Metrics: &nexus.Metrics{
				ReadTime:             readTime,
				ProcessStartTime:     processedAt.Add(-processDuration),
				ProcessDuration:      processDuration,
				WatermarkAdvanceTime: watermarkNow,
			},
		}
		nexus.SetDeadLetter(&dl.Metrics.Traits)
		oc.returnMessageAndCollectMetrics(dl)
	}

	wd := rb.windowData()

	if wd.TotalProcessed != 30 {
		t.Fatalf("TotalProcessed: expected 30, got %d", wd.TotalProcessed)
	}
	if wd.TotalCount != 30 {
		t.Errorf("TotalCount: expected 30, got %d", wd.TotalCount)
	}

	// windowData returns buckets oldest-first: index 0 = epoch 985, index 14 = epoch 999
	for i := 0; i < bucketCount; i++ {
		if wd.ThroughputPerBucket[i] != 2 {
			t.Errorf("bucket[%d] (epoch %d) processedCount: expected 2, got %d",
				i, startEpoch+int64(i), wd.ThroughputPerBucket[i])
		}
		if wd.DeadLetterPerBucket[i] != 1 {
			t.Errorf("bucket[%d] (epoch %d) deadLetterCount: expected 1, got %d",
				i, startEpoch+int64(i), wd.DeadLetterPerBucket[i])
		}
	}

	// average ProcessDuration: (10*10ms + 10*20ms + 10*30ms) / 30 = 20ms
	expectedAvgProcess := (20 * time.Millisecond).Nanoseconds()
	actualAvgProcess := wd.TotalProcessNanos / wd.TotalCount
	if actualAvgProcess != expectedAvgProcess {
		t.Errorf("avg ProcessDuration: expected %v, got %v",
			time.Duration(expectedAvgProcess), time.Duration(actualAvgProcess))
	}

	// max ProcessDuration: 30ms
	expectedMaxProcess := (30 * time.Millisecond).Nanoseconds()
	if wd.MaxProcessNanos != expectedMaxProcess {
		t.Errorf("max ProcessDuration: expected %v, got %v",
			time.Duration(expectedMaxProcess), time.Duration(wd.MaxProcessNanos))
	}

	// average EndToEndDuration: (10*100ms + 10*200ms + 10*300ms) / 30 = 200ms
	expectedAvgE2E := (200 * time.Millisecond).Nanoseconds()
	actualAvgE2E := wd.TotalEndToEndNanos / wd.TotalCount
	if actualAvgE2E != expectedAvgE2E {
		t.Errorf("avg EndToEndDuration: expected %v, got %v",
			time.Duration(expectedAvgE2E), time.Duration(actualAvgE2E))
	}

	// max EndToEndDuration: 300ms
	expectedMaxE2E := (300 * time.Millisecond).Nanoseconds()
	if wd.MaxEndToEndNanos != expectedMaxE2E {
		t.Errorf("max EndToEndDuration: expected %v, got %v",
			time.Duration(expectedMaxE2E), time.Duration(wd.MaxEndToEndNanos))
	}
}

// Test_Committer_SnapshotHandler_ReflectsCommitterState wires a real Committer
// (with known partition state and ring buffer data) through a real Recorder and
// snapshot HTTP handler, verifying the JSON response contains actual committer state.
// This proves the full chain: Committer -> Recorder -> Handler -> JSON.
func Test_Committer_SnapshotHandler_ReflectsCommitterState(t *testing.T) {
	rebalanceTime := time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC)
	rb := testRingBuffer(1000)

	// record 3 messages with known latencies
	rb.record(timeAt(1000), 10*time.Millisecond, 50*time.Millisecond, false)
	rb.record(timeAt(1000), 20*time.Millisecond, 60*time.Millisecond, false)
	rb.record(timeAt(1000), 30*time.Millisecond, 70*time.Millisecond, true) // dead letter

	oc := &Committer[string]{
		mu:                &sync.Mutex{},
		lastRebalanceTime: rebalanceTime,
		metricsBuffer:     rb,
		totalProcessed:    3,
		offsetsByPartition: &OffsetsByPartition[string]{
			PartitionMap: map[int32]*OffsetsTracker[string]{
				0: {
					CommittedPlusOne: 1050,
					Ready: &ports.WorkItem[string]{
						Message: &nexus.Message[string]{Partition: 0, Offset: 1052},
					},
					GapBuffer:  make([]*ports.WorkItem[string], 3),
					Assignment: nexus.Assign,
				},
			},
		},
	}

	recorder := snapshot.NewRecorder(
		"orders.completed",
		func() snapshot.ConcurrencySnapshot {
			return snapshot.ConcurrencySnapshot{GuardActive: 7, GuardCapacity: 250}
		},
		func() []snapshot.ShardSnapshot {
			return []snapshot.ShardSnapshot{{Shard: 0, ActiveWorkers: 2, PooledWorkers: 4}}
		},
		oc.PreCommitsSnapshot,
		oc.WindowData,
	)

	handler := snapshot.NewHandler(recorder.TakeSnapshot)

	req := httptest.NewRequest(http.MethodGet, "/snapshot", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	// verify summary reflects committer's totalProcessed and partition count
	summary := result["summary"].(map[string]any)
	if summary["topicName"] != "orders.completed" {
		t.Errorf("topicName: expected \"orders.completed\", got %v", summary["topicName"])
	}
	if summary["totalProcessed"] != float64(3) {
		t.Errorf("totalProcessed: expected 3, got %v", summary["totalProcessed"])
	}
	if summary["assignedPartitionCount"] != float64(1) {
		t.Errorf("assignedPartitionCount: expected 1, got %v", summary["assignedPartitionCount"])
	}

	// verify preCommits reflects real committer partition state
	preCommits := result["preCommits"].(map[string]any)
	partitions := preCommits["partitions"].([]any)
	if len(partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(partitions))
	}
	p := partitions[0].(map[string]any)
	if p["partition"] != float64(0) {
		t.Errorf("partition: expected 0, got %v", p["partition"])
	}
	if p["committedOffset"] != float64(1049) {
		t.Errorf("committedOffset: expected 1049, got %v", p["committedOffset"])
	}
	if p["highestReadyOffset"] != float64(1052) {
		t.Errorf("highestReadyOffset: expected 1052, got %v", p["highestReadyOffset"])
	}
	if p["gapBufferDepth"] != float64(3) {
		t.Errorf("gapBufferDepth: expected 3, got %v", p["gapBufferDepth"])
	}
	if p["assigned"] != true {
		t.Errorf("assigned: expected true, got %v", p["assigned"])
	}

	// verify throughput reflects ring buffer data
	throughput := result["throughput"].(map[string]any)
	if throughput["maxProcessDuration"] != "30ms" {
		t.Errorf("maxProcessDuration: expected \"30ms\", got %v", throughput["maxProcessDuration"])
	}
	if throughput["maxEndToEndDuration"] != "70ms" {
		t.Errorf("maxEndToEndDuration: expected \"70ms\", got %v", throughput["maxEndToEndDuration"])
	}
	if throughput["avgProcessDuration"] != "20ms" {
		t.Errorf("avgProcessDuration: expected \"20ms\", got %v", throughput["avgProcessDuration"])
	}
	if throughput["totalDeadLettered"] != float64(1) {
		t.Errorf("totalDeadLettered: expected 1, got %v", throughput["totalDeadLettered"])
	}

	// verify throughput windows contain per-bucket data from the ring buffer
	windows := throughput["windows"].([]any)
	if len(windows) != snapshot.DefaultBucketCount {
		t.Fatalf("expected %d windows, got %d", snapshot.DefaultBucketCount, len(windows))
	}
	// all 3 messages recorded at same epoch land in the most recent bucket (index 0)
	mostRecent := windows[0].(map[string]any)
	if mostRecent["processedCount"] != float64(3) {
		t.Errorf("windows[0].processedCount: expected 3, got %v", mostRecent["processedCount"])
	}
	if mostRecent["deadLetterCount"] != float64(1) {
		t.Errorf("windows[0].deadLetterCount: expected 1, got %v", mostRecent["deadLetterCount"])
	}
	// verify timestamps are present (fromTime/toTime)
	if _, ok := mostRecent["fromTime"]; !ok {
		t.Error("windows[0] missing fromTime")
	}
	if _, ok := mostRecent["toTime"]; !ok {
		t.Error("windows[0] missing toTime")
	}

	// verify concurrency comes through
	concurrency := result["concurrency"].(map[string]any)
	if concurrency["guardActive"] != float64(7) {
		t.Errorf("guardActive: expected 7, got %v", concurrency["guardActive"])
	}

	// verify shards come through
	shards := result["shards"].([]any)
	if len(shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(shards))
	}
	shard := shards[0].(map[string]any)
	if shard["activeWorkers"] != float64(2) {
		t.Errorf("activeWorkers: expected 2, got %v", shard["activeWorkers"])
	}
}

func Test_ringBuffer_RealTime_SlidingWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-time ring buffer test in short mode")
	}

	rb := newRingBuffer(snapshot.DefaultBucketDuration, snapshot.DefaultBucketCount)

	// Phase 1: record 10 messages/sec for 5 seconds
	for sec := 0; sec < 5; sec++ {
		now := time.Now()
		for i := 0; i < 10; i++ {
			rb.record(now, 10*time.Millisecond, 50*time.Millisecond, false)
		}
		time.Sleep(time.Second)
	}

	wd := rb.windowData()
	if wd.TotalProcessed != 50 {
		t.Errorf("phase 1: expected totalProcessed 50, got %d", wd.TotalProcessed)
	}

	// throughput slots should show ~10 msgs/sec in recent buckets
	nonZeroPhase1 := 0
	for i := 0; i < len(wd.ThroughputPerBucket); i++ {
		if wd.ThroughputPerBucket[i] > 0 {
			nonZeroPhase1++
		}
	}
	if nonZeroPhase1 < 4 || nonZeroPhase1 > 6 {
		t.Errorf("phase 1: expected ~5 non-zero buckets, got %d: %v",
			nonZeroPhase1, wd.ThroughputPerBucket)
	}
	t.Logf("phase 1: %d non-zero buckets, throughput: %v", nonZeroPhase1, wd.ThroughputPerBucket)

	// Phase 2: wait for the entire window to expire
	t.Log("phase 2: waiting for window to expire...")
	time.Sleep(time.Duration(snapshot.DefaultBucketCount) * time.Second)

	// record 1 message to advance the ring buffer past the stale data
	now := time.Now()
	rb.record(now, 5*time.Millisecond, 25*time.Millisecond, false)

	wd = rb.windowData()

	// TotalProcessed is cumulative - all 51 messages
	if wd.TotalProcessed != 51 {
		t.Errorf("phase 2: expected totalProcessed 51, got %d", wd.TotalProcessed)
	}

	// window count should be just 1 - old data has faded out
	if wd.TotalCount != 1 {
		t.Errorf("phase 2: expected window count 1 (old data expired), got %d", wd.TotalCount)
	}

	// only 1 non-zero throughput bucket
	nonZeroPhase2 := 0
	for i := 0; i < len(wd.ThroughputPerBucket); i++ {
		if wd.ThroughputPerBucket[i] > 0 {
			nonZeroPhase2++
		}
	}
	if nonZeroPhase2 != 1 {
		t.Errorf("phase 2: expected 1 non-zero bucket, got %d: %v",
			nonZeroPhase2, wd.ThroughputPerBucket)
	}
	t.Logf("phase 2: window expired, %d non-zero buckets, throughput: %v",
		nonZeroPhase2, wd.ThroughputPerBucket)
}

// Test_PreCommitsSnapshot_ThroughProcessCommit sends messages through processCommit
// across 5 partitions with varying gap/contiguity states, then verifies that
// PreCommitsSnapshot() accurately reflects the real committed offsets, ready
// offsets, gap buffer depths, and assignment status for each partition.
//
// Partition scenarios:
//
//	P0: fully contiguous (100-104) - Ready=104, gapBuffer=[]
//	P1: gaps at 202,204 - Ready=201, gapBuffer=[203,205]
//	P2: no contiguous start (302,304 only) - Ready=nil, gapBuffer=[302,304], revoked
//	P3: no messages sent - Ready=nil, gapBuffer=[]
//	P4: gap then fill (500,502,501) - Ready=502, gapBuffer=[] (gap resolved)
func Test_PreCommitsSnapshot_ThroughProcessCommit(t *testing.T) {
	now := time.Now()

	var collectedCount int
	oc := &Committer[string]{
		mu:                 new(sync.Mutex),
		offsetsByPartition: New[string](16),
		metricsBuffer:      newRingBuffer(snapshot.DefaultBucketDuration, snapshot.DefaultBucketCount),
		collectMetrics:     func(_ *ports.WorkItem[string]) { collectedCount++ },
		gapBufferSize:      100,
		gapBufferSize70Pct: 70,
		logger:             nexus.NewDefaultLogger(slog.LevelInfo),
		ctx:                context.Background(),
	}

	// initialise committed offsets for all partitions via rebalance reset
	oc.ResetCommittedOffsets(map[int32]int64{
		0: 100,
		1: 200,
		2: 300,
		3: 400,
		4: 500,
	})

	makeWorkItem := func(partition int32, offset, prevOffset int64, first bool) *ports.WorkItem[string] {
		return &ports.WorkItem[string]{
			Message: &nexus.Message[string]{Partition: partition, Offset: offset},
			Metrics: &nexus.Metrics{
				ReadTime:         now.Add(-100 * time.Millisecond),
				ProcessStartTime: now.Add(-50 * time.Millisecond),
				ProcessDuration:  50 * time.Millisecond,
			},
			PreviousOffset: prevOffset,
			First:          first,
		}
	}

	oc.mu.Lock()

	// Partition 0: fully contiguous 100-104
	// Each offset advances Ready: 100 becomes Ready, 101 swaps 100 out, ..., 104 is final Ready
	oc.processCommit(makeWorkItem(0, 100, 0, true), now)
	oc.processCommit(makeWorkItem(0, 101, 100, false), now)
	oc.processCommit(makeWorkItem(0, 102, 101, false), now)
	oc.processCommit(makeWorkItem(0, 103, 102, false), now)
	oc.processCommit(makeWorkItem(0, 104, 103, false), now)

	// Partition 1: 200,201 contiguous then gaps at 202,204
	// 200 becomes Ready, 201 swaps 200 out. 203 and 205 go to gap buffer.
	oc.processCommit(makeWorkItem(1, 200, 0, true), now)
	oc.processCommit(makeWorkItem(1, 201, 200, false), now)
	oc.processCommit(makeWorkItem(1, 203, 202, false), now)
	oc.processCommit(makeWorkItem(1, 205, 204, false), now)

	// Partition 2: no contiguous start - offsets 302,304 both go to gap buffer
	// CommittedPlusOne=300, neither 302 nor 304 == 300, so both buffered
	oc.processCommit(makeWorkItem(2, 302, 301, false), now)
	oc.processCommit(makeWorkItem(2, 304, 303, false), now)

	// Partition 3: no messages sent - tracker exists from ResetCommittedOffsets

	// Partition 4: gap then fill - 500 becomes Ready, 502 buffered, 501 fills the gap
	// When 501 arrives: Ready(500) swaps out, Ready=501, gap buffer scanned - 502 contiguous - Ready=502
	oc.processCommit(makeWorkItem(4, 500, 0, true), now)
	oc.processCommit(makeWorkItem(4, 502, 501, false), now)
	oc.processCommit(makeWorkItem(4, 501, 500, false), now)

	// flush deferred gap buffer walks before unlocking
	oc.flushGapBuffers(now)

	// flip partition 2 to Revoked for assignment status test
	oc.offsetsByPartition.PartitionMap[2].Assignment = nexus.Revoke

	oc.mu.Unlock()

	// verify collected count: P0=4 (100-103 swapped out), P1=1 (200 swapped out),
	// P2=0, P3=0, P4=2 (500 swapped, 501 swapped when gap resolved)
	if collectedCount != 7 {
		t.Errorf("collected count: expected 7, got %d", collectedCount)
	}

	// take snapshot and build lookup
	ps := oc.PreCommitsSnapshot()
	if len(ps.Partitions) != 5 {
		t.Fatalf("expected 5 partitions, got %d", len(ps.Partitions))
	}

	byPartition := make(map[int32]snapshot.PartitionSnapshot)
	for _, p := range ps.Partitions {
		byPartition[p.Partition] = p
	}

	assertPartition := func(partition int32, committedOffset, highestReadyOffset int64, gapBufferDepth int, assigned bool) {
		t.Helper()
		p, ok := byPartition[partition]
		if !ok {
			t.Fatalf("partition %d: not found in snapshot", partition)
		}
		if p.CommittedOffset != committedOffset {
			t.Errorf("partition %d CommittedOffset: expected %d, got %d", partition, committedOffset, p.CommittedOffset)
		}
		if p.HighestReadyOffset != highestReadyOffset {
			t.Errorf("partition %d HighestReadyOffset: expected %d, got %d", partition, highestReadyOffset, p.HighestReadyOffset)
		}
		if p.GapBufferDepth != gapBufferDepth {
			t.Errorf("partition %d GapBufferDepth: expected %d, got %d", partition, gapBufferDepth, p.GapBufferDepth)
		}
		if p.Assigned != assigned {
			t.Errorf("partition %d Assigned: expected %v, got %v", partition, assigned, p.Assigned)
		}
	}

	// P0: CommittedPlusOne=100 - committedOffset=99, Ready.offset=104, no gaps, assigned
	assertPartition(0, 99, 104, 0, true)

	// P1: CommittedPlusOne=200 - committedOffset=199, Ready.offset=201, 2 gap items, assigned
	assertPartition(1, 199, 201, 2, true)

	// P2: CommittedPlusOne=300 - committedOffset=299, no Ready (=299), 2 gap items, revoked
	assertPartition(2, 299, 299, 2, false)

	// P3: CommittedPlusOne=400 - committedOffset=399, no Ready (=399), no gaps, assigned
	assertPartition(3, 399, 399, 0, true)

	// P4: CommittedPlusOne=500 - committedOffset=499, Ready.offset=502, gap resolved, assigned
	assertPartition(4, 499, 502, 0, true)
}
