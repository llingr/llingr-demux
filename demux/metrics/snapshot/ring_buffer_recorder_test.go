// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package snapshot

import (
	"testing"
	"time"
)

func testWindowData() WindowData {
	return WindowData{
		ThroughputPerBucket: []uint32{10, 20, 30},
		DeadLetterPerBucket: []uint32{1, 0, 2},
		BucketDuration:      15 * time.Second,
		TotalProcessed:      1000,
		TotalCount:          60,
		TotalProcessNanos:   600_000_000,   // 600ms total → 10ms avg
		TotalEndToEndNanos:  3_000_000_000, // 3s total → 50ms avg
		MaxProcessNanos:     50_000_000,    // 50ms
		MaxEndToEndNanos:    200_000_000,   // 200ms
	}
}

func TestNewRecorder_AllSamplersStored(t *testing.T) {
	called := make(map[string]bool)

	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot { called["concurrency"] = true; return ConcurrencySnapshot{} },
		func() []ShardSnapshot { called["shardInfo"] = true; return nil },
		func() PreCommitsSnapshot { called["preCommits"] = true; return PreCommitsSnapshot{} },
		func() WindowData { called["windowData"] = true; return WindowData{} },
	)

	// trigger all samplers via TakeSnapshot
	r.TakeSnapshot()

	expected := []string{"concurrency", "shardInfo", "preCommits", "windowData"}
	for _, name := range expected {
		if !called[name] {
			t.Errorf("sampler %q was not called by TakeSnapshot", name)
		}
	}
}

func TestTakeSnapshot_CopiesWindowDataFields(t *testing.T) {
	wd := testWindowData()

	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot { return ConcurrencySnapshot{} },
		func() []ShardSnapshot { return nil },
		func() PreCommitsSnapshot { return PreCommitsSnapshot{} },
		func() WindowData { return wd },
	)

	snap := r.TakeSnapshot()

	if snap.Summary.TotalProcessed != 1000 {
		t.Errorf("Summary.TotalProcessed: expected 1000, got %d", snap.Summary.TotalProcessed)
	}
	if snap.Throughput.MaxProcessDuration != 50*time.Millisecond {
		t.Errorf("MaxProcessDuration: expected 50ms, got %v", snap.Throughput.MaxProcessDuration)
	}
	if snap.Throughput.MaxEndToEndDuration != 200*time.Millisecond {
		t.Errorf("MaxEndToEndDuration: expected 200ms, got %v", snap.Throughput.MaxEndToEndDuration)
	}

	// throughput windows: 3 entries (from 3 buckets), most recent first
	if len(snap.Throughput.Windows) != 3 {
		t.Fatalf("Windows length: expected 3, got %d", len(snap.Throughput.Windows))
	}
	// reversed from window data: [10, 20, 30] oldest-first → [30, 20, 10] most-recent-first
	expectedCounts := []uint32{30, 20, 10}
	for i, exp := range expectedCounts {
		if snap.Throughput.Windows[i].ProcessedCount != exp {
			t.Errorf("Windows[%d].ProcessedCount: expected %d, got %d", i, exp, snap.Throughput.Windows[i].ProcessedCount)
		}
	}

	// reversed from window data: [1, 0, 2] oldest-first → [2, 0, 1] most-recent-first
	expectedDL := []uint32{2, 0, 1}
	for i, exp := range expectedDL {
		if snap.Throughput.Windows[i].DeadLetterCount != exp {
			t.Errorf("Windows[%d].DeadLetterCount: expected %d, got %d", i, exp, snap.Throughput.Windows[i].DeadLetterCount)
		}
	}

	// sliding window totals
	if snap.Throughput.TotalProcessed != 60 { // 10+20+30
		t.Errorf("Throughput.TotalProcessed: expected 60, got %d", snap.Throughput.TotalProcessed)
	}
	if snap.Throughput.TotalDeadLettered != 3 { // 1+0+2
		t.Errorf("Throughput.TotalDeadLettered: expected 3, got %d", snap.Throughput.TotalDeadLettered)
	}
}

func TestTakeSnapshot_ThroughputTimestamps(t *testing.T) {
	wd := WindowData{
		ThroughputPerBucket: []uint32{100, 200, 300},
		DeadLetterPerBucket: []uint32{0, 0, 0},
		BucketDuration:      time.Second,
		TotalProcessed:      600,
	}

	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot { return ConcurrencySnapshot{} },
		func() []ShardSnapshot { return nil },
		func() PreCommitsSnapshot { return PreCommitsSnapshot{} },
		func() WindowData { return wd },
	)

	snap := r.TakeSnapshot()

	if len(snap.Throughput.Windows) != 3 {
		t.Fatalf("Windows length: expected 3, got %d", len(snap.Throughput.Windows))
	}

	// most recent entry should be first
	for i := 1; i < len(snap.Throughput.Windows); i++ {
		if !snap.Throughput.Windows[i].FromTime.Before(snap.Throughput.Windows[i-1].FromTime) {
			t.Errorf("Windows[%d].FromTime (%v) should be before Windows[%d].FromTime (%v)",
				i, snap.Throughput.Windows[i].FromTime, i-1, snap.Throughput.Windows[i-1].FromTime)
		}
	}

	// each entry's toTime should be fromTime + 999ms
	for i, entry := range snap.Throughput.Windows {
		expectedTo := entry.FromTime.Add(999 * time.Millisecond)
		if !entry.ToTime.Equal(expectedTo) {
			t.Errorf("Windows[%d].ToTime: expected %v, got %v", i, expectedTo, entry.ToTime)
		}
	}

	// entries should be contiguous (each fromTime = previous fromTime - bucketDuration)
	for i := 1; i < len(snap.Throughput.Windows); i++ {
		expected := snap.Throughput.Windows[i-1].FromTime.Add(-time.Second)
		if !snap.Throughput.Windows[i].FromTime.Equal(expected) {
			t.Errorf("Windows[%d].FromTime: expected %v, got %v", i, expected, snap.Throughput.Windows[i].FromTime)
		}
	}
}

func TestTakeSnapshot_ComputesAveragesFromTotals(t *testing.T) {
	wd := testWindowData()

	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot { return ConcurrencySnapshot{} },
		func() []ShardSnapshot { return nil },
		func() PreCommitsSnapshot { return PreCommitsSnapshot{} },
		func() WindowData { return wd },
	)

	snap := r.TakeSnapshot()

	// 600_000_000 / 60 = 10_000_000 = 10ms
	if snap.Throughput.AvgProcessDuration != 10*time.Millisecond {
		t.Errorf("AvgProcessDuration: expected 10ms, got %v", snap.Throughput.AvgProcessDuration)
	}
	// 3_000_000_000 / 60 = 50_000_000 = 50ms
	if snap.Throughput.AvgEndToEndDuration != 50*time.Millisecond {
		t.Errorf("AvgEndToEndDuration: expected 50ms, got %v", snap.Throughput.AvgEndToEndDuration)
	}
}

func TestTakeSnapshot_ZeroCount_NoAverages(t *testing.T) {
	wd := WindowData{
		ThroughputPerBucket: []uint32{0, 0},
		DeadLetterPerBucket: []uint32{0, 0},
		BucketDuration:      time.Second,
		TotalCount:          0,
	}

	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot { return ConcurrencySnapshot{} },
		func() []ShardSnapshot { return nil },
		func() PreCommitsSnapshot { return PreCommitsSnapshot{} },
		func() WindowData { return wd },
	)

	snap := r.TakeSnapshot()

	if snap.Throughput.AvgProcessDuration != 0 {
		t.Errorf("AvgProcessDuration with zero count: expected 0, got %v", snap.Throughput.AvgProcessDuration)
	}
	if snap.Throughput.AvgEndToEndDuration != 0 {
		t.Errorf("AvgEndToEndDuration with zero count: expected 0, got %v", snap.Throughput.AvgEndToEndDuration)
	}
}

func TestTakeSnapshot_ConcurrencyFields(t *testing.T) {
	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot {
			return ConcurrencySnapshot{
				GuardActive: 5, GuardCapacity: 100,
				OverflowActive: 3, OverflowCapacity: 50,
			}
		},
		func() []ShardSnapshot { return nil },
		func() PreCommitsSnapshot { return PreCommitsSnapshot{} },
		func() WindowData { return WindowData{ThroughputPerBucket: []uint32{}} },
	)

	snap := r.TakeSnapshot()

	if snap.Concurrency.GuardActive != 5 {
		t.Errorf("GuardActive: expected 5, got %d", snap.Concurrency.GuardActive)
	}
	if snap.Concurrency.GuardCapacity != 100 {
		t.Errorf("GuardCapacity: expected 100, got %d", snap.Concurrency.GuardCapacity)
	}
	if snap.Concurrency.OverflowActive != 3 {
		t.Errorf("OverflowActive: expected 3, got %d", snap.Concurrency.OverflowActive)
	}
	if snap.Concurrency.OverflowCapacity != 50 {
		t.Errorf("OverflowCapacity: expected 50, got %d", snap.Concurrency.OverflowCapacity)
	}
}

func TestTakeSnapshot_SummaryAndPreCommits(t *testing.T) {
	rebalanceTime := time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC)
	preCommits := PreCommitsSnapshot{
		LastRebalanceTime: rebalanceTime,
		Partitions: []PartitionSnapshot{
			{Partition: 0, CommittedOffset: 500, HighestReadyOffset: 503, GapBufferDepth: 2, Assigned: true},
			{Partition: 1, CommittedOffset: 200, HighestReadyOffset: 200, GapBufferDepth: 0, Assigned: false},
			{Partition: 2, CommittedOffset: 750, HighestReadyOffset: 751, GapBufferDepth: 1, Assigned: true},
		},
	}

	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot { return ConcurrencySnapshot{} },
		func() []ShardSnapshot {
			return []ShardSnapshot{
				{Shard: 0, ActiveWorkers: 3, PooledWorkers: 5},
				{Shard: 1, ActiveWorkers: 1, PooledWorkers: 7},
			}
		},
		func() PreCommitsSnapshot { return preCommits },
		func() WindowData { return WindowData{ThroughputPerBucket: []uint32{}, TotalProcessed: 5000} },
	)

	snap := r.TakeSnapshot()

	// summary
	if !snap.Summary.LastRebalanceTime.Equal(rebalanceTime) {
		t.Errorf("Summary.LastRebalanceTime: expected %v, got %v", rebalanceTime, snap.Summary.LastRebalanceTime)
	}
	if snap.Summary.TotalProcessed != 5000 {
		t.Errorf("Summary.TotalProcessed: expected 5000, got %d", snap.Summary.TotalProcessed)
	}
	if snap.Summary.AssignedPartitionCount != 2 {
		t.Errorf("Summary.AssignedPartitionCount: expected 2, got %d", snap.Summary.AssignedPartitionCount)
	}

	// shards
	if len(snap.Shards) != 2 {
		t.Fatalf("Shards: expected 2, got %d", len(snap.Shards))
	}
	if snap.Shards[0].ActiveWorkers != 3 || snap.Shards[0].PooledWorkers != 5 {
		t.Errorf("Shard 0: expected {3,5}, got %+v", snap.Shards[0])
	}

	// preCommits partitions
	if len(snap.PreCommits.Partitions) != 3 {
		t.Fatalf("Partitions: expected 3, got %d", len(snap.PreCommits.Partitions))
	}
	if snap.PreCommits.Partitions[0].Partition != 0 || snap.PreCommits.Partitions[0].GapBufferDepth != 2 || !snap.PreCommits.Partitions[0].Assigned {
		t.Errorf("Partition 0: expected {0,2,true}, got %+v", snap.PreCommits.Partitions[0])
	}
	if snap.PreCommits.Partitions[1].Partition != 1 || snap.PreCommits.Partitions[1].GapBufferDepth != 0 || snap.PreCommits.Partitions[1].Assigned {
		t.Errorf("Partition 1: expected {1,0,false}, got %+v", snap.PreCommits.Partitions[1])
	}
}

func TestTakeSnapshot_SetsTimestamp(t *testing.T) {
	r := NewRecorder(
		"test-topic",
		func() ConcurrencySnapshot { return ConcurrencySnapshot{} },
		func() []ShardSnapshot { return nil },
		func() PreCommitsSnapshot { return PreCommitsSnapshot{} },
		func() WindowData { return WindowData{ThroughputPerBucket: []uint32{}} },
	)

	before := time.Now()
	snap := r.TakeSnapshot()
	after := time.Now()

	if snap.Timestamp.Before(before) || snap.Timestamp.After(after) {
		t.Errorf("Timestamp %v not between %v and %v", snap.Timestamp, before, after)
	}
}

func TestBuildThroughput_SubSecondBucketDuration(t *testing.T) {
	now := time.Date(2025, 6, 15, 14, 32, 7, 0, time.UTC)
	wd := WindowData{
		ThroughputPerBucket: []uint32{10, 20},
		DeadLetterPerBucket: []uint32{1, 0},
		BucketDuration:      500 * time.Millisecond, // sub-second, clamped to 1s internally
	}

	entries := buildThroughput(now, wd)

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// data reversed: [10,20] oldest-first → [20,10] most-recent-first
	if entries[0].ProcessedCount != 20 {
		t.Errorf("entries[0].ProcessedCount: expected 20, got %d", entries[0].ProcessedCount)
	}
	if entries[0].DeadLetterCount != 0 {
		t.Errorf("entries[0].DeadLetterCount: expected 0, got %d", entries[0].DeadLetterCount)
	}
	if entries[1].ProcessedCount != 10 {
		t.Errorf("entries[1].ProcessedCount: expected 10, got %d", entries[1].ProcessedCount)
	}
	if entries[1].DeadLetterCount != 1 {
		t.Errorf("entries[1].DeadLetterCount: expected 1, got %d", entries[1].DeadLetterCount)
	}

	// most recent entry first
	if !entries[1].FromTime.Before(entries[0].FromTime) {
		t.Errorf("entries should be most-recent-first: entries[1].FromTime (%v) not before entries[0].FromTime (%v)",
			entries[1].FromTime, entries[0].FromTime)
	}
}
