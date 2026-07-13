// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package snapshot

import "time"

// Recorder assembles point-in-time snapshots from subsystem samplers.
//
// It holds no data and no mutex - each sampler function is a closure
// wired during Build() that captures the relevant component and acquires
// whatever lock it needs internally.
type Recorder struct {
	topicName   string
	concurrency func() ConcurrencySnapshot
	shardInfo   func() []ShardSnapshot
	preCommits  func() PreCommitsSnapshot
	windowData  func() WindowData
}

// NewRecorder creates a Recorder with point-in-time sampler functions.
// All function parameters are closures created during Build() that capture
// the relevant channels and components.
func NewRecorder(
	topicName string,
	concurrency func() ConcurrencySnapshot,
	shardInfo func() []ShardSnapshot,
	preCommits func() PreCommitsSnapshot,
	windowData func() WindowData,
) *Recorder {
	return &Recorder{
		topicName:   topicName,
		concurrency: concurrency,
		shardInfo:   shardInfo,
		preCommits:  preCommits,
		windowData:  windowData,
	}
}

// TakeSnapshot returns a point-in-time view of engine state.
// Safe to call from any goroutine.
//
// Each sampler acquires its own lock internally. The division for
// average latency is computed here - outside any mutex.
func (r *Recorder) TakeSnapshot() Snapshot {
	now := time.Now()

	wd := r.windowData()
	windows := buildThroughput(now, wd)
	preCommits := r.preCommits()

	// compute sliding window totals
	var windowProcessed, windowDeadLettered int64
	for _, w := range windows {
		windowProcessed += int64(w.ProcessedCount)
		windowDeadLettered += int64(w.DeadLetterCount)
	}

	// compute assigned partition count
	assignedCount := 0
	for _, p := range preCommits.Partitions {
		if p.Assigned {
			assignedCount++
		}
	}

	snap := Snapshot{
		Timestamp: now,
		Summary: SummarySnapshot{
			TopicName:              r.topicName,
			AssignedPartitionCount: assignedCount,
			TotalProcessed:         wd.TotalProcessed,
			LastRebalanceTime:      preCommits.LastRebalanceTime,
		},
		Concurrency: r.concurrency(),
		Shards:      r.shardInfo(),
		Throughput: ThroughputSnapshot{
			Windows:             windows,
			TotalProcessed:      windowProcessed,
			TotalDeadLettered:   windowDeadLettered,
			MaxProcessDuration:  time.Duration(wd.MaxProcessNanos),
			MaxEndToEndDuration: time.Duration(wd.MaxEndToEndNanos),
			MinEndToEndDuration: time.Duration(wd.MinEndToEndNanos),
		},
		PreCommits: preCommits,
	}

	if wd.TotalCount > 0 {
		snap.Throughput.AvgProcessDuration = time.Duration(wd.TotalProcessNanos / wd.TotalCount)
		snap.Throughput.AvgEndToEndDuration = time.Duration(wd.TotalEndToEndNanos / wd.TotalCount)
	}

	return snap
}

// buildThroughput converts raw per-bucket counts (oldest first) into
// timestamped ThroughputEntry slices sorted most recent first.
func buildThroughput(now time.Time, wd WindowData) []ThroughputEntry {
	bucketCount := len(wd.ThroughputPerBucket)
	if bucketCount == 0 {
		return nil
	}

	bucketSecs := int64(wd.BucketDuration / time.Second)
	if bucketSecs < 1 {
		bucketSecs = 1
	}

	// most recent bucket's start time: truncate now to bucket boundary
	currentEpoch := now.UTC().Unix() / bucketSecs
	currentFrom := time.Unix(currentEpoch*bucketSecs, 0).UTC()

	entries := make([]ThroughputEntry, bucketCount)

	// wd.ThroughputPerBucket is oldest first (index 0 = oldest)
	// reverse into entries so index 0 = most recent
	for i := 0; i < bucketCount; i++ {
		fromTime := currentFrom.Add(-time.Duration(i) * wd.BucketDuration)
		srcIdx := bucketCount - 1 - i
		entries[i] = ThroughputEntry{
			FromTime:        fromTime,
			ToTime:          fromTime.Add(wd.BucketDuration - time.Millisecond),
			ProcessedCount:  wd.ThroughputPerBucket[srcIdx],
			DeadLetterCount: wd.DeadLetterPerBucket[srcIdx],
		}
	}

	return entries
}
