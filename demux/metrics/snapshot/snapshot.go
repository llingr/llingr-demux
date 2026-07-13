// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package snapshot

import (
	"encoding/json"
	"time"
)

const (
	// DefaultBucketCount is the default number of buckets in the sliding window.
	DefaultBucketCount = 15

	// DefaultBucketDuration is the default time span each bucket covers.
	DefaultBucketDuration = time.Second
)

const timestampFormat = "2006-01-02T15:04:05.000Z"

// Snapshot is a point-in-time view of engine state
type Snapshot struct {
	Summary     SummarySnapshot     `json:"summary"`
	Throughput  ThroughputSnapshot  `json:"throughput"`
	PreCommits  PreCommitsSnapshot  `json:"preCommits"`
	Concurrency ConcurrencySnapshot `json:"concurrency"`
	Shards      []ShardSnapshot     `json:"shards"`
	Timestamp   time.Time           `json:"-"`
}

// SummarySnapshot is the top-level engine summary
type SummarySnapshot struct {
	TopicName              string    `json:"topicName"`
	AssignedPartitionCount int       `json:"assignedPartitionCount"`
	TotalProcessed         int64     `json:"totalProcessed"`
	LastRebalanceTime      time.Time `json:"-"`
}

// MarshalJSON serialises SummarySnapshot with UTC millisecond-precision timestamp.
func (s SummarySnapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		TopicName              string `json:"topicName"`
		AssignedPartitionCount int    `json:"assignedPartitionCount"`
		TotalProcessed         int64  `json:"totalProcessed"`
		LastRebalanceTime      string `json:"lastRebalanceTime"`
	}{
		TopicName:              s.TopicName,
		AssignedPartitionCount: s.AssignedPartitionCount,
		TotalProcessed:         s.TotalProcessed,
		LastRebalanceTime:      s.LastRebalanceTime.UTC().Format(timestampFormat),
	})
}

// ConcurrencySnapshot is the guard channel utilisation
type ConcurrencySnapshot struct {
	GuardActive        int `json:"guardActive"`
	GuardCapacity      int `json:"guardCapacity"`
	OverflowActive     int `json:"overflowActive"`
	OverflowCapacity   int `json:"overflowCapacity"`
	CommitIngestActive int `json:"commitIngestActive"`
	CommitIngestCap    int `json:"commitIngestCapacity"`
}

// ThroughputSnapshot is the sliding window throughput and latency state
type ThroughputSnapshot struct {
	Windows             []ThroughputEntry `json:"windows"`
	TotalProcessed      int64             `json:"totalProcessed"`
	TotalDeadLettered   int64             `json:"totalDeadLettered"`
	AvgProcessDuration  time.Duration     `json:"-"`
	MaxProcessDuration  time.Duration     `json:"-"`
	AvgEndToEndDuration time.Duration     `json:"-"`
	MaxEndToEndDuration time.Duration     `json:"-"`
	MinEndToEndDuration time.Duration     `json:"-"`
}

// MarshalJSON serialises ThroughputSnapshot with human-readable duration strings.
func (t ThroughputSnapshot) MarshalJSON() ([]byte, error) {
	type throughputAlias ThroughputSnapshot
	return json.Marshal(struct {
		throughputAlias
		AvgProcessDuration  string `json:"avgProcessDuration"`
		MaxProcessDuration  string `json:"maxProcessDuration"`
		AvgEndToEndDuration string `json:"avgEndToEndDuration"`
		MaxEndToEndDuration string `json:"maxEndToEndDuration"`
		MinEndToEndDuration string `json:"minEndToEndDuration"`
	}{
		throughputAlias:     throughputAlias(t),
		AvgProcessDuration:  t.AvgProcessDuration.String(),
		MaxProcessDuration:  t.MaxProcessDuration.String(),
		AvgEndToEndDuration: t.AvgEndToEndDuration.String(),
		MaxEndToEndDuration: t.MaxEndToEndDuration.String(),
		MinEndToEndDuration: t.MinEndToEndDuration.String(),
	})
}

// ThroughputEntry is the message count for a time window
type ThroughputEntry struct {
	FromTime        time.Time `json:"-"`
	ToTime          time.Time `json:"-"`
	ProcessedCount  uint32    `json:"processedCount"`
	DeadLetterCount uint32    `json:"deadLetterCount"`
}

// MarshalJSON serialises ThroughputEntry with UTC millisecond-precision timestamps.
func (te ThroughputEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		FromTime        string `json:"fromTime"`
		ToTime          string `json:"toTime"`
		ProcessedCount  uint32 `json:"processedCount"`
		DeadLetterCount uint32 `json:"deadLetterCount"`
	}{
		FromTime:        te.FromTime.UTC().Format(timestampFormat),
		ToTime:          te.ToTime.UTC().Format(timestampFormat),
		ProcessedCount:  te.ProcessedCount,
		DeadLetterCount: te.DeadLetterCount,
	})
}

// PreCommitsSnapshot is the per-partition offset tracking state
type PreCommitsSnapshot struct {
	LastRebalanceTime time.Time           `json:"-"`
	Partitions        []PartitionSnapshot `json:"partitions"`
}

// ShardSnapshot is the worker state for a single shard
type ShardSnapshot struct {
	Shard         int `json:"shard"`
	ActiveWorkers int `json:"activeWorkers"`
	PooledWorkers int `json:"pooledWorkers"`
}

// PartitionSnapshot is the offset tracking state for a single partition
type PartitionSnapshot struct {
	Partition          int32 `json:"partition"`
	CommittedOffset    int64 `json:"committedOffset"`
	HighestReadyOffset int64 `json:"highestReadyOffset"`
	MaxOffsetSeen      int64 `json:"maxOffsetSeen"`
	GapBufferDepth     int   `json:"gapBufferDepth"`
	Assigned           bool  `json:"assigned"`
}

// WindowData holds raw ring buffer statistics copied under the committer's mutex.
// Averages are computed by the caller (outside the lock) to avoid holding the
// mutex during integer division.
type WindowData struct {
	ThroughputPerBucket []uint32
	DeadLetterPerBucket []uint32
	BucketDuration      time.Duration
	TotalProcessed      int64
	TotalCount          int64
	TotalProcessNanos   int64
	TotalEndToEndNanos  int64
	MaxProcessNanos     int64
	MaxEndToEndNanos    int64
	MinEndToEndNanos    int64
}
