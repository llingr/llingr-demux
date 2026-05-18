// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_CommitterWithMissingOffsets validates that the committer correctly handles
// non-contiguous offsets caused by Kafka control records, log compaction, or
// transactional markers. These create gaps in the offset sequence that must be
// handled transparently.
func Test_CommitterWithMissingOffsets(t *testing.T) {
	tests := []struct {
		name            string
		offsetSequences map[int32][]offsetWithPrev // partition -> [(offset, previousOffset)]
		expectedCommits map[int32][]int64          // partition -> expected committed offsets
		expectedMetrics map[int32][]int64          // partition -> expected metric offsets
		scramble        bool                       // whether to send out of order
	}{
		{
			name: "control records gap - single partition",
			offsetSequences: map[int32][]offsetWithPrev{
				0: {
					{offset: 0, prev: -1}, // first message
					{offset: 1, prev: 0},  // normal
					{offset: 2, prev: 1},  // normal
					// offsets 3-9 are control records, not delivered
					{offset: 10, prev: 2},  // gap: prev is 2, not 9!
					{offset: 11, prev: 10}, // normal after gap
					{offset: 12, prev: 11}, // normal
				},
			},
			expectedCommits: map[int32][]int64{
				0: {12}, // should commit highest (all contiguous)
			},
			expectedMetrics: map[int32][]int64{
				0: {0, 1, 2, 10, 11, 12}, // all messages in order
			},
			scramble: true,
		},
		{
			name: "multiple gaps - transaction boundaries",
			offsetSequences: map[int32][]offsetWithPrev{
				0: {
					{offset: 0, prev: -1},
					{offset: 1, prev: 0},
					// gap: 2-4 are transaction markers
					{offset: 5, prev: 1},
					{offset: 6, prev: 5},
					// gap: 7-9 are aborted transaction
					{offset: 10, prev: 6},
					{offset: 11, prev: 10},
					// gap: 12-19 are control records
					{offset: 20, prev: 11},
					{offset: 21, prev: 20},
				},
			},
			expectedCommits: map[int32][]int64{
				0: {21}, // all logically contiguous
			},
			expectedMetrics: map[int32][]int64{
				0: {0, 1, 5, 6, 10, 11, 20, 21},
			},
			scramble: true,
		},
		{
			name: "log compaction gaps - multiple partitions",
			offsetSequences: map[int32][]offsetWithPrev{
				0: {
					{offset: 0, prev: -1},
					{offset: 1, prev: 0},
					// gap: 2-99 compacted away
					{offset: 100, prev: 1},
					{offset: 101, prev: 100},
				},
				1: {
					{offset: 0, prev: -1},
					// gap: 1-49 compacted
					{offset: 50, prev: 0},
					// gap: 51-74 compacted
					{offset: 75, prev: 50},
					{offset: 76, prev: 75},
				},
				2: {
					// starts at high offset due to retention
					{offset: 1000, prev: -1}, // first for this consumer
					{offset: 1001, prev: 1000},
					// gap: 1002-1009 control records
					{offset: 1010, prev: 1001},
				},
			},
			expectedCommits: map[int32][]int64{
				0: {101},
				1: {76},
				2: {1010},
			},
			expectedMetrics: map[int32][]int64{
				0: {0, 1, 100, 101},
				1: {0, 50, 75, 76},
				2: {1000, 1001, 1010},
			},
			scramble: true,
		},
		{
			name: "extreme gap - almost all messages compacted",
			offsetSequences: map[int32][]offsetWithPrev{
				0: {
					{offset: 0, prev: -1},
					// gap: 1-999998 all compacted/control records!
					{offset: 999999, prev: 0},
				},
			},
			expectedCommits: map[int32][]int64{
				0: {999999}, // should handle extreme gaps
			},
			expectedMetrics: map[int32][]int64{
				0: {0, 999999},
			},
			scramble: false, // order doesn't matter with 2 messages
		},
		{
			name: "mixed gaps and normal sequences",
			offsetSequences: map[int32][]offsetWithPrev{
				0: {
					{offset: 0, prev: -1},
					{offset: 1, prev: 0},
					{offset: 2, prev: 1},
					{offset: 3, prev: 2},
					// gap: 4-9
					{offset: 10, prev: 3},
					{offset: 11, prev: 10},
					// gap: 12-14
					{offset: 15, prev: 11},
					{offset: 16, prev: 15},
					{offset: 17, prev: 16},
					{offset: 18, prev: 17},
					{offset: 19, prev: 18},
					{offset: 20, prev: 19},
					// gap: 21-29
					{offset: 30, prev: 20},
				},
			},
			expectedCommits: map[int32][]int64{
				0: {30}, // all logically contiguous despite gaps
			},
			expectedMetrics: map[int32][]int64{
				0: {0, 1, 2, 3, 10, 11, 15, 16, 17, 18, 19, 20, 30},
			},
			scramble: true,
		},
		{
			name: "gap at start after rebalance",
			offsetSequences: map[int32][]offsetWithPrev{
				0: {
					// Consumer starts at offset 100 after rebalance
					// (offsets 0-99 already committed by previous consumer)
					{offset: 100, prev: -1}, // first=true
					{offset: 101, prev: 100},
					// gap: 102-109 control records
					{offset: 110, prev: 101},
					{offset: 111, prev: 110},
				},
			},
			expectedCommits: map[int32][]int64{
				0: {111},
			},
			expectedMetrics: map[int32][]int64{
				0: {100, 101, 110, 111},
			},
			scramble: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := nexus.NewDefaultLogger(slog.LevelInfo)

			cfg := config.DemuxConfig{
				AutoCommitInterval:      250 * time.Millisecond,
				CommitPartitionSliceLen: 100, // sufficient for our tests
			}
			cfg.SetDemuxConfigDefaults()

			pool := alloc.NewWorkItemsPool[string](cfg)

			// Capture metrics
			mc := newMetricsCapture()

			// Capture commits
			var commitMu sync.Mutex
			committedByPartition := make(map[int32][]int64)

			commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
				commitMu.Lock()
				defer commitMu.Unlock()

				for _, msg := range messages {
					// Only record if not already present (avoid duplicates in test)
					offsets := committedByPartition[msg.Partition]
					if len(offsets) == 0 || offsets[len(offsets)-1] != msg.Offset {
						committedByPartition[msg.Partition] = append(offsets, msg.Offset)
					}
				}
				return messages, nil
			}

			metricsCollector := metrics.NewCollector[string](ctx, cfg, mc.Sink, nexus.SinkContext{}, pool, logger)
			metricsCollector.StartCollectingMetrics()

			committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

			// mark all partitions as assigned (required for commit guard)
			for partition := range tt.offsetSequences {
				committer.MarkPartitionAssigned(partition)
			}

			// Prepare work items - calculate total for preallocation
			totalMessages := 0
			for _, sequences := range tt.offsetSequences {
				totalMessages += len(sequences)
			}
			workItems := make([]*ports.WorkItem[string], 0, totalMessages)

			for partition, sequences := range tt.offsetSequences {
				for _, seq := range sequences {
					workItem := pool.Borrow()
					workItem.Message.Partition = partition
					workItem.Message.Offset = seq.offset
					workItem.Metrics.Partition = partition
					workItem.Metrics.Offset = seq.offset

					if seq.prev == -1 {
						workItem.First = true
						// PreviousOffset is not set for first message
					} else {
						workItem.PreviousOffset = seq.prev
					}

					workItems = append(workItems, workItem)
				}
			}

			// Optionally scramble to test out-of-order handling
			if tt.scramble {
				// Simple scramble: reverse the order
				for i := 0; i < len(workItems)/2; i++ {
					j := len(workItems) - 1 - i
					workItems[i], workItems[j] = workItems[j], workItems[i]
				}
			}

			// Submit all work items
			for _, workItem := range workItems {
				committer.CollectAndCommit(workItem)
				time.Sleep(100 * time.Microsecond) // tiny delay to simulate realistic arrival
			}

			// Wait for all metrics to be collected
			deadline := time.Now().Add(10 * time.Second)
			for mc.Count() != int64(totalMessages) {
				if time.Now().After(deadline) {
					t.Fatalf("timeout waiting for metrics: got %d/%d",
						mc.Count(), totalMessages)
				}
				time.Sleep(10 * time.Millisecond)
			}

			// Allow final commit to happen
			time.Sleep(500 * time.Millisecond)

			// Verify metrics (watermark advancement)
			metricsCollected := mc.ByPartition()
			for partition, expected := range tt.expectedMetrics {
				actual := metricsCollected[partition]
				if !offsetSlicesEqual(expected, actual) {
					t.Errorf("partition %d metrics mismatch\nexpected: %v\nactual:   %v",
						partition, expected, actual)
				}
			}

			// Verify commits
			commitMu.Lock()
			for partition, expected := range tt.expectedCommits {
				actual := committedByPartition[partition]
				if len(actual) == 0 {
					t.Errorf("partition %d: no commits found", partition)
					continue
				}

				// Check highest committed offset
				highestCommit := actual[len(actual)-1]
				expectedHighest := expected[len(expected)-1]
				if highestCommit != expectedHighest {
					t.Errorf("partition %d: expected highest commit %d, got %d\nall commits: %v",
						partition, expectedHighest, highestCommit, actual)
				}

				// Verify commits are always ascending
				for i := 1; i < len(actual); i++ {
					if actual[i] <= actual[i-1] {
						t.Errorf("partition %d: commits not ascending at index %d: %d -> %d",
							partition, i, actual[i-1], actual[i])
					}
				}
			}
			commitMu.Unlock()

			// Verify we handled all partitions
			if len(metricsCollected) != len(tt.expectedMetrics) {
				t.Errorf("expected metrics for %d partitions, got %d",
					len(tt.expectedMetrics), len(metricsCollected))
			}
		})
	}
}

// offsetWithPrev represents an offset and its logical previous offset
type offsetWithPrev struct {
	offset int64
	prev   int64 // -1 for first message
}

// offsetSlicesEqual compares two slices of offsets
func offsetSlicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
