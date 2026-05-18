// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_CommitterHandlesDuplicates verifies that the committer handles duplicate
// offsets correctly (the "should never happen" case) by logging an error but
// continuing to process without deadlocking.
func Test_CommitterHandlesDuplicates(t *testing.T) {
	tests := []struct {
		name            string
		messages        []duplicateTestMessage
		expectedCommit  int64
		expectedMetrics []int64
		expectErrorLog  bool
	}{
		{
			name: "duplicate offset mid-stream",
			messages: []duplicateTestMessage{
				firstMsg(0), msg(1), msg(2), msg(3),
				msg(3), // DUPLICATE!
				msg(4), msg(5),
			},
			expectedCommit:  5,
			expectedMetrics: []int64{0, 1, 2, 3, 3, 4, 5},
			expectErrorLog:  true,
		},
		{
			name: "multiple duplicates",
			messages: []duplicateTestMessage{
				firstMsg(0),
				msg(1), msg(1), msg(1), // offset 1 three times
				msg(2), msg(2), // offset 2 twice
				msg(3),
			},
			expectedCommit:  3,
			expectedMetrics: []int64{0, 1, 1, 1, 2, 2, 3},
			expectErrorLog:  true,
		},
		{
			name: "duplicate of first message",
			messages: []duplicateTestMessage{
				firstMsg(100), firstMsg(100), // DUPLICATE OF FIRST!
				msg(101), msg(102),
			},
			expectedCommit:  102,
			expectedMetrics: []int64{100, 100, 101, 102},
			expectErrorLog:  true,
		},
		{
			name: "duplicate when still in Ready triggers error",
			messages: []duplicateTestMessage{
				firstMsg(0), msg(1), msg(1), msg(2), // msg(1) duplicated
			},
			expectedCommit:  2,
			expectedMetrics: []int64{0, 1, 1, 2},
			expectErrorLog:  true,
		},
		{
			name: "duplicates in gap buffer silently deduplicated",
			messages: []duplicateTestMessage{
				firstMsg(0), msg(1),
				gapMsg(3, 2), gapMsg(3, 2), // duplicate in gap buffer
				gapMsg(4, 3), // another gap
				msg(2),       // fills gap, triggers sort
			},
			expectedCommit:  4,
			expectedMetrics: []int64{0, 1, 2, 3, 4}, // duplicate silently removed
			expectErrorLog:  false,
		},
		{
			name: "no duplicates - normal flow",
			messages: []duplicateTestMessage{
				firstMsg(0), msg(1), msg(2), msg(3),
			},
			expectedCommit:  3,
			expectedMetrics: []int64{0, 1, 2, 3},
			expectErrorLog:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Capture logs to verify error message
			var logBuffer bytes.Buffer
			logger := &testDuplicateLogger{buffer: &logBuffer}

			cfg := config.DemuxConfig{
				AutoCommitInterval:      250 * time.Millisecond,
				CommitPartitionSliceLen: 100,
			}
			cfg.SetDemuxConfigDefaults()

			pool := alloc.NewWorkItemsPool[string](cfg)

			// Capture metrics
			metricsMu := &sync.Mutex{}
			var metricsCollected []int64
			metricsCount := &atomic.Int64{}

			metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
				metricsMu.Lock()
				defer metricsMu.Unlock()
				metricsCollected = append(metricsCollected, m.Offset)
				metricsCount.Add(1)
				return nil
			}

			// Capture commits
			var commitMu sync.Mutex
			var lastCommit int64 = -1

			commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
				commitMu.Lock()
				defer commitMu.Unlock()
				for _, msg := range messages {
					if msg.Offset > lastCommit {
						lastCommit = msg.Offset
					}
				}
				return messages, nil
			}

			metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
			metricsCollector.StartCollectingMetrics()

			committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
			committer.MarkPartitionAssigned(0) // required for commit guard

			// Send all messages including duplicates
			for _, msg := range tt.messages {
				workItem := pool.Borrow()
				workItem.Message.Partition = msg.partition
				workItem.Message.Offset = msg.offset
				workItem.Metrics.Partition = msg.partition
				workItem.Metrics.Offset = msg.offset

				if msg.isFirst {
					workItem.First = true
				} else {
					workItem.PreviousOffset = msg.prev
				}

				committer.CollectAndCommit(workItem)
				time.Sleep(50 * time.Microsecond) // tiny delay to ensure order
			}

			// Wait for all metrics
			deadline := time.Now().Add(5 * time.Second)
			for metricsCount.Load() != int64(len(tt.expectedMetrics)) {
				if time.Now().After(deadline) {
					t.Fatalf("timeout waiting for metrics: got %d/%d",
						metricsCount.Load(), len(tt.expectedMetrics))
				}
				time.Sleep(10 * time.Millisecond)
			}

			// Allow final commit
			time.Sleep(500 * time.Millisecond)

			// Verify we didn't deadlock and processed everything
			metricsMu.Lock()
			if len(metricsCollected) != len(tt.expectedMetrics) {
				t.Errorf("expected %d metrics, got %d\nmetrics: %v",
					len(tt.expectedMetrics), len(metricsCollected), metricsCollected)
			}
			for i, expected := range tt.expectedMetrics {
				if i >= len(metricsCollected) {
					break
				}
				if metricsCollected[i] != expected {
					t.Errorf("metric[%d]: expected offset %d, got %d",
						i, expected, metricsCollected[i])
				}
			}
			metricsMu.Unlock()

			// Verify final commit
			commitMu.Lock()
			if lastCommit != tt.expectedCommit {
				t.Errorf("expected final commit %d, got %d", tt.expectedCommit, lastCommit)
			}
			commitMu.Unlock()

			// Verify error log for duplicates
			logOutput := logBuffer.String()
			hasErrorLog := strings.Contains(logOutput, "duplicate 'should never happen'")

			if tt.expectErrorLog && !hasErrorLog {
				t.Errorf("expected error log for duplicate, but not found in:\n%s", logOutput)
			}
			if !tt.expectErrorLog && hasErrorLog {
				t.Errorf("unexpected error log for duplicate found:\n%s", logOutput)
			}

			// Verify specific duplicate detection
			if tt.expectErrorLog {
				// Count how many duplicate errors we logged
				duplicateCount := strings.Count(logOutput, "duplicate 'should never happen'")
				// Count actual duplicates in test data
				seen := make(map[int64]bool)
				actualDuplicates := 0
				for _, msg := range tt.messages {
					if msg.partition == 0 {
						if seen[msg.offset] {
							actualDuplicates++
						}
						seen[msg.offset] = true
					}
				}
				if duplicateCount != actualDuplicates {
					t.Errorf("expected %d duplicate error logs, found %d",
						actualDuplicates, duplicateCount)
				}
			}
		})
	}
}

// Test_CommitterDuplicateDoesNotDeadlock ensures that even with many duplicates,
// the system continues processing and doesn't deadlock.
func Test_CommitterDuplicateDoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelError) // only errors

	cfg := config.DemuxConfig{
		AutoCommitInterval:      250 * time.Millisecond, // minimum allowed
		CommitPartitionSliceLen: 100,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// Track progress
	processedCount := &atomic.Int64{}
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		processedCount.Add(1)
		return nil
	}

	commitCount := &atomic.Int64{}
	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitCount.Add(1)
		return messages, nil
	}

	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	committer.MarkPartitionAssigned(0) // required for commit guard

	// Send a pathological sequence: every offset twice!
	totalMessages := 0
	for i := int64(0); i < 100; i++ {
		for duplicate := 0; duplicate < 2; duplicate++ {
			workItem := pool.Borrow()
			workItem.Message.Partition = 0
			workItem.Message.Offset = i
			workItem.Metrics.Partition = 0
			workItem.Metrics.Offset = i

			if i == 0 {
				workItem.First = true
			} else {
				workItem.PreviousOffset = i - 1
			}

			committer.CollectAndCommit(workItem)
			totalMessages++
		}
	}

	// Verify it doesn't deadlock - should process all within reasonable time
	deadline := time.Now().Add(3 * time.Second)
	for processedCount.Load() != int64(totalMessages) {
		if time.Now().After(deadline) {
			t.Fatalf("DEADLOCK DETECTED: only processed %d/%d messages in 3 seconds",
				processedCount.Load(), totalMessages)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify commits happened
	if commitCount.Load() == 0 {
		t.Errorf("no commits occurred - system might be stuck")
	}

	t.Logf("Successfully processed %d messages (50%% duplicates) with %d commits",
		processedCount.Load(), commitCount.Load())
}

type duplicateTestMessage struct {
	partition int32
	offset    int64
	prev      int64
	isFirst   bool
}

// msg creates a regular message with prev = offset - 1.
func msg(offset int64) duplicateTestMessage {
	return duplicateTestMessage{partition: 0, offset: offset, prev: offset - 1}
}

// firstMsg creates a first message (isFirst=true, prev=-1).
func firstMsg(offset int64) duplicateTestMessage {
	return duplicateTestMessage{partition: 0, offset: offset, prev: -1, isFirst: true}
}

// gapMsg creates a message that will be gap-buffered (prev doesn't match expected).
func gapMsg(offset, prev int64) duplicateTestMessage {
	return duplicateTestMessage{partition: 0, offset: offset, prev: prev}
}

// testDuplicateLogger is a test logger that captures output
type testDuplicateLogger struct {
	buffer *bytes.Buffer
	mu     sync.Mutex
}

func (l *testDuplicateLogger) Error(_ context.Context, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(args) > 0 {
		format = fmt.Sprintf(format, args...)
	}
	l.buffer.WriteString(format)
	l.buffer.WriteString("\n")
}

func (l *testDuplicateLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (l *testDuplicateLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (l *testDuplicateLogger) Warn(_ context.Context, _ string, _ ...any)  {}
