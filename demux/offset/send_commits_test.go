// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_StartAsyncCommits_LogsCommitError verifies that errors from CommitOffsets
// are logged (send_commits.go:21-23)
func Test_StartAsyncCommits_LogsCommitError(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond, // short timeout for testing
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func([]*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return nil, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// Block the autoCommitGuard to force CommitOffsets to timeout
	committer.autoCommitGuard <- struct{}{}

	// Wait for async commit cycle to attempt and timeout (250ms interval + 100ms for timeout)
	time.Sleep(400 * time.Millisecond)

	if !logger.HasErrors() {
		t.Fatal("expected error to be logged, but no error was logged")
	}

	if !logger.ContainsError("failed to commit offset(s)") {
		t.Errorf("expected log to contain 'failed to commit offset(s)', got: %v", logger.Errors())
	}
}

// Test_AsyncCommit_CommitOffsetsFails verifies that when commitOffsets returns
// an error, it is logged AND surfaced to the caller as ErrBrokerCommitFailed.
// (Previously the error was swallowed and CommitOffsets returned nil, which
// made a failed final commit before a partition handoff invisible to the
// drain; see the reset-to-zero zombie regression tests.)
func Test_AsyncCommit_CommitOffsetsFails(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	// commitOffsets returns an error
	commitError := fmt.Errorf("broker connection failed")
	commitOffsets := func([]*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return nil, commitError
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// manually add a Ready item to partition 0
	workItem := pool.Borrow()
	workItem.Message.Partition = 0
	workItem.Message.Offset = 100
	workItem.Message.Key = "test-key"

	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		Ready:            workItem,
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker
	committer.MarkPartitionAssigned(0) // required for commit guard

	// call CommitOffsets directly
	err := committer.CommitOffsets()

	// CommitOffsets must surface the broker failure to the caller
	if !errors.Is(err, ErrBrokerCommitFailed) {
		t.Errorf("CommitOffsets() returned %v, want ErrBrokerCommitFailed", err)
	}
	if err == nil || !strings.Contains(err.Error(), commitError.Error()) {
		t.Errorf("expected the broker error to be carried, got: %v", err)
	}

	// verify error was logged
	if logger.ErrorCount() != 1 {
		t.Errorf("expected 1 log call, got %d", logger.ErrorCount())
	}
	if !logger.ContainsError("failed to commit offset(s)") {
		t.Errorf("expected log to contain 'failed to commit offset(s)', got: %v", logger.Errors())
	}
	if !logger.ContainsError(commitError.Error()) {
		t.Errorf("expected log to contain error message %q, got: %v", commitError.Error(), logger.Errors())
	}

	// verify Ready item is still there (not cleared because commit failed)
	if tracker.Ready == nil {
		t.Error("Ready item was cleared even though commit failed")
	}
}

// Test_AsyncCommit_Success verifies that when commitOffsets succeeds,
// metrics are collected and Ready is cleared (send_commits.go:56-61)
func Test_AsyncCommit_Success(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// track metrics collection (protected by mutex for thread safety)
	var metricsCollected []nexus.Metrics
	var metricsMu sync.Mutex
	metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
		metricsMu.Lock()
		defer metricsMu.Unlock()
		metricsCollected = append(metricsCollected, m)
		return nil
	}
	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()
	defer metricsCollector.Stop()

	// commitOffsets succeeds
	var committedMessages []*nexus.Message[string]
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		committedMessages = msgs
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// manually add Ready items to multiple partitions
	workItem1 := pool.Borrow()
	workItem1.Message.Partition = 0
	workItem1.Message.Offset = 100
	workItem1.Message.Key = "key-0"

	workItem2 := pool.Borrow()
	workItem2.Message.Partition = 1
	workItem2.Message.Offset = 200
	workItem2.Message.Key = "key-1"

	tracker0 := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		Ready:            workItem1,
	}
	tracker1 := &OffsetsTracker[string]{
		CommittedPlusOne: 200,
		Ready:            workItem2,
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker0
	committer.offsetsByPartition.PartitionMap[1] = tracker1
	committer.MarkPartitionAssigned(0) // required for commit guard
	committer.MarkPartitionAssigned(1) // required for commit guard

	// call CommitOffsets directly
	err := committer.CommitOffsets()

	// should succeed
	if err != nil {
		t.Errorf("CommitOffsets() returned error %v, want nil", err)
	}

	// allow time for async metrics processing
	time.Sleep(50 * time.Millisecond)

	// verify commitOffsets was called with both messages
	if len(committedMessages) != 2 {
		t.Errorf("expected 2 messages committed, got %d", len(committedMessages))
	}

	// verify CommittedPlusOne was updated for both partitions
	if tracker0.CommittedPlusOne != 101 { // offset 100 + 1
		t.Errorf("tracker0.CommittedPlusOne = %d, want 101", tracker0.CommittedPlusOne)
	}
	if tracker1.CommittedPlusOne != 201 { // offset 200 + 1
		t.Errorf("tracker1.CommittedPlusOne = %d, want 201", tracker1.CommittedPlusOne)
	}

	// verify Ready was cleared for both partitions
	if tracker0.Ready != nil {
		t.Error("tracker0.Ready should be nil after successful commit")
	}
	if tracker1.Ready != nil {
		t.Error("tracker1.Ready should be nil after successful commit")
	}

	// verify metrics were collected for both items
	metricsMu.Lock()
	defer metricsMu.Unlock()
	if len(metricsCollected) != 2 {
		t.Errorf("expected 2 metrics collected, got %d", len(metricsCollected))
	}
}

// Test_AsyncCommit_NoReadyCommits verifies that when there are no Ready items,
// CommitOffsets returns nil without attempting to commit (send_commits.go:51-64)
func Test_AsyncCommit_NoReadyCommits(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	// commitOffsets should not be called
	commitCalled := false
	commitOffsets := func([]*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitCalled = true
		return nil, fmt.Errorf("should not be called")
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// add trackers with no Ready items
	tracker0 := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		Ready:            nil, // no ready item
	}
	tracker1 := &OffsetsTracker[string]{
		CommittedPlusOne: 200,
		Ready:            nil, // no ready item
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker0
	committer.offsetsByPartition.PartitionMap[1] = tracker1

	// call CommitOffsets directly
	if err := committer.CommitOffsets(); err != nil {
		t.Errorf("CommitOffsets() returned error %v, want nil", err)
	}

	// verify commitOffsets was not called
	if commitCalled {
		t.Error("commitOffsets should not be called when there are no Ready items")
	}
}

// Test_AsyncCommit_PartialSuccess verifies that when some partitions have
// Ready items and commitOffsets succeeds, only those partitions are affected
func Test_AsyncCommit_PartialSuccess(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// partition 0 has a Ready item
	workItem := pool.Borrow()
	workItem.Message.Partition = 0
	workItem.Message.Offset = 100
	tracker0 := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		Ready:            workItem,
	}

	// partition 1 has no Ready item
	tracker1 := &OffsetsTracker[string]{
		CommittedPlusOne: 200,
		Ready:            nil,
	}

	committer.offsetsByPartition.PartitionMap[0] = tracker0
	committer.offsetsByPartition.PartitionMap[1] = tracker1
	committer.MarkPartitionAssigned(0) // required for commit guard
	committer.MarkPartitionAssigned(1) // required for commit guard

	if err := committer.CommitOffsets(); err != nil {
		t.Errorf("CommitOffsets() returned error %v, want nil", err)
	}

	// verify only tracker0 was updated
	if tracker0.CommittedPlusOne != 101 {
		t.Errorf("tracker0.CommittedPlusOne = %d, want 101", tracker0.CommittedPlusOne)
	}
	if tracker0.Ready != nil {
		t.Error("tracker0.Ready should be nil after commit")
	}

	// verify tracker1 was not affected
	if tracker1.CommittedPlusOne != 200 {
		t.Errorf("tracker1.CommittedPlusOne = %d, want 200 (unchanged)", tracker1.CommittedPlusOne)
	}
	if tracker1.Ready != nil {
		t.Error("tracker1.Ready should remain nil")
	}
}

// Test_AsyncCommit_OrphanedWorkItemAfterRevoke verifies that when a partition is revoked,
// any Ready items for that partition are skipped and cleaned up during CommitOffsets().
// This tests the orphaned work item protection added to close the model-implementation gap
// discovered during TLA+ formal verification (see COMMIT_GUARD_ANALYSIS.md).
//
// Terminology: "orphaned" work items are internally called "zombies" in the TLA+ model and
// design documents - workers that complete after their partition was revoked due to drain timeout.
func Test_AsyncCommit_OrphanedWorkItemAfterRevoke(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// track metrics collection
	var metricsCollected []nexus.Metrics
	var metricsMu sync.Mutex
	metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
		metricsMu.Lock()
		defer metricsMu.Unlock()
		metricsCollected = append(metricsCollected, m)
		return nil
	}
	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()
	defer metricsCollector.Stop()

	// commitOffsets should NOT be called for revoked partition
	commitCalled := false
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitCalled = true
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// simulate: partition 0 was assigned, processed messages, has Ready item
	workItem := pool.Borrow()
	workItem.Message.Partition = 0
	workItem.Message.Offset = 100
	workItem.Message.Key = "orphaned-key"
	workItem.Metrics.Partition = 0
	workItem.Metrics.Offset = 100

	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		Ready:            workItem,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
	}
	// add some items to gap buffer to verify cleanup
	gapItem := pool.Borrow()
	gapItem.Message.Partition = 0
	gapItem.Message.Offset = 102
	tracker.GapBuffer = append(tracker.GapBuffer, gapItem)

	committer.offsetsByPartition.PartitionMap[0] = tracker

	// initially assign the partition
	committer.MarkPartitionAssigned(0)

	// simulate rebalance: partition is revoked
	committer.MarkPartitionRevoked(0)

	// now call CommitOffsets - the orphaned item should be detected and skipped
	err := committer.CommitOffsets()
	if err != nil {
		t.Errorf("CommitOffsets() returned error %v, want nil", err)
	}

	// allow time for async metrics processing
	time.Sleep(50 * time.Millisecond)

	// verify commitOffsets was NOT called (no valid commits)
	if commitCalled {
		t.Error("commitOffsets should not be called for revoked partition")
	}

	// verify Ready was cleaned up
	if tracker.Ready != nil {
		t.Error("Ready should be nil after orphaned item cleanup")
	}

	// verify GapBuffer was cleared
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("GapBuffer should be empty after orphaned item cleanup, got %d items",
			len(tracker.GapBuffer))
	}

	// verify warnings were logged with exact messages: one for the Ready item and
	// one for the gap-buffer leftovers (previously the gap item was dropped
	// silently and never returned through the collector)
	if logger.WarnCount() != 2 {
		t.Errorf("expected 2 warnings (Ready + gap buffer), got %d", logger.WarnCount())
	}
	expectedWarn := "discarding orphaned work item for partition 0 offset 100 (partition no longer assigned)"
	if !logger.HasWarning(expectedWarn) {
		t.Errorf("warning message mismatch\nexpected: %q\ngot:      %v", expectedWarn, logger.Warnings())
	}
	expectedGapWarn := "discarding 1 orphaned gap-buffer item(s) for partition 0 (partition no longer assigned)"
	if !logger.HasWarning(expectedGapWarn) {
		t.Errorf("gap-buffer warning mismatch\nexpected: %q\ngot:      %v", expectedGapWarn, logger.Warnings())
	}

	// verify metrics were collected for BOTH discarded items (they were processed,
	// just not committed): the Ready at 100 and the gap-buffer item at 102
	metricsMu.Lock()
	if len(metricsCollected) != 2 {
		t.Errorf("expected 2 metrics collected for discarded items, got %d", len(metricsCollected))
	}
	collectedOffsets := map[int64]bool{}
	for _, m := range metricsCollected {
		collectedOffsets[m.Offset] = true
	}
	if !collectedOffsets[100] || !collectedOffsets[102] {
		t.Errorf("expected metrics for offsets 100 and 102, got %v", collectedOffsets)
	}
	metricsMu.Unlock()

	// verify CommittedPlusOne was NOT updated (orphaned item didn't advance watermark)
	if tracker.CommittedPlusOne != 100 {
		t.Errorf("CommittedPlusOne should remain 100, got %d", tracker.CommittedPlusOne)
	}
}

// Test_AsyncCommit_MixedAssignedAndRevoked verifies that when some partitions are
// assigned and some are revoked, only the assigned partitions are committed.
func Test_AsyncCommit_MixedAssignedAndRevoked(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	var committedPartitions []int32
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		for _, msg := range msgs {
			committedPartitions = append(committedPartitions, msg.Partition)
		}
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// partition 0: assigned, has Ready
	workItem0 := pool.Borrow()
	workItem0.Message.Partition = 0
	workItem0.Message.Offset = 100
	tracker0 := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		Ready:            workItem0,
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker0
	committer.MarkPartitionAssigned(0)

	// partition 1: revoked (orphaned), has Ready
	workItem1 := pool.Borrow()
	workItem1.Message.Partition = 1
	workItem1.Message.Offset = 200
	tracker1 := &OffsetsTracker[string]{
		CommittedPlusOne: 200,
		Ready:            workItem1,
	}
	committer.offsetsByPartition.PartitionMap[1] = tracker1
	// NOT marking as assigned - simulates revoked partition

	// partition 2: assigned, has Ready
	workItem2 := pool.Borrow()
	workItem2.Message.Partition = 2
	workItem2.Message.Offset = 300
	tracker2 := &OffsetsTracker[string]{
		CommittedPlusOne: 300,
		Ready:            workItem2,
	}
	committer.offsetsByPartition.PartitionMap[2] = tracker2
	committer.MarkPartitionAssigned(2)

	err := committer.CommitOffsets()
	if err != nil {
		t.Errorf("CommitOffsets() returned error %v, want nil", err)
	}

	// verify only partitions 0 and 2 were committed
	if len(committedPartitions) != 2 {
		t.Errorf("expected 2 partitions committed, got %d: %v",
			len(committedPartitions), committedPartitions)
	}

	// verify partition 1 (revoked) was NOT in the commits
	for _, p := range committedPartitions {
		if p == 1 {
			t.Error("partition 1 (revoked) should not have been committed")
		}
	}

	// verify trackers for assigned partitions were updated
	if tracker0.CommittedPlusOne != 101 {
		t.Errorf("tracker0.CommittedPlusOne = %d, want 101", tracker0.CommittedPlusOne)
	}
	if tracker2.CommittedPlusOne != 301 {
		t.Errorf("tracker2.CommittedPlusOne = %d, want 301", tracker2.CommittedPlusOne)
	}

	// verify tracker for revoked partition was cleaned up but NOT advanced
	if tracker1.Ready != nil {
		t.Error("tracker1.Ready should be nil after orphaned item cleanup")
	}
	if tracker1.CommittedPlusOne != 200 {
		t.Errorf("tracker1.CommittedPlusOne should remain 200, got %d", tracker1.CommittedPlusOne)
	}
}
