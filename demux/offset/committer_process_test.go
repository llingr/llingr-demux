// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_CommitterHappyPath validates that scrambled messages across multiple
// partitions are eventually committed in correct order per partition
func Test_CommitterHappyPath(t *testing.T) {
	const (
		partitionCount  = 24
		messagesPerPart = 1000
		totalMessages   = partitionCount * messagesPerPart
	)

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond, // minimum allowed for fast tests
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// capture all metrics (one per message as watermark advances)
	mc := newMetricsCapture()

	// capture committed offsets (sparse - only at commit intervals)
	var commitMu sync.Mutex
	committedByPartition := make(map[int32][]int64)

	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitMu.Lock()
		defer commitMu.Unlock()

		for _, msg := range messages {
			committedByPartition[msg.Partition] = append(
				committedByPartition[msg.Partition],
				msg.Offset,
			)
		}
		return messages, nil
	}
	metricsCollector := metrics.NewCollector[string](ctx, cfg, mc.Sink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// mark all partitions as assigned (required for commit guard)
	for partition := int32(0); partition < partitionCount; partition++ {
		committer.MarkPartitionAssigned(partition)
	}

	// create all work items IN ORDER
	workItems := make([]*ports.WorkItem[string], 0, totalMessages)
	for partition := int32(0); partition < partitionCount; partition++ {
		for offset := int64(0); offset < messagesPerPart; offset++ {
			workItem := pool.Borrow()
			populateWorkItem(workItem, partition, offset)
			workItems = append(workItems, workItem)
		}
	}

	// scramble the work items to test ordering
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // G404: deterministic seed for test reproducibility
	rng.Shuffle(len(workItems), func(i, j int) {
		workItems[i], workItems[j] = workItems[j], workItems[i]
	})

	// send all scrambled work items to committer
	for _, workItem := range workItems {
		committer.CollectAndCommit(workItem)
		time.Sleep(4 * time.Microsecond)
	}

	// await all metrics collection (happens after watermark advances)
	deadline := time.Now().Add(30 * time.Second)
	for mc.Count() != totalMessages {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for metrics: got %d/%d", mc.Count(), totalMessages)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// confirm metrics show complete contiguous ordering per partition
	metricsCollected := mc.ByPartition()
	if len(metricsCollected) != partitionCount {
		t.Errorf("expected %d partitions in metrics, got %d",
			partitionCount, len(metricsCollected))
	}

	for partition := int32(0); partition < partitionCount; partition++ {
		offsets, ok := metricsCollected[partition]
		if !ok {
			t.Errorf("partition %d: no metrics found", partition)
			continue
		}

		if len(offsets) != messagesPerPart {
			t.Errorf("partition %d: expected %d metrics, got %d", partition, messagesPerPart, len(offsets))
			continue
		}

		// confirm offsets are contiguous and in order (0, 1, 2, ... 999)
		for i, offset := range offsets {
			expectedOffset := int64(i)
			if offset != expectedOffset {
				t.Errorf("partition %d: metric[%d] expected offset %d, got %d", partition, i, expectedOffset, offset)
			}
		}
	}

	// confirm commits are always ascending and highest offset is 999 (commits will jump)
	commitMu.Lock()
	for partition := int32(0); partition < partitionCount; partition++ {
		offsets, ok := committedByPartition[partition]
		if !ok {
			t.Errorf("partition %d: no commits found", partition)
			continue
		}

		// commits must always be ascending
		for i := 1; i < len(offsets); i++ {
			if offsets[i] <= offsets[i-1] {
				t.Errorf("partition %d: commit offsets not ascending at index %d: %d -> %d", partition, i, offsets[i-1], offsets[i])
			}
		}

		// highest committed offset is 999 (last message)
		if len(offsets) > 0 {
			highestCommit := offsets[len(offsets)-1]
			if highestCommit != messagesPerPart-1 {
				t.Errorf("partition %d: expected highest commit %d, got %d",
					partition, messagesPerPart-1, highestCommit)
			}
		}
	}
	commitMu.Unlock()
}

// Test_Committer_sortGapBuffer_WithDuplicates confirms duplicate
// offsets in the gap buffer are detected and removed using compact
func Test_Committer_sortGapBuffer_WithDuplicates(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		return nil
	}
	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return messages, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// create gap buffer with duplicates and out-of-order offsets
	offsetsTracker := &OffsetsTracker[string]{
		CommittedPlusOne: 0,
		Assignment:       nexus.Assign,
		MinOffsetSeen:    0,
		MaxOffsetSeen:    100,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 20),
	}

	// add messages with duplicates
	offsets := []int64{
		10,
		10, // duplicate
		20,
		5,
		15,
		20, // duplicate
		3,
	}
	for _, offset := range offsets {
		workItem := pool.Borrow()
		workItem.Message.Partition = 0
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = 0
		workItem.Metrics.Offset = offset
		offsetsTracker.GapBuffer = append(offsetsTracker.GapBuffer, workItem)
	}

	// verify initial state
	if len(offsetsTracker.GapBuffer) != 7 {
		t.Fatalf("expected 7 items in gap buffer before sort, got %d", len(offsetsTracker.GapBuffer))
	}

	// sort includes de-dupe
	committer.sortGapBuffer(offsetsTracker)

	// confirm buffer is sorted and duplicates removed
	expectedOffsets := []int64{3, 5, 10, 15, 20}
	if len(offsetsTracker.GapBuffer) != len(expectedOffsets) {
		t.Fatalf("expected %d items after de-dupe, got %d", len(expectedOffsets), len(offsetsTracker.GapBuffer))
	}

	// confirm no duplicates
	for i, workItem := range offsetsTracker.GapBuffer {
		expectedOffset := expectedOffsets[i]
		actualOffset := workItem.Message.Offset

		if actualOffset != expectedOffset {
			t.Errorf("gap buffer[%d]: expected offset %d, got %d", i, expectedOffset, actualOffset)
		}

		if i < len(offsetsTracker.GapBuffer)-1 {
			nextOffset := offsetsTracker.GapBuffer[i+1].Message.Offset
			if actualOffset == nextOffset {
				t.Errorf("found duplicate at index %d: offset %d appears twice", i, actualOffset)
			}
		}
	}

	// verify strict ascending order
	for i := 1; i < len(offsetsTracker.GapBuffer); i++ {
		prev := offsetsTracker.GapBuffer[i-1].Message.Offset
		curr := offsetsTracker.GapBuffer[i].Message.Offset
		if curr <= prev {
			t.Errorf("gap buffer not sorted: index %d has offset %d, but index %d has offset %d", i-1, prev, i, curr)
		}
	}
}

// Test_Committer_sortGapBuffer_NoDuplicates validates that sorting
// works correctly when there are no duplicates
func Test_Committer_sortGapBuffer_NoDuplicates(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		return nil
	}
	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return messages, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	offsetsTracker := &OffsetsTracker[string]{
		CommittedPlusOne: 0,
		Assignment:       nexus.Assign,
		MinOffsetSeen:    0,
		MaxOffsetSeen:    100,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
	}

	// add messages with unique offsets out of order
	offsets := []int64{50, 10, 30, 20, 40}
	for _, offset := range offsets {
		workItem := pool.Borrow()
		workItem.Message.Partition = 0
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = 0
		workItem.Metrics.Offset = offset
		offsetsTracker.GapBuffer = append(offsetsTracker.GapBuffer, workItem)
	}

	initialLen := len(offsetsTracker.GapBuffer)

	committer.sortGapBuffer(offsetsTracker)

	// verify length unchanged (no duplicates to remove)
	if len(offsetsTracker.GapBuffer) != initialLen {
		t.Errorf("expected length %d after sort, got %d", initialLen, len(offsetsTracker.GapBuffer))
	}

	// verify strict ascending order
	expectedOffsets := []int64{10, 20, 30, 40, 50}
	for i, workItem := range offsetsTracker.GapBuffer {
		if workItem.Message.Offset != expectedOffsets[i] {
			t.Errorf("gap buffer[%d]: expected offset %d, got %d", i, expectedOffsets[i], workItem.Message.Offset)
		}
	}
}

// Test_Committer_CommittedPlusOneAheadOfMessage validates handling of orphaned messages from
// drain timeout scenarios. When drain times out, in-flight workers are abandoned but may
// eventually complete. Meanwhile, the partition is reassigned to another consumer which
// processes and commits further offsets. When rebalance returns the partition here,
// CommittedPlusOne reflects the broker's advanced position. The abandoned worker's stale
// WorkItem arrives with an offset behind CommittedPlusOne - without this check, it would
// incorrectly become Ready and commit backwards, corrupting the offset sequence. The
// CommittedPlusOne > offset check detects this and routes to immediate collection instead.
//
// This test works in concert with the previousOffset contiguity tests to provide complete offset
// integrity guarantees. Test_CommitterWithMissingOffsets validates that advancement only occurs
// when previousOffset links messages as logically contiguous (handling control records, log
// compaction, and transaction markers). Test_CommitterHandlesDuplicates confirms duplicate
// detection via previousOffset comparison. Together, these tests ensure that:
//   - CommittedPlusOne guards against orphaned messages from drain timeouts (this test)
//   - previousOffset guards against non-contiguous advancement (missing offsets tests)
//   - Both mechanisms together prevent any scenario where offset integrity could be corrupted
func Test_Committer_CommittedPlusOneAheadOfMessage(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// track metrics to verify edge case was hit
	var metricsMu sync.Mutex
	metricsCollected := make(map[int32][]int64)

	metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
		metricsMu.Lock()
		defer metricsMu.Unlock()
		metricsCollected[m.Partition] = append(metricsCollected[m.Partition], m.Offset)
		return nil
	}

	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return messages, nil
	}

	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// manually create partition state with CommittedPlusOne = 10
	partition := int32(0)
	committer.mu.Lock()
	committer.offsetsByPartition.PartitionMap[partition] = &OffsetsTracker[string]{
		CommittedPlusOne: 10, // broker committed up to 9, expects 10 next
		Assignment:       nexus.Assign,
		MinOffsetSeen:    0,
		MaxOffsetSeen:    0,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
		Ready:            nil,
	}
	committer.mu.Unlock()

	// send a message with offset 5 (< CommittedPlusOne of 10)
	workItem := pool.Borrow()
	workItem.Message.Partition = partition
	workItem.Message.Offset = 5
	workItem.Metrics.Partition = partition
	workItem.Metrics.Offset = 5
	workItem.First = false

	// process the commit (should hit edge case)
	now := time.Now()
	committer.processCommit(workItem, now)

	// wait for metrics to be collected
	time.Sleep(50 * time.Millisecond)

	// verify the edge case was reached by checking metrics were collected
	metricsMu.Lock()
	offsets, ok := metricsCollected[partition]
	metricsMu.Unlock()

	if !ok {
		t.Fatal("expected metrics to be collected for partition 0")
	}

	if len(offsets) != 1 {
		t.Fatalf("expected 1 metric collected, got %d", len(offsets))
	}

	if offsets[0] != 5 {
		t.Errorf("expected metric offset 5, got %d", offsets[0])
	}

	// verify message was NOT added to gap buffer (immediate collect path)
	committer.mu.Lock()
	offsetsTracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(offsetsTracker.GapBuffer) != 0 {
		committer.mu.Unlock()
		t.Errorf("expected empty gap buffer, got length %d", len(offsetsTracker.GapBuffer))
		return
	}

	// verify Ready is still nil (message didn't become ready)
	if offsetsTracker.Ready != nil {
		committer.mu.Unlock()
		t.Error("expected Ready to remain nil, but it was set")
		return
	}

	// verify CommittedPlusOne unchanged (still 10)
	if offsetsTracker.CommittedPlusOne != 10 {
		committer.mu.Unlock()
		t.Errorf("expected CommittedPlusOne=10, got %d", offsetsTracker.CommittedPlusOne)
		return
	}
	committer.mu.Unlock()
}

// Test_Committer_ResetCommittedOffsets validates that ResetCommittedOffsets correctly
// updates CommittedPlusOne from RebalanceInfo during assign, closing the race window
// where an orphaned message could arrive before the first new message sets CommittedPlusOne.
func Test_Committer_ResetCommittedOffsets(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// simulate: partition was previously processed up to offset 100
	committer.mu.Lock()
	committer.offsetsByPartition.PartitionMap[0] = &OffsetsTracker[string]{
		CommittedPlusOne: 100, // stale value from before rebalance
		Assignment:       nexus.Assign,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
	}
	committer.mu.Unlock()

	// simulate: rebalance returns partition with broker at offset 150
	// (another consumer advanced and committed)
	partitionOffsets := map[int32]int64{
		0: 150, // broker's actual committed position
	}
	committer.ResetCommittedOffsets(partitionOffsets)

	// verify CommittedPlusOne was updated
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[0]
	if tracker.CommittedPlusOne != 150 {
		t.Errorf("expected CommittedPlusOne=150 after reset, got %d", tracker.CommittedPlusOne)
	}
	committer.mu.Unlock()

	// now simulate orphaned message at offset 100 arriving - should be rejected
	var metricsCollected []int64
	var metricsMu sync.Mutex
	metricsSink2 := func(_ nexus.SinkContext, m nexus.Metrics) error {
		metricsMu.Lock()
		defer metricsMu.Unlock()
		metricsCollected = append(metricsCollected, m.Offset)
		return nil
	}
	metricsCollector2 := metrics.NewCollector[string](ctx, cfg, metricsSink2, nexus.SinkContext{}, pool, logger)
	metricsCollector2.StartCollectingMetrics()
	committer.collectMetrics = metricsCollector2.Collect

	orphanedWorkItem := pool.Borrow()
	orphanedWorkItem.Message.Partition = 0
	orphanedWorkItem.Message.Offset = 100 // orphan from before the rebalance
	orphanedWorkItem.Metrics.Partition = 0
	orphanedWorkItem.Metrics.Offset = 100
	orphanedWorkItem.First = false

	now := time.Now()
	committer.processCommit(orphanedWorkItem, now)

	// wait for metrics collection
	time.Sleep(50 * time.Millisecond)

	// verify orphaned WorkItem was collected (metrics captured) but didn't become Ready
	metricsMu.Lock()
	if len(metricsCollected) != 1 || metricsCollected[0] != 100 {
		t.Errorf("expected orphaned WorkItem to be collected with offset 100, got %v", metricsCollected)
	}
	metricsMu.Unlock()

	committer.mu.Lock()
	defer committer.mu.Unlock()

	// verify orphan didn't become Ready
	if tracker.Ready != nil {
		t.Errorf("orphan should not become Ready, but Ready.Offset=%d", tracker.Ready.Message.Offset)
	}

	// verify CommittedPlusOne unchanged (orphan didn't corrupt it)
	if tracker.CommittedPlusOne != 150 {
		t.Errorf("CommittedPlusOne should remain 150, got %d", tracker.CommittedPlusOne)
	}
}

// Test_Committer_ResetCommittedOffsets_NewPartition validates that ResetCommittedOffsets
// creates a new tracker for partitions not previously seen.
func Test_Committer_ResetCommittedOffsets_NewPartition(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// partition 5 doesn't exist yet
	committer.mu.Lock()
	_, exists := committer.offsetsByPartition.PartitionMap[5]
	committer.mu.Unlock()
	if exists {
		t.Fatal("partition 5 should not exist before reset")
	}

	// reset with new partition
	partitionOffsets := map[int32]int64{
		5: 500,
	}
	committer.ResetCommittedOffsets(partitionOffsets)

	// verify tracker was created with correct CommittedPlusOne
	committer.mu.Lock()
	defer committer.mu.Unlock()

	tracker, exists := committer.offsetsByPartition.PartitionMap[5]
	if !exists {
		t.Fatal("partition 5 should exist after reset")
	}
	if tracker.CommittedPlusOne != 500 {
		t.Errorf("expected CommittedPlusOne=500, got %d", tracker.CommittedPlusOne)
	}
	if tracker.Assignment != nexus.Assign {
		t.Errorf("expected Assignment=Assign, got %d", tracker.Assignment)
	}
}

// Test_Committer_ReadyOffsetAheadOfMessage validates the edge case where
// Ready.offset > message offset (line 81-83 in committer_process.go)
func Test_Committer_ReadyOffsetAheadOfMessage(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// track metrics to verify edge case was hit
	var metricsMu sync.Mutex
	metricsCollected := make(map[int32][]int64)

	metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
		metricsMu.Lock()
		defer metricsMu.Unlock()
		metricsCollected[m.Partition] = append(metricsCollected[m.Partition], m.Offset)
		return nil
	}

	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return messages, nil
	}

	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// manually create partition state with Ready at offset 20
	partition := int32(0)
	readyWorkItem := pool.Borrow()
	readyWorkItem.Message.Partition = partition
	readyWorkItem.Message.Offset = 20
	readyWorkItem.Metrics.Partition = partition
	readyWorkItem.Metrics.Offset = 20

	committer.mu.Lock()
	committer.offsetsByPartition.PartitionMap[partition] = &OffsetsTracker[string]{
		CommittedPlusOne: 0,
		Assignment:       nexus.Assign,
		MinOffsetSeen:    0,
		MaxOffsetSeen:    20,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
		Ready:            readyWorkItem, // Ready is at offset 20
	}
	committer.mu.Unlock()

	// send a message with offset 15 (< Ready.offset of 20)
	workItem := pool.Borrow()
	workItem.Message.Partition = partition
	workItem.Message.Offset = 15
	workItem.Metrics.Partition = partition
	workItem.Metrics.Offset = 15
	workItem.First = false

	// process the commit (should hit edge case)
	now := time.Now()
	committer.processCommit(workItem, now)

	// wait for metrics to be collected
	time.Sleep(50 * time.Millisecond)

	// verify the edge case was reached by checking metrics were collected
	metricsMu.Lock()
	offsets, ok := metricsCollected[partition]
	metricsMu.Unlock()

	if !ok {
		t.Fatal("expected metrics to be collected for partition 0")
	}

	if len(offsets) != 1 {
		t.Fatalf("expected 1 metric collected, got %d", len(offsets))
	}

	if offsets[0] != 15 {
		t.Errorf("expected metric offset 15, got %d", offsets[0])
	}

	// verify message was NOT added to gap buffer (immediate collect path)
	committer.mu.Lock()
	offsetsTracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(offsetsTracker.GapBuffer) != 0 {
		committer.mu.Unlock()
		t.Errorf("expected empty gap buffer, got length %d", len(offsetsTracker.GapBuffer))
		return
	}

	// verify Ready unchanged (still at offset 20)
	if offsetsTracker.Ready == nil {
		committer.mu.Unlock()
		t.Fatal("expected Ready to remain set, but it was nil")
	}

	if offsetsTracker.Ready.Message.Offset != 20 {
		committer.mu.Unlock()
		t.Errorf("expected Ready.offset=20, got %d", offsetsTracker.Ready.Message.Offset)
		return
	}
	committer.mu.Unlock()
}

// -----------------------------------------------------------------------------
// TLA+ Formal Verification Invariant Tests
//
// These tests explicitly verify the invariants from the TLA+ specification
// (OffsetCommitterP5r.tla) to ensure implementation matches formal model.
// -----------------------------------------------------------------------------

// Test_Invariant_GapBufferAhead verifies that all offsets in the gap buffer
// are always greater than CommittedPlusOne.
//
// TLA+ invariant: GapBufferAhead ==
//
//	\A p \in DOMAIN offsetsByPartition :
//	  \A i \in 1..Len(offsetsByPartition[p].gapBuffer) :
//	    offsetsByPartition[p].gapBuffer[i] > offsetsByPartition[p].committedPlusOne
//
// This invariant ensures we never buffer offsets that are at or before the commit point.
func Test_Invariant_GapBufferAhead(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	committer.MarkPartitionAssigned(0)

	// set up tracker with CommittedPlusOne = 100
	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
	}
	committer.mu.Lock()
	committer.offsetsByPartition.PartitionMap[0] = tracker
	committer.mu.Unlock()

	// test cases: offsets that should go to gap buffer (all > 100)
	testOffsets := []int64{105, 110, 103, 108, 102, 115}

	now := time.Now()
	for _, offset := range testOffsets {
		workItem := pool.Borrow()
		workItem.Message.Partition = 0
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = 0
		workItem.Metrics.Offset = offset
		workItem.PreviousOffset = offset - 1

		committer.processCommit(workItem, now)
	}

	// verify invariant: all gap buffer offsets > CommittedPlusOne
	committer.mu.Lock()
	defer committer.mu.Unlock()

	for i, wi := range tracker.GapBuffer {
		if wi.Message.Offset <= tracker.CommittedPlusOne {
			t.Errorf("GapBufferAhead violated: gapBuffer[%d].offset=%d <= CommittedPlusOne=%d",
				i, wi.Message.Offset, tracker.CommittedPlusOne)
		}
	}

	// verify gap buffer has expected items (all offsets except those that became Ready)
	if len(tracker.GapBuffer) == 0 {
		// all items may have advanced if they were contiguous
		t.Log("all items advanced through gap buffer (contiguous)")
	} else {
		t.Logf("gap buffer has %d items, all > CommittedPlusOne=%d",
			len(tracker.GapBuffer), tracker.CommittedPlusOne)
	}
}

// Test_Invariant_ReadyMonotonic verifies that Ready.offset is always >= CommittedPlusOne.
//
// TLA+ invariant: ReadyMonotonic ==
//
//	\A p \in DOMAIN offsetsByPartition :
//	  offsetsByPartition[p].ready # NULL =>
//	    offsetsByPartition[p].ready >= offsetsByPartition[p].committedPlusOne
//
// This invariant ensures Ready never regresses behind the commit point.
func Test_Invariant_ReadyMonotonic(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	committer.MarkPartitionAssigned(0)

	// set up tracker with CommittedPlusOne = 50
	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 50,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 10),
	}
	committer.mu.Lock()
	committer.offsetsByPartition.PartitionMap[0] = tracker
	committer.mu.Unlock()

	// send a contiguous sequence starting at CommittedPlusOne
	now := time.Now()
	for offset := int64(50); offset <= 60; offset++ {
		workItem := pool.Borrow()
		workItem.Message.Partition = 0
		workItem.Message.Offset = offset
		workItem.Metrics.Partition = 0
		workItem.Metrics.Offset = offset
		if offset == 50 {
			workItem.First = true
		} else {
			workItem.PreviousOffset = offset - 1
		}

		committer.processCommit(workItem, now)

		// verify invariant after each message
		committer.mu.Lock()
		if tracker.Ready != nil {
			if tracker.Ready.Message.Offset < tracker.CommittedPlusOne {
				t.Errorf("ReadyMonotonic violated: Ready.offset=%d < CommittedPlusOne=%d",
					tracker.Ready.Message.Offset, tracker.CommittedPlusOne)
			}
		}
		committer.mu.Unlock()
	}

	// final verification
	committer.mu.Lock()
	defer committer.mu.Unlock()

	if tracker.Ready == nil {
		t.Fatal("expected Ready to be set after contiguous sequence")
	}

	if tracker.Ready.Message.Offset < tracker.CommittedPlusOne {
		t.Errorf("ReadyMonotonic violated: Ready.offset=%d < CommittedPlusOne=%d",
			tracker.Ready.Message.Offset, tracker.CommittedPlusOne)
	}

	t.Logf("invariant holds: Ready.offset=%d >= CommittedPlusOne=%d",
		tracker.Ready.Message.Offset, tracker.CommittedPlusOne)
}

// Test_Invariant_GapBufferAhead_WithScrambledInput verifies the GapBufferAhead invariant
// holds under scrambled out-of-order message delivery, which is the common case in production.
func Test_Invariant_GapBufferAhead_WithScrambledInput(t *testing.T) {
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)

	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	committer.MarkPartitionAssigned(0)

	// set up tracker with CommittedPlusOne = 0
	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 0,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 100),
	}
	committer.mu.Lock()
	committer.offsetsByPartition.PartitionMap[0] = tracker
	committer.mu.Unlock()

	// create 100 messages
	const messageCount = 100
	workItems := make([]*ports.WorkItem[string], messageCount)
	for i := int64(0); i < messageCount; i++ {
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
		workItems[i] = workItem
	}

	// scramble order
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // G404: deterministic seed for test reproducibility
	rng.Shuffle(len(workItems), func(i, j int) {
		workItems[i], workItems[j] = workItems[j], workItems[i]
	})

	// process all messages, checking invariant after each
	now := time.Now()
	for _, wi := range workItems {
		committer.processCommit(wi, now)

		// verify invariant after each message
		committer.mu.Lock()
		for i, gapWi := range tracker.GapBuffer {
			if gapWi.Message.Offset <= tracker.CommittedPlusOne {
				t.Errorf("GapBufferAhead violated after processing offset %d: "+
					"gapBuffer[%d].offset=%d <= CommittedPlusOne=%d",
					wi.Message.Offset, i, gapWi.Message.Offset, tracker.CommittedPlusOne)
			}
		}
		committer.mu.Unlock()
	}

	// flush deferred gap buffer walks, then verify final state
	committer.mu.Lock()
	defer committer.mu.Unlock()
	committer.flushGapBuffers(now)

	if len(tracker.GapBuffer) != 0 {
		t.Errorf("expected empty gap buffer after all contiguous messages, got %d items",
			len(tracker.GapBuffer))
	}

	if tracker.Ready == nil {
		t.Fatal("expected Ready to be set after all messages")
	}

	t.Logf("invariant holds throughout: Ready.offset=%d, CommittedPlusOne=%d, GapBuffer.len=%d",
		tracker.Ready.Message.Offset, tracker.CommittedPlusOne, len(tracker.GapBuffer))
}

// Test_OrphanedTrait_NotSetOnNormalAdvance verifies that the Orphaned trait is NOT set
// when messages advance normally through the commit pipeline
func Test_OrphanedTrait_NotSetOnNormalAdvance(t *testing.T) {
	ctx := context.Background()
	cfg := config.DemuxConfig{}
	cfg.SetDemuxConfigDefaults()
	pool := alloc.NewWorkItemsPool[string](cfg)

	// track collected metrics with traits
	var collectedItems []nexus.Traits
	collectMetrics := func(wi *ports.WorkItem[string]) {
		collectedItems = append(collectedItems, wi.Metrics.Traits)
		pool.Return(wi)
	}

	committer := &Committer[string]{
		offsetsByPartition: New[string](16),
		collectMetrics:     collectMetrics,
		gapBufferSize:      100,
		mu:                 new(sync.Mutex),
		logger:             nexus.NewDefaultLogger(slog.LevelInfo),
		ctx:                ctx,
		metricsBuffer:      newRingBuffer(snapshot.DefaultBucketDuration, snapshot.DefaultBucketCount),
	}

	// set up tracker starting at offset 0
	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 0,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 100),
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker

	// send 10 messages in order - normal advancement
	for i := int64(0); i < 10; i++ {
		wi := pool.Borrow()
		wi.Message.Partition = 0
		wi.Message.Offset = i
		wi.PreviousOffset = i - 1
		wi.Metrics.Partition = 0
		wi.Metrics.Offset = i

		committer.mu.Lock()
		committer.processCommit(wi, time.Now())
		committer.mu.Unlock()
	}

	// verify no Orphaned traits were set (last item still in Ready, 9 collected)
	for i, traits := range collectedItems {
		if traits&nexus.Orphaned != 0 {
			t.Errorf("offset %d: Orphaned trait should NOT be set on normal advance", i)
		}
	}

	// verify Ready also doesn't have Orphaned trait
	if tracker.Ready != nil && tracker.Ready.Metrics.Traits&nexus.Orphaned != 0 {
		t.Error("Ready item should NOT have Orphaned trait on normal advance")
	}

	t.Logf("verified %d collected items have no Orphaned trait", len(collectedItems))
}

// Test_OrphanedTrait_CommittedPlusOneAhead verifies that the Orphaned trait is set
// when a WorkItem arrives with offset behind CommittedPlusOne (rebalance scenario)
func Test_OrphanedTrait_CommittedPlusOneAhead(t *testing.T) {
	ctx := context.Background()
	cfg := config.DemuxConfig{}
	cfg.SetDemuxConfigDefaults()
	pool := alloc.NewWorkItemsPool[string](cfg)

	// track collected metrics with traits
	var collectedTraits nexus.Traits
	var collectedOffset int64
	collectMetrics := func(wi *ports.WorkItem[string]) {
		collectedTraits = wi.Metrics.Traits
		collectedOffset = wi.Message.Offset
		pool.Return(wi)
	}

	committer := &Committer[string]{
		offsetsByPartition: New[string](16),
		collectMetrics:     collectMetrics,
		gapBufferSize:      100,
		mu:                 new(sync.Mutex),
		logger:             nexus.NewDefaultLogger(slog.LevelInfo),
		ctx:                ctx,
		metricsBuffer:      newRingBuffer(snapshot.DefaultBucketDuration, snapshot.DefaultBucketCount),
	}

	// simulate rebalance: CommittedPlusOne advanced to 50 (by another consumer)
	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 50,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 100),
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker

	// orphaned WorkItem: completed after partition reassigned with advanced offset
	wi := pool.Borrow()
	wi.Message.Partition = 0
	wi.Message.Offset = 25 // behind CommittedPlusOne
	wi.Metrics.Partition = 0
	wi.Metrics.Offset = 25

	committer.mu.Lock()
	committer.processCommit(wi, time.Now())
	committer.mu.Unlock()

	// verify Orphaned trait was set
	if collectedTraits&nexus.Orphaned == 0 {
		t.Error("expected Orphaned trait to be set when offset behind CommittedPlusOne")
	}
	if collectedOffset != 25 {
		t.Errorf("expected offset 25, got %d", collectedOffset)
	}
}

// Test_OrphanedTrait_ReadyOffsetAhead verifies that the Orphaned trait is set
// when a WorkItem arrives with offset behind the current Ready offset
func Test_OrphanedTrait_ReadyOffsetAhead(t *testing.T) {
	ctx := context.Background()
	cfg := config.DemuxConfig{}
	cfg.SetDemuxConfigDefaults()
	pool := alloc.NewWorkItemsPool[string](cfg)

	// track collected metrics with traits
	var collectedTraits nexus.Traits
	var collectedOffset int64
	collectMetrics := func(wi *ports.WorkItem[string]) {
		collectedTraits = wi.Metrics.Traits
		collectedOffset = wi.Message.Offset
		pool.Return(wi)
	}

	committer := &Committer[string]{
		offsetsByPartition: New[string](16),
		collectMetrics:     collectMetrics,
		gapBufferSize:      100,
		mu:                 new(sync.Mutex),
		logger:             nexus.NewDefaultLogger(slog.LevelInfo),
		ctx:                ctx,
		metricsBuffer:      newRingBuffer(snapshot.DefaultBucketDuration, snapshot.DefaultBucketCount),
	}

	// set up tracker with Ready at offset 100
	readyWi := pool.Borrow()
	readyWi.Message.Partition = 0
	readyWi.Message.Offset = 100
	readyWi.Metrics.Partition = 0
	readyWi.Metrics.Offset = 100

	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 50,
		Ready:            readyWi,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 100),
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker

	// orphaned WorkItem: offset 75 is behind Ready offset 100
	wi := pool.Borrow()
	wi.Message.Partition = 0
	wi.Message.Offset = 75 // behind Ready offset
	wi.Metrics.Partition = 0
	wi.Metrics.Offset = 75

	committer.mu.Lock()
	committer.processCommit(wi, time.Now())
	committer.mu.Unlock()

	// verify Orphaned trait was set
	if collectedTraits&nexus.Orphaned == 0 {
		t.Error("expected Orphaned trait to be set when offset behind Ready")
	}
	if collectedOffset != 75 {
		t.Errorf("expected offset 75, got %d", collectedOffset)
	}

	// verify Ready is unchanged
	if tracker.Ready.Message.Offset != 100 {
		t.Errorf("Ready should remain at offset 100, got %d", tracker.Ready.Message.Offset)
	}
}

// Test_OrphanedTrait_ResetCommittedOffsets verifies that the Orphaned trait is set
// when ResetCommittedOffsets finds a Ready item behind the new committed offset
func Test_OrphanedTrait_ResetCommittedOffsets(t *testing.T) {
	ctx := context.Background()
	cfg := config.DemuxConfig{}
	cfg.SetDemuxConfigDefaults()
	pool := alloc.NewWorkItemsPool[string](cfg)

	// track collected metrics with traits
	var collectedTraits nexus.Traits
	var collectedOffset int64
	collectMetrics := func(wi *ports.WorkItem[string]) {
		collectedTraits = wi.Metrics.Traits
		collectedOffset = wi.Message.Offset
		pool.Return(wi)
	}

	committer := &Committer[string]{
		offsetsByPartition: New[string](16),
		collectMetrics:     collectMetrics,
		gapBufferSize:      100,
		mu:                 new(sync.Mutex),
		logger:             nexus.NewDefaultLogger(slog.LevelInfo),
		ctx:                ctx,
		metricsBuffer:      newRingBuffer(snapshot.DefaultBucketDuration, snapshot.DefaultBucketCount),
	}

	// set up tracker with Ready at offset 25
	readyWi := pool.Borrow()
	readyWi.Message.Partition = 0
	readyWi.Message.Offset = 25
	readyWi.Metrics.Partition = 0
	readyWi.Metrics.Offset = 25

	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 25,
		Ready:            readyWi,
		GapBuffer:        make([]*ports.WorkItem[string], 0, 100),
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker

	// simulate rebalance: another consumer advanced to offset 50
	committer.ResetCommittedOffsets(map[int32]int64{0: 50})

	// verify Orphaned trait was set on the old Ready
	if collectedTraits&nexus.Orphaned == 0 {
		t.Error("expected Orphaned trait to be set when Ready behind new CommittedPlusOne")
	}
	if collectedOffset != 25 {
		t.Errorf("expected offset 25, got %d", collectedOffset)
	}

	// verify Ready was cleared
	if tracker.Ready != nil {
		t.Error("Ready should be nil after ResetCommittedOffsets clears orphaned item")
	}

	// verify CommittedPlusOne was updated
	if tracker.CommittedPlusOne != 50 {
		t.Errorf("CommittedPlusOne should be 50, got %d", tracker.CommittedPlusOne)
	}
}
