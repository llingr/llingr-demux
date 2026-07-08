// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Regression tests for the unknown-baseline defect family: when an adapter
// cannot supply the broker's committed offset at assign, every orphan guard
// (all baseline comparisons) goes inert. An orphaned work item arriving
// around a re-assign could then survive as a stale Ready and commit
// BACKWARDS, or poison the gap buffer below the watermark and silently stall
// commits; either way the next handoff re-reads a contiguous offset block.

// resetZeroCapturingLogger captures Error log lines for assertions.
type resetZeroCapturingLogger struct {
	mu     sync.Mutex
	errors []string
}

func (l *resetZeroCapturingLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (l *resetZeroCapturingLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (l *resetZeroCapturingLogger) Warn(_ context.Context, _ string, _ ...any)  {}
func (l *resetZeroCapturingLogger) Error(_ context.Context, format string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, format)
}

func (l *resetZeroCapturingLogger) errorContaining(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.errors {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

// newOrphanTestCommitter builds a committer with a capturing broker-commit fn.
func newOrphanTestCommitter(t *testing.T, commitOffsets nexus.CommitOffsets[string],
	logger nexus.Logger) (*Committer[string], *alloc.WorkItemsPool[string], func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = nexus.NewDefaultLogger(slog.LevelError)
	}
	cfg := config.DemuxConfig{
		AutoCommitInterval: 250 * time.Millisecond, // minimum allowed for fast tests
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	return committer, pool, cancel
}

// makeOrphanItem builds a work item with explicit predecessor linkage.
func makeOrphanItem(pool *alloc.WorkItemsPool[string], partition int32, offset, prev int64,
	first bool) *ports.WorkItem[string] {

	item := pool.Borrow()
	item.Message.Partition = partition
	item.Message.Offset = offset
	item.Metrics.Partition = partition
	item.Metrics.Offset = offset
	item.First = first
	item.PreviousOffset = prev
	return item
}

// capturingCommit returns a broker-commit fn that records committed offsets.
func capturingCommit() (nexus.CommitOffsets[string], func() []int64) {
	var mu sync.Mutex
	var committed []int64
	fn := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			committed = append(committed, m.Offset)
		}
		return msgs, nil
	}
	snapshot := func() []int64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]int64{}, committed...)
	}
	return fn, snapshot
}

// Test_ResetToZero_ClearsStaleEpochState: a re-assign with an unknown baseline
// (CommittedOffset=0, an achievable offset) must clear the tracker's Ready and
// GapBuffer. Anything still in the tracker at assign time is from a previous
// ownership epoch: the revoke drain already committed everything committable,
// so leftovers are definitionally stale, and with a 0 baseline the old
// "reject Ready below the baseline" guard can never fire (nothing is < 0).
// Without the clear, the stale Ready is committed on the next tick, moving the
// broker's committed offset BACKWARDS below the position another owner
// advanced it to, and the next handoff re-reads the span (duplicates).
func Test_ResetToZero_ClearsStaleEpochState(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	// Epoch 1: partition owned, baseline 100; an orphan completion at offset 100
	// becomes Ready, and an out-of-order completion at 105 sits in the gap buffer.
	committer.ResetCommittedOffsets(map[int32]int64{partition: 100})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 100, 99, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 105, 104, false), now)
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if tracker.Ready == nil || tracker.Ready.Message.Offset != 100 || len(tracker.GapBuffer) != 1 {
		committer.mu.Unlock()
		t.Fatalf("setup failed: want Ready=100 and one gap item, got Ready=%v gap=%d",
			tracker.Ready, len(tracker.GapBuffer))
	}
	committer.mu.Unlock()

	// Revoke: the partition leaves this consumer with the stale state in place
	// (no commit tick ran, so the Layer-5 unassigned guard never fired).
	committer.MarkPartitionRevoked(partition)

	// Re-assign with unknown baseline (0). Meanwhile another owner
	// advanced the broker to 150.
	committer.ResetCommittedOffsets(map[int32]int64{partition: 0})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	tracker = committer.offsetsByPartition.PartitionMap[partition]
	if tracker.Ready != nil {
		t.Errorf("stale Ready (offset %d) survived a reset with unknown baseline; "+
			"it will be committed backwards on the next tick", tracker.Ready.Message.Offset)
	}
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("stale gap buffer (%d items) survived a reset with unknown baseline; "+
			"a below-watermark leftover permanently stalls the flush walk", len(tracker.GapBuffer))
	}
	committer.mu.Unlock()

	// New epoch: the first record (First flag) rebuilds the baseline at 150.
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 150, 149, true), now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	for _, offset := range got {
		if offset < 150 {
			t.Errorf("broker commit regression: offset %d committed while the broker position is 150 "+
				"(committed set: %v)", offset, got)
		}
	}
	if len(got) == 0 || got[len(got)-1] != 150 {
		t.Errorf("expected the new epoch's offset 150 to be committed, got %v", got)
	}
}

// Test_GapBufferPoisonBelowWatermarkIsPruned: an orphan that arrives AFTER the
// unknown-baseline reset (so no reset-time clear can catch it) lands in the
// gap buffer below the eventual watermark. The flush walk must prune it, not
// break on it: previously the sorted walk compared the poison at index 0,
// failed the predecessor-linkage test, and broke, so no gap item ever resolved
// again, commits silently stalled at the blocked offset, and the next handoff
// re-read the whole stalled span.
func Test_GapBufferPoisonBelowWatermarkIsPruned(t *testing.T) {
	commitFn, _ := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	// Re-assign with unknown baseline.
	committer.ResetCommittedOffsets(map[int32]int64{partition: 0})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	defer committer.mu.Unlock()

	// The orphan from the previous epoch completes now: baseline is 0, Ready is
	// nil, so no orphan branch can reject it and it is buffered below the
	// watermark the First record is about to establish.
	committer.processCommit(makeOrphanItem(pool, partition, 100, 99, false), now)

	// New epoch: First record establishes the baseline and becomes Ready.
	committer.processCommit(makeOrphanItem(pool, partition, 150, 149, true), now)
	// Out-of-order completion: 152 before 151.
	committer.processCommit(makeOrphanItem(pool, partition, 152, 151, false), now)
	// 151 arrives: direct swap advances Ready to 151 and flags the gap advance.
	committer.processCommit(makeOrphanItem(pool, partition, 151, 150, false), now)

	committer.flushGapBuffers(now)

	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if tracker.Ready == nil || tracker.Ready.Message.Offset != 152 {
		readyOffset := int64(-1)
		if tracker.Ready != nil {
			readyOffset = tracker.Ready.Message.Offset
		}
		t.Errorf("gap-buffer stall: Ready stuck at %d, want 152 "+
			"(the below-watermark poison at 100 must be pruned, not break the walk)", readyOffset)
	}
	if len(tracker.GapBuffer) != 0 {
		offsets := make([]int64, 0, len(tracker.GapBuffer))
		for _, g := range tracker.GapBuffer {
			offsets = append(offsets, g.Message.Offset)
		}
		t.Errorf("gap buffer should be empty after the flush, still holds %v", offsets)
	}
}

// Test_CommitNeverMovesBaselineBackwards: a stale Ready below the current
// baseline must never be committed (the broker's committed offset would move
// backwards), and a commit must never lower CommittedPlusOne. This simulates
// the corrupted state the unknown-baseline reset previously allowed.
func Test_CommitNeverMovesBaselineBackwards(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)

	committer.ResetCommittedOffsets(map[int32]int64{partition: 150})
	committer.MarkPartitionAssigned(partition)

	// Simulate the corruption: a stale orphan occupying Ready below the baseline.
	orphan := makeOrphanItem(pool, partition, 100, 99, false)
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	tracker.Ready = orphan
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	for _, offset := range committedOffsets() {
		if offset < 150 {
			t.Errorf("broker commit regression: stale Ready at offset %d was committed "+
				"while CommittedPlusOne=150", offset)
		}
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker = committer.offsetsByPartition.PartitionMap[partition]
	if tracker.CommittedPlusOne != 150 {
		t.Errorf("CommittedPlusOne moved backwards: got %d, want 150", tracker.CommittedPlusOne)
	}
	if tracker.Ready != nil {
		t.Errorf("stale Ready should be discarded, still holds offset %d", tracker.Ready.Message.Offset)
	}
}

// Test_UnknownBaselineMinusOne_ZeroOffsetOrphanStaysInert: adapters that
// cannot supply the broker's committed offset at assign time pass
// RebalanceInfo.CommittedOffset = -1 ("unknown"), and this test is why the
// sentinel must be unreachable rather than 0. Every orphan guard here is a
// comparison against the seeded baseline, and the flag-independent
// Ready-initialisation rule is CommittedPlusOne == offset
// (committer_process.go checkAndAdvance): with a baseline of 0, a stale
// completion at offset 0 (an orphan from a previous ownership epoch of a
// young partition) initialises itself as Ready and is committed, moving the
// broker's committed offset backwards, and predecessor linkage then carries
// the rest of the stale batch. With -1 no real offset
// can ever equal the baseline, so the orphan buffers inertly and is pruned
// once the epoch's First record anchors the true position.
//
// Honest scope: a stale completion that itself carries First=true can still
// re-anchor an unknown baseline (the First guard is offset >= baseline, which
// any offset passes against any sentinel). That residue is inherent to not
// knowing the broker's committed offset; closing it needs a real baseline at
// assign or epoch-tagged work items, not a better sentinel.
func Test_UnknownBaselineMinusOne_ZeroOffsetOrphanStaysInert(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	// Re-assign with an unknown baseline, the -1 adapter contract.
	committer.ResetCommittedOffsets(map[int32]int64{partition: -1})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	// A stale non-First completion at offset 0 arrives from a closed epoch.
	// Against a 0 baseline this would initialise Ready (0 == 0) and commit;
	// against -1 it must buffer inertly.
	committer.processCommit(makeOrphanItem(pool, partition, 0, -1, false), now)
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if tracker.Ready != nil {
		committer.mu.Unlock()
		t.Fatalf("orphan at offset 0 initialised itself as Ready against the unknown baseline (Ready=%d)",
			tracker.Ready.Message.Offset)
	}
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	if got := committedOffsets(); len(got) != 0 {
		t.Fatalf("nothing should commit while the baseline is unknown, got %v", got)
	}

	// The epoch's First record anchors the true position; the buffered orphan
	// is below the watermark. The commit that consumes the anchored Ready flags
	// a gap advance (buffer non-empty), and the NEXT flush prunes the orphan:
	// arrivals that cannot initialise Ready no longer flag at buffering time,
	// so the prune rides the commit-then-flush cadence of the tick.
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 500, 499, true), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	got := committedOffsets()
	if len(got) != 1 || got[0] != 500 {
		t.Errorf("expected exactly the anchored offset 500 committed, got %v", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker = committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("the stale orphan should be pruned, gap buffer still holds %d item(s)",
			len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 501 {
		t.Errorf("CommittedPlusOne = %d, want 501", tracker.CommittedPlusOne)
	}
}

// Test_CommitOffsets_SurfacesBrokerError: a failed broker commit must be
// visible to callers (previously CommitOffsets logged it and returned nil, so
// a failed FINAL commit before a partition handoff was invisible to the drain,
// the rebalance callback, and the ack). The drain surfaces it loudly but does
// not escalate: the pipeline is drained, the partition is leaving anyway, and
// the uncommitted tail is an at-least-once re-read for the next owner, not an
// availability failure.
func Test_CommitOffsets_SurfacesBrokerError(t *testing.T) {
	brokerErr := errors.New("broker rejected the commit")
	failingCommit := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return nil, fmt.Errorf("commit records failed: %w", brokerErr)
	}
	logger := &resetZeroCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, failingCommit, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 100})
	committer.MarkPartitionAssigned(partition)
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 100, 99, false), now)
	committer.mu.Unlock()

	err := committer.CommitOffsets()
	if err == nil {
		t.Fatal("CommitOffsets must return the broker error, got nil")
	}

	// The drain path surfaces the failure with its consequence but returns nil.
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if tracker.Ready == nil {
		committer.mu.Unlock()
		t.Fatal("setup failed: Ready should be retained after a failed commit")
	}
	committer.mu.Unlock()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	if drainErr := committer.DrainCommitter(timer); drainErr != nil {
		t.Errorf("DrainCommitter must not escalate a broker commit rejection, got %v", drainErr)
	}
	if !logger.errorContaining("re-read") {
		t.Error("expected the drain to log the failed final commit with its re-read consequence")
	}
}
