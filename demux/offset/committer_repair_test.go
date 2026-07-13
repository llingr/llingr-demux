// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests pin the commit-cadence successor repair (repairSuccessor) and
// the exact-match walk's division of labour with it. The flush walk advances
// on exact evidence only, which is always sufficient within one ownership
// epoch; predecessor linkage broken by log compaction ACROSS epochs (a stale
// orphaned work item sorting ahead of the chain's true continuation, a stamp
// superseded by the repair's own progress, a second claimant of one anchor)
// stalls the partition with Ready empty: commits stop, every completion joins
// the buffer, and the buffer grows without bound. The repair runs per commit
// tick for a partition in exactly that state, promotes the lowest provable
// successor of the record committed this epoch, prunes the provably stale run
// below it, and logs a warning explaining what broke.
//
// The second half of this file is the MUST-NOT suite: every seam where a gap
// is VALID (a worker still processing a work item) and the repair must refuse
// to jump it, however many ticks fire. The dividing line is the proof: an
// item awaiting an in-flight predecessor carries a stamp ABOVE the committed
// record, so no rule can mistake it for a successor; an item with no
// committed-this-epoch anchor to reason from is never touched at all.

// repairWarnCapturingLogger captures Warn log lines for assertions.
type repairWarnCapturingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *repairWarnCapturingLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (l *repairWarnCapturingLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (l *repairWarnCapturingLogger) Error(_ context.Context, _ string, _ ...any) {}
func (l *repairWarnCapturingLogger) Warn(_ context.Context, format string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, format)
}

func (l *repairWarnCapturingLogger) warnContaining(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, w := range l.warns {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}

// Test_RepairPromotesSuccessorBehindStaleBlocker: the canonical stall. Broker
// records 7, 8, 12 (9-11 compacted after a previous epoch read them); this
// epoch commits 8. A stale orphan for 11 (stamped with its then-true
// predecessor 10) buffers, and the epoch's real successor 12 (stamped 8)
// buffers behind it in sorted order. The exact walk stops at 11 and nothing
// ever flags again; the repair must promote 12, prune 11, and warn.
func Test_RepairPromotesSuccessorBehindStaleBlocker(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	// record 8 completes and commits: CommittedPlusOne=9, LastCommittedOffset=8
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 11, 10, false), now) // stale orphan
	committer.processCommit(makeOrphanItem(pool, partition, 12, 8, false), now)  // real successor
	committer.flushGapBuffers(now)                                               // batch end: the exact walk stops at 11
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 12 {
		t.Errorf("stale-blocker stall: offset 12 never committed (committed set: %v); "+
			"the partition is stalled and the gap buffer will grow unboundedly", got)
	}
	if !logger.warnContaining("repaired successor on partition 0") {
		t.Errorf("the repair must log a warning explaining the broken linkage, warns: %v",
			logger.warns)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty (blocker pruned), holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 13 {
		t.Errorf("CommittedPlusOne = %d, want 13", tracker.CommittedPlusOne)
	}
}

// Test_RepairPromotesSuccessorAfterReadyCommits: the Ready-held variant. 12
// (predecessor 9) buffers while Ready is empty, then 9 arrives and initialises
// Ready with the stale orphan 11 sorted between them. Ready itself is the next
// tick's commit, so the blockage surfaces as Ready-empty one tick later; the
// repair then promotes 12 past the blocker.
func Test_RepairPromotesSuccessorAfterReadyCommits(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 11, 10, false), now) // stale orphan
	committer.processCommit(makeOrphanItem(pool, partition, 12, 9, false), now)  // real, awaits 9
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now)   // initialises Ready
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 3; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 12 {
		t.Errorf("stale-blocker stall: offset 12 never committed (committed set: %v)", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
}

// Test_RepairPrunesStaleRunBeforeProvableSuccessor: a RUN of stale blockers.
// Every buffered item below a provable successor sits inside an interval the
// successor's own stamp proves empty, so the whole run is pruned in one repair.
func Test_RepairPrunesStaleRunBeforeProvableSuccessor(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 11, 10, false), now) // stale
	committer.processCommit(makeOrphanItem(pool, partition, 14, 13, false), now) // stale
	committer.processCommit(makeOrphanItem(pool, partition, 16, 8, false), now)  // real successor
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 16 {
		t.Errorf("stale-run stall: offset 16 never committed (committed set: %v)", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty (both blockers pruned), holds %d item(s)",
			len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 17 {
		t.Errorf("CommittedPlusOne = %d, want 17", tracker.CommittedPlusOne)
	}
}

// Test_AdjacentSuccessorHealsViaPositionalBackstop: an item at exactly
// committed+1 with a broken stamp needs no repair at all. Committing its
// predecessor re-baselines CommittedPlusOne onto its offset, so the next
// tick's flush promotes it positionally; the repair's adjacency proof exists
// for the same reason but is reached only if this backstop were ever bypassed.
// Green by construction; pins the equivalence.
func Test_AdjacentSuccessorHealsViaPositionalBackstop(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	// linkage successor of the committed record 8
	committer.processCommit(makeOrphanItem(pool, partition, 12, 8, false), now)
	// offset-adjacent to 12; the stamp (11) is a stale epoch's claim and wrong
	committer.processCommit(makeOrphanItem(pool, partition, 13, 11, false), now)
	committer.flushGapBuffers(now) // walk promotes 12, stops at 13's stamp
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	if got := committedOffsets(); len(got) != 2 || got[1] != 12 {
		t.Fatalf("tick #1 should commit 12, got %v", got)
	}
	// committing 12 makes CommittedPlusOne 13: tick #2's flush promotes 13
	// positionally, stamp irrelevant
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) != 3 || got[2] != 13 {
		t.Errorf("adjacent successor should heal on tick #2 via the positional rule, got %v", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 14 {
		t.Errorf("CommittedPlusOne = %d, want 14", tracker.CommittedPlusOne)
	}
}

// Test_AdjacentSuccessorBehindSwappedReady_Heals: the swap-path variant of the
// positional backstop. The broken-stamp item buffers unflagged, Ready swaps
// forward to its neighbour and commits; the next tick promotes the adjacent
// item positionally. Green by construction; pins that no arrival path needs
// broadened rules for adjacency.
func Test_AdjacentSuccessorBehindSwappedReady_Heals(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 13, 11, false), now) // broken stamp
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now)   // Ready
	committer.processCommit(makeOrphanItem(pool, partition, 12, 9, false), now)  // swaps Ready to 12
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 13 {
		t.Errorf("adjacent successor behind the swapped Ready should heal by tick #2, got %v", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
}

// Test_RepairMustNotJumpViaStrayFirst: a First's authority lives in the
// baseline, not in the flag. Every legitimately buffered First raised
// CommittedPlusOne to its own offset on arrival, so it qualifies positionally;
// a First found ABOVE the baseline was overridden by a later, higher authority
// (an assign that re-based the partition below it, or an operator resetting
// the group offset backwards), and the offsets in between may hold valid
// in-flight work. Honouring the bare flag would jump that work: loss. The
// repair must leave it alone.
func Test_RepairMustNotJumpViaStrayFirst(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now) // Ready = 9
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	// a stray First for 15 above the baseline (its arrival-time raise was
	// overridden): records 10-14 may be validly in flight
	tracker.GapBuffer = append(tracker.GapBuffer, makeOrphanItem(pool, partition, 15, 14, true))
	committer.mu.Unlock()

	// tick #1 commits Ready 9; the following ticks must NOT promote the First
	for tick := 1; tick <= 4; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 9 {
		t.Errorf("the stray First must not be promoted past possibly in-flight work: "+
			"committed set %v, want tail 9", got)
	}
	if logger.warnContaining("repaired successor") {
		t.Errorf("no repair may be logged for a stray First, warns: %v", logger.warns)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(tracker.GapBuffer) != 1 {
		t.Errorf("the stray First must stay buffered (rebalance discards it), holds %d item(s)",
			len(tracker.GapBuffer))
	}
}

// Test_RepairLinksSupersededPredecessorStamp: repairing (or exact-promoting) a
// stale claimant advances the committed record PAST the stamp the epoch's own
// next record carries, so that record arrives with a predecessor BELOW the
// anchor: exact rules can never link it and nothing flags it. The stamp is
// still delivery evidence (its interval held no records when fetched, and
// compaction only removes), so the repair's at-or-below proof must promote it.
// Tick-only after the arrival: the repair, not any flag, is the healer.
func Test_RepairLinksSupersededPredecessorStamp(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// a stale claimant of the committed record 8 (its own record since
	// compacted): the exact walk promotes and commits it, advancing the
	// anchor to 10
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 10, 8, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	if got := committedOffsets(); len(got) != 2 || got[1] != 10 {
		t.Fatalf("stale claimant should commit via exact linkage, committed set %v", got)
	}

	// the epoch's own next record, stamped against the superseded anchor 8
	// (records 9-11 do not exist in the current stream)
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 12, 8, false), now)
	committer.mu.Unlock()

	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 12 {
		t.Errorf("superseded-stamp stall: offset 12 never committed (committed set: %v); "+
			"the epoch's own chain is stranded behind its promoted claimant", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
}

// Test_RepairLinksSecondClaimantOfSameAnchor: two stale claimants of the SAME
// committed record (stamped in different epochs as compaction progressed).
// The exact walk promotes the lower; committing it advances the anchor past
// the higher one's stamp, which the repair must then link at-or-below. One
// claimant converges per tick.
func Test_RepairLinksSecondClaimantOfSameAnchor(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 10, 8, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 13, 8, false), now)
	committer.flushGapBuffers(now) // walk promotes 10 exactly, stops at 13
	committer.mu.Unlock()

	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) != 3 || got[2] != 13 {
		t.Errorf("second-claimant stall: want committed set [8 10 13], got %v", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 14 {
		t.Errorf("CommittedPlusOne = %d, want 14", tracker.CommittedPlusOne)
	}
}

// Test_RepairNeverActsWithoutProvableSuccessor: the guard. Items that merely
// await their in-flight predecessors (true holes) satisfy no proof: neither
// the walk nor the repair may prune or promote them, however many ticks fire.
// The chain then heals normally as the holes fill. Pins the repair against
// over-eager pruning: lifting past a genuine hole is loss, not duplicates.
func Test_RepairNeverActsWithoutProvableSuccessor(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// both await in-flight predecessors: 11 awaits 10, 14 awaits 13
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 11, 10, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 14, 13, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	// ticks drive the repair against the standing holes: it must be a no-op
	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}
	if got := committedOffsets(); len(got) != 1 {
		t.Fatalf("nothing further may commit against standing holes, committed set %v", got)
	}
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 2 {
		committer.mu.Unlock()
		t.Fatalf("nothing is provably stale: no item may be pruned, buffer holds %d item(s), want 2",
			len(tracker.GapBuffer))
	}

	// the holes fill: 9, 10 link the head; 13 (record 12 does not exist, its
	// stamp says so) links 11 and then 14
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 10, 9, false), now)
	committer.flushGapBuffers(now)
	if len(tracker.GapBuffer) != 1 {
		committer.mu.Unlock()
		t.Fatalf("14 still awaits 13 and must stay buffered, buffer holds %d item(s), want 1",
			len(tracker.GapBuffer))
	}
	committer.processCommit(makeOrphanItem(pool, partition, 13, 11, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 14 {
		t.Errorf("chain should heal to 14 once the holes fill, committed set %v", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
}

// ---------------------------------------------------------------------------
// MUST-NOT suite: valid gaps the repair must never jump.
// ---------------------------------------------------------------------------

// Test_RepairMustNotJumpHoleBehindCommitted: the everyday shape. The record
// directly after the committed one is with a slow worker; its successors
// complete out of order and buffer with honest in-epoch stamps. Nothing may
// commit, nothing may be pruned, no warning may be logged, however many ticks
// fire; the chain heals only when the worker finishes.
func Test_RepairMustNotJumpHoleBehindCommitted(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// record 9 is still processing; 10 and 11 completed out of order
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 10, 9, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 11, 10, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 4; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}
	if got := committedOffsets(); len(got) != 1 || got[0] != 8 {
		t.Fatalf("nothing may commit while 9 is in flight, committed set %v", got)
	}
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 2 {
		committer.mu.Unlock()
		t.Fatalf("10 and 11 must stay buffered, holds %d item(s)", len(tracker.GapBuffer))
	}

	// the worker finishes: 9 initialises Ready and the chain folds
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 11 {
		t.Errorf("chain should heal to 11 once 9 completes, committed set %v", got)
	}
	if logger.warnContaining("repaired successor") {
		t.Errorf("a valid in-flight hole must never be repaired, warns: %v", logger.warns)
	}
}

// Test_RepairMustNotJumpSecondHoleAfterPartialProgress: two workers slow at
// once. After the first hole fills and its chain commits, the repair sees a
// fresh anchor and a still-buffered tail; the tail's stamps sit ABOVE the new
// anchor (12 is in flight), so it must again refuse.
func Test_RepairMustNotJumpSecondHoleAfterPartialProgress(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// records 9 and 12 are in flight; everything around them has completed
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 10, 9, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 11, 10, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 13, 12, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 14, 13, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}
	if got := committedOffsets(); len(got) != 1 {
		t.Fatalf("nothing may commit while 9 is in flight, committed set %v", got)
	}

	// 9 completes: the chain commits through 11 and STOPS at the second hole
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	for tick := 1; tick <= 3; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (post-9 tick #%d): %v", tick, err)
		}
	}
	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 11 {
		t.Fatalf("chain should commit through 11 and stall at the hole at 12, committed set %v", got)
	}
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 2 {
		committer.mu.Unlock()
		t.Fatalf("13 and 14 must stay buffered while 12 is in flight, holds %d item(s)",
			len(tracker.GapBuffer))
	}

	// 12 completes: everything folds
	committer.processCommit(makeOrphanItem(pool, partition, 12, 11, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	got = committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 14 {
		t.Errorf("chain should heal to 14 once 12 completes, committed set %v", got)
	}
	if logger.warnContaining("repaired successor") {
		t.Errorf("valid in-flight holes must never be repaired, warns: %v", logger.warns)
	}
}

// Test_RepairMustNotJumpBrokerGapAwaitingPredecessor: gappy offsets are NOT
// the repair signature. Records 8, 9, 12 exist (10-11 never delivered); 9 is
// with a worker and 12 completed first, stamped predecessor 9. The buffer
// LOOKS like the compaction case (offset jumps) but the stamp says exactly
// what it awaits, and only the stamp decides: no repair, and the normal exact
// linkage folds the chain when 9 completes.
func Test_RepairMustNotJumpBrokerGapAwaitingPredecessor(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 12, 9, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 3; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}
	if got := committedOffsets(); len(got) != 1 {
		t.Fatalf("12 awaits in-flight 9 and must not commit, committed set %v", got)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 12 {
		t.Errorf("exact linkage should fold 12 once 9 completes, committed set %v", got)
	}
	if logger.warnContaining("repaired successor") {
		t.Errorf("a broker gap awaiting its predecessor must never be repaired, warns: %v",
			logger.warns)
	}
}

// Test_RepairMustNotActWithoutCommittedAnchor: nothing has been committed
// this epoch (LastCommittedOffset -1), the epoch's opening record is with a
// worker, and its successors buffer. There is no record to reason from: the
// baseline is a position, and the record it names is validly in flight -
// promoting anything would lose it. The repair must be inert until the
// epoch's own commits create an anchor.
func Test_RepairMustNotActWithoutCommittedAnchor(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	// record 8 (the baseline record) is still processing; 9 and 10 completed
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 9, 8, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 10, 9, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 4; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}
	if got := committedOffsets(); len(got) != 0 {
		t.Fatalf("nothing may commit while the baseline record is in flight, committed set %v", got)
	}
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 2 {
		committer.mu.Unlock()
		t.Fatalf("9 and 10 must stay buffered, holds %d item(s)", len(tracker.GapBuffer))
	}

	// the baseline record completes: the whole chain folds via exact rules
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 10 {
		t.Errorf("chain should heal to 10 once the baseline record completes, committed set %v", got)
	}
	if logger.warnContaining("repaired successor") {
		t.Errorf("no repair may fire without a committed-this-epoch anchor, warns: %v", logger.warns)
	}
}

// Test_RepairMustNotResurrectBelowBaseline: a leftover below the baseline can
// never be a successor (committing it would move the broker position
// backwards). The repair must skip it entirely, select nothing, and leave the
// pruning to the flagged walk.
func Test_RepairMustNotResurrectBelowBaseline(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// a stale below-baseline leftover, injected directly (arrivals below the
	// baseline are discarded on arrival; this pins the repair's own guard)
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	tracker.GapBuffer = append(tracker.GapBuffer, makeOrphanItem(pool, partition, 5, 4, false))
	committer.mu.Unlock()

	for tick := 1; tick <= 3; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	for _, offset := range got {
		if offset < 8 {
			t.Errorf("a below-baseline offset must never commit (backwards), committed set %v", got)
		}
	}
	if logger.warnContaining("repaired successor") {
		t.Errorf("no repair may be logged for below-baseline junk, warns: %v", logger.warns)
	}
}

// Test_RepairMustNotTrustNegativeStamp: a stamp of -1 carries no delivery
// evidence at all. Whatever anchor is committed, an item with a negative
// stamp (and no other proof) must never be promoted.
func Test_RepairMustNotTrustNegativeStamp(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 14, -1, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 3; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) != 1 || got[0] != 8 {
		t.Errorf("an evidence-free stamp must never be promoted, committed set %v", got)
	}
	if logger.warnContaining("repaired successor") {
		t.Errorf("no repair may be logged for an evidence-free stamp, warns: %v", logger.warns)
	}
}

// Test_RepairSparesWaitersAboveSuccessor: the prune boundary. A repair prunes
// ONLY the provably stale run below its successor; an item ABOVE the
// successor whose stamp names in-flight work must survive the repair intact.
func Test_RepairSparesWaitersAboveSuccessor(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 11, 10, false), now) // stale blocker
	committer.processCommit(makeOrphanItem(pool, partition, 12, 8, false), now)  // provable successor
	committer.processCommit(makeOrphanItem(pool, partition, 20, 19, false), now) // awaits in-flight 19
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 12 {
		t.Fatalf("the repair should commit 12, committed set %v", got)
	}
	if !logger.warnContaining("repaired successor") {
		t.Fatalf("the repair must log its action, warns: %v", logger.warns)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 1 || tracker.GapBuffer[0].Message.Offset != 20 {
		offsets := make([]int64, 0, len(tracker.GapBuffer))
		for _, g := range tracker.GapBuffer {
			offsets = append(offsets, g.Message.Offset)
		}
		t.Errorf("the waiter at 20 must survive the repair, buffer holds %v", offsets)
	}
}

// Test_RepairGates: the entry gates, pinned by direct call. The repair must
// be a no-op when Ready is held (the tick will commit it: no blockage), when
// a flush is pending (the walk has first right of refusal), when the buffer
// is empty, and when nothing has been committed this epoch.
func Test_RepairGates(t *testing.T) {
	commitFn, _ := capturingCommit()
	logger := &repairWarnCapturingLogger{}
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, logger)
	defer cancel()

	partition := int32(0)

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]

	// (a) no committed-this-epoch anchor: even a qualifying-looking stamp is inert
	tracker.GapBuffer = append(tracker.GapBuffer, makeOrphanItem(pool, partition, 12, 7, false))
	committer.repairSuccessor(partition, tracker)
	if tracker.Ready != nil {
		t.Fatal("gate (a): repair must not act without a committed anchor")
	}

	// establish an anchor for the remaining gates
	tracker.LastCommittedOffset = 8
	tracker.CommittedPlusOne = 9

	// (b) flush pending: the walk has first right of refusal
	tracker.NeedsGapAdvance = true
	committer.repairSuccessor(partition, tracker)
	if tracker.Ready != nil {
		t.Fatal("gate (b): repair must defer to a pending flush")
	}
	tracker.NeedsGapAdvance = false

	// (c) Ready held: no blockage, the tick will commit it
	tracker.Ready = makeOrphanItem(pool, partition, 9, 8, false)
	committer.repairSuccessor(partition, tracker)
	if tracker.Ready == nil || tracker.Ready.Message.Offset != 9 {
		t.Fatal("gate (c): repair must not touch a held Ready")
	}
	tracker.Ready = nil

	// gates released: the same buffered item now repairs
	committer.repairSuccessor(partition, tracker)
	if tracker.Ready == nil || tracker.Ready.Message.Offset != 12 {
		t.Fatal("with all gates released the qualifying item must repair")
	}

	// (d) empty buffer: nothing to do
	tracker.Ready = nil
	committer.repairSuccessor(partition, tracker)
	if tracker.Ready != nil {
		t.Fatal("gate (d): repair must be a no-op on an empty buffer")
	}
}
