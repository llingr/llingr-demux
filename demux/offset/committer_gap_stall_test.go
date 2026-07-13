// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"testing"
	"time"
)

// These tests pin the "stranded gap buffer" stall: Ready can only be
// initialised by an ARRIVING completion whose offset equals CommittedPlusOne,
// and the gap buffer is never re-examined while Ready is nil. Any path that
// consumes Ready while the next committable completion is already inside the
// buffer (or when the expected next offset does not exist on the broker) would
// otherwise stall the partition permanently: commits stop, every later
// completion joins the buffer, and the buffer grows without bound
// (resizeGapBuffer grows it and guard tokens are released on completion, so
// there is no backpressure). The flush walk must be able to RE-INITIALISE
// Ready from the buffer.

// Test_CommitBoundaryBrokerGap_DoesNotStall: broker offsets are not contiguous
// on compacted topics or around transaction control records. Predecessor
// linkage handles a gap while Ready is set (the swap path), but a commit
// tick consumes Ready at an arbitrary moment, and at that moment the successor
// is by construction not yet completed. If the committed offset sits just
// before a broker gap, the successor's completion arrives with an offset that
// can never equal CommittedPlusOne (that offset does not exist), so it buffers
// and, without re-initialisation from the buffer, the partition stalls forever.
func Test_CommitBoundaryBrokerGap_DoesNotStall(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 100})
	committer.MarkPartitionAssigned(partition)

	// Offset 100 completes and is committed by a tick: CommittedPlusOne = 101.
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 100, 99, false), now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// Broker gap: offset 101 does not exist (compacted away). The next record
	// is 102 with predecessor linkage to 100; its completion arrives AFTER the
	// commit consumed Ready. 102 != CommittedPlusOne (101), so it cannot
	// initialise Ready on arrival; the batch-end flush must re-initialise it
	// from the buffer via its predecessor linkage
	// (PreviousOffset 100 == LastCommittedOffset).
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 102, 100, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 102 {
		t.Errorf("commit-boundary gap stall: offset 102 was never committed (committed set: %v); "+
			"the partition is stalled and the gap buffer will grow unboundedly", got)
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, still holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 103 {
		t.Errorf("CommittedPlusOne = %d, want 103", tracker.CommittedPlusOne)
	}
}

// Test_CommitTickReInitialisesWithoutFurtherTraffic: the batch-end flush can
// only re-initialise Ready when another ingest batch arrives. If completions
// buffered against an unknown baseline (for example, deliveries racing ahead
// of the assign) and the assign then establishes a usable baseline with NO
// further traffic ever arriving, the buffer would strand forever. The commit
// tick must therefore attempt the flag-gated flush itself: with the flag set
// and nothing else flowing, the tick alone must initialise Ready, walk, and
// commit the buffered chain.
func Test_CommitTickReInitialisesWithoutFurtherTraffic(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	// Completions arrive BEFORE any assign: baseline unknown (fresh tracker,
	// -1), everything buffers, nothing can initialise Ready.
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 0, -1, true), now)
	committer.processCommit(makeOrphanItem(pool, partition, 1, 0, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 2, 1, false), now)
	committer.flushGapBuffers(now) // batch-end flush: nothing actionable yet
	committer.mu.Unlock()

	// The assign lands with a usable baseline; no further completions ever
	// arrive, so no ingest batch will run another flush.
	committer.ResetCommittedOffsets(map[int32]int64{partition: 0})
	committer.MarkPartitionAssigned(partition)

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 2 {
		t.Errorf("idle-strand stall: buffered chain never committed by the tick alone "+
			"(committed set: %v, want ... 2)", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty after the tick's flush, holds %d item(s)",
			len(tracker.GapBuffer))
	}
}

// Test_ResetFlagsBufferedItems_TickAloneCommits: non-First completions that
// buffered BEFORE the assign (deliveries racing the assign) cannot flag a gap
// advance at arrival: the baseline is still unknown, so no initialisation
// rule can match. The reset that installs the real baseline must flag the
// tracker itself, and the commit tick's flag-gated flush must then initialise
// Ready and commit the chain with NO further traffic ever arriving.
func Test_ResetFlagsBufferedItems_TickAloneCommits(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	// fresh tracker, unknown baseline: both buffer, neither can flag
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 5, 4, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 6, 5, false), now)
	committer.flushGapBuffers(now) // batch end: no flag, nothing actionable
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 2 {
		t.Fatalf("both completions should be buffered, got %d", len(tracker.GapBuffer))
	}
	committer.mu.Unlock()

	// the assign installs the real baseline: the reset must flag the buffered items
	committer.ResetCommittedOffsets(map[int32]int64{partition: 5})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	if !tracker.NeedsGapAdvance {
		committer.mu.Unlock()
		t.Fatal("reset must flag a gap advance when items are already buffered")
	}
	committer.mu.Unlock()

	// no further traffic: the tick alone must flush, initialise Ready, and commit
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 6 {
		t.Errorf("idle-strand stall: the tick alone never committed the buffered chain "+
			"(committed set: %v, want ... 6)", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 7 {
		t.Errorf("CommittedPlusOne = %d, want 7", tracker.CommittedPlusOne)
	}
}

// Test_BufferPromotionRequiresRecordedCommit: predecessor linkage in the flush
// walk must anchor on a commit RECORDED this epoch, never on baseline
// arithmetic (CommittedPlusOne-1). A reset-derived baseline is a position, not
// a record: after compaction the offset below it need not exist, and a stale
// buffered completion from a previous epoch can carry a predecessor stamp that
// collides with the arithmetic by coincidence. Promotion on that coincidence
// commits an offset this epoch has no delivery evidence for; the item must
// stay buffered until the epoch's own records anchor the position.
func Test_BufferPromotionRequiresRecordedCommit(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	// broker state: records 7 and 12 exist (8-11 compacted away); the previous
	// owner processed 7, so the broker's committed-next is 8
	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	// a stale orphan completion for 12 from the previous epoch, stamped with
	// its true predecessor 7, which collides with CommittedPlusOne-1 by
	// arithmetic coincidence
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 12, 7, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	// ticks fire with no delivery evidence from this epoch: nothing may commit
	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
		if got := committedOffsets(); len(got) != 0 {
			t.Fatalf("tick #%d promoted a stale item on baseline arithmetic alone: committed %v "+
				"(nothing was committed this epoch to link against)", tick, got)
		}
	}
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 1 {
		t.Errorf("the stale item must stay buffered, buffer holds %d item(s)", len(tracker.GapBuffer))
	}
	committer.mu.Unlock()

	// the epoch's own delivery arrives: a fresh First for 12 (the prev tracker
	// was reset on assign, so its predecessor stamp is synthetic offset-1)
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 12, 11, true), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	// one more tick so the flagged flush prunes the stale duplicate below the
	// new watermark
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) != 1 || got[0] != 12 {
		t.Errorf("the epoch's First should anchor and commit 12 exactly once, got %v", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("stale duplicate should be pruned, buffer holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 13 {
		t.Errorf("CommittedPlusOne = %d, want 13", tracker.CommittedPlusOne)
	}
}

// Test_CommitNeverAdvancesPastMissingOffset: the dual of the stall pins. The
// committer must stall exactly while a hole exists and resume exactly when it
// fills. A saturated pipeline masks over-eager promotion (holes fill within
// milliseconds), so this drives repeated EMPTY commit ticks against a standing
// hole: 3 buffered, 1 committed, 2 missing. No tick may lift 3 (that would
// move the broker watermark past the unprocessed offset 2: message loss, not
// duplicates); the arrival of 2, and only that, advances the commit to 3.
func Test_CommitNeverAdvancesPastMissingOffset(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 1})
	committer.MarkPartitionAssigned(partition)

	// 3 arrives first (out of order): buffers behind the hole at 2
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 3, 2, false), now)
	// 1 arrives: the baseline offset, becomes Ready
	committer.processCommit(makeOrphanItem(pool, partition, 1, 0, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	// tick #1 commits 1; the hole at 2 remains
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}
	if got := committedOffsets(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("tick #1 should commit exactly offset 1, got %v", got)
	}

	// ticks #2 and #3 fire against the standing hole: 3 must NOT be lifted
	for tick := 2; tick <= 3; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
		if got := committedOffsets(); len(got) != 1 {
			t.Fatalf("tick #%d advanced past the missing offset 2: committed set %v "+
				"(the broker watermark now covers an unprocessed offset: message loss)",
				tick, got)
		}
	}
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 1 {
		t.Errorf("offset 3 must stay buffered while 2 is missing, buffer holds %d item(s)",
			len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 2 {
		t.Errorf("CommittedPlusOne = %d, want 2 while the hole stands", tracker.CommittedPlusOne)
	}

	// the hole fills: 2 arrives, links to 1, and 3 links behind it
	committer.processCommit(makeOrphanItem(pool, partition, 2, 1, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) != 2 || got[1] != 3 {
		t.Errorf("filling the hole should advance the commit to 3, committed set %v", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 4 {
		t.Errorf("CommittedPlusOne = %d, want 4", tracker.CommittedPlusOne)
	}
}

// Test_StaleReadyDiscard_DoesNotStrandBufferedSuccessor: when the commit-time
// stale-Ready guard discards a below-baseline Ready, the completion for the
// current baseline may ALREADY be in the gap buffer (the new epoch's First
// record buffers when a stale Ready occupies the slot, after having raised
// CommittedPlusOne). Arriving completions can then never initialise Ready (the
// baseline offset has already arrived), so without re-initialisation from the
// buffer the partition stalls with the buffer growing unboundedly.
func Test_StaleReadyDiscard_DoesNotStrandBufferedSuccessor(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 150})
	committer.MarkPartitionAssigned(partition)

	// Simulate the corrupted state an unknown-baseline reset can allow: a stale
	// orphan occupying Ready below the baseline.
	committer.mu.Lock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	tracker.Ready = makeOrphanItem(pool, partition, 100, 99, false)

	// The new epoch's First record at the baseline offset: it re-affirms
	// CommittedPlusOne=150 but cannot displace the occupied Ready, so it buffers.
	committer.processCommit(makeOrphanItem(pool, partition, 150, 149, true), now)
	committer.mu.Unlock()

	// Commit tick: the stale guard discards Ready@100 without committing it.
	// State now: Ready=nil, CommittedPlusOne=150, and 150 is IN THE BUFFER.
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// Normal traffic continues: 151 arrives. It cannot initialise Ready
	// (151 != 150); the batch-end flush must re-initialise Ready with 150 from
	// the buffer and walk through 151.
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 151, 150, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()

	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 151 {
		t.Errorf("stranded-successor stall: offset 151 was never committed (committed set: %v); "+
			"the buffered baseline record can never initialise Ready and commits are stalled", got)
	}
	for _, offset := range got {
		if offset < 150 {
			t.Errorf("stale offset %d must never be committed (committed set: %v)", offset, got)
		}
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker = committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, still holds %d item(s)", len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 152 {
		t.Errorf("CommittedPlusOne = %d, want 152", tracker.CommittedPlusOne)
	}
}
