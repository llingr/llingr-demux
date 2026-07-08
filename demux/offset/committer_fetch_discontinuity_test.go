// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"testing"
	"time"
)

// These tests pin the committer against fetch-position discontinuities that
// happen WITHOUT a rebalance (offset out of range after log truncation or an
// unclean leader election resets the position backward; consumer lag exceeding
// retention resets it forward). The two directions differ at the poll boundary:
// a BACKWARD jump violates ascending-only delivery and trips the circuit
// breaker there (prev.GetPrevious returns ErrOffsetRegression, the poll loop
// escalates), so the committer never sees it in production; a FORWARD jump is
// legitimate ascending delivery and flows through.

// Test_FetchResetBelowBaseline_DiscardsWithoutStallOrRegression: defence in
// depth BEHIND the poll-boundary ascending-only validation. Should
// below-baseline completions ever reach the committer anyway, each must be
// discarded on arrival (never committed, never buffered), and commits must
// resume once the stream passes the baseline again.
func Test_FetchResetBelowBaseline_DiscardsWithoutStallOrRegression(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 150})
	committer.MarkPartitionAssigned(partition)

	// real progress: 150 (First), 151, 152 complete and commit; CPO = 153
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 150, 149, true), now)
	committer.processCommit(makeOrphanItem(pool, partition, 151, 150, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 152, 151, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// below-baseline completions with stale linkage, injected directly (the
	// poll boundary would reject this delivery pattern; see the file header)
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 100, 152, false), now)
	for offset := int64(101); offset <= 152; offset++ {
		committer.processCommit(makeOrphanItem(pool, partition, offset, offset-1, false), now)
	}
	committer.flushGapBuffers(now)

	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("below-baseline redeliveries must be discarded, not buffered: %d item(s) buffered",
			len(tracker.GapBuffer))
	}
	if tracker.CommittedPlusOne != 153 {
		t.Errorf("baseline moved during the below-baseline burst: CommittedPlusOne = %d, want 153",
			tracker.CommittedPlusOne)
	}
	committer.mu.Unlock()

	// the stream catches up past the baseline: commits must resume
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 153, 152, false), now)
	committer.processCommit(makeOrphanItem(pool, partition, 154, 153, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 154 {
		t.Errorf("commits did not resume after the stream passed the baseline (committed set: %v)", got)
	}
	previous := int64(-1)
	for _, offset := range got {
		if offset < previous {
			t.Errorf("commit regression in %v", got)
		}
		if offset >= 100 && offset < 152 {
			t.Errorf("a below-baseline redelivery was committed: %d in %v", offset, got)
		}
		previous = offset
	}
}

// Test_RetentionJumpForward_CommitsThrough: lag beyond retention resets the
// fetch position to the new log start; offsets jump forward by millions with
// nothing in between and no rebalance. Predecessor linkage (not offset+1
// arithmetic) must carry commits across the jump, including the nastier
// commit-boundary case where Ready was just consumed.
func Test_RetentionJumpForward_CommitsThrough(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	now := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 1000})
	committer.MarkPartitionAssigned(partition)

	// progress to 1001, committed: CPO = 1002, Ready consumed
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 1000, 999, true), now)
	committer.processCommit(makeOrphanItem(pool, partition, 1001, 1000, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// retention jump: next delivered record is 5,000,000 with predecessor 1001
	// (the last delivered offset). It cannot initialise Ready on arrival
	// (5M != 1002); the batch-end flush must re-initialise Ready from the
	// buffer via predecessor linkage.
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 5_000_000, 1001, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	// stream continues after the jump
	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 5_000_001, 5_000_000, false), now)
	committer.flushGapBuffers(now)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 5_000_001 {
		t.Errorf("retention-jump stall: commits did not carry across the forward jump (committed set: %v)",
			got)
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if tracker.CommittedPlusOne != 5_000_002 {
		t.Errorf("CommittedPlusOne = %d, want 5000002", tracker.CommittedPlusOne)
	}
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
}
