// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"testing"
	"time"
)

// These tests pin cross-epoch duplicate handling in the gap buffer dedupe.
// Layer 1 (prev.PartitionOffsets) makes a same-offset duplicate impossible
// within one fetch stream, so twins are always cross-epoch: a drain-timeout
// orphan completing late and the new epoch's redelivery of the same record.
// When compaction changed the record's neighbourhood between the epochs, the
// twins carry DIFFERENT predecessor stamps and are not interchangeable: the
// stale stamp satisfies no successor proof, so keeping it stalls the
// partition permanently (the walk and the repair are both blind to it, and
// the real completion's evidence is gone). The dedupe must keep the twin read
// LATER: the current epoch's poll strictly postdates the stale epoch's.
//
// Contract basis (documented, version-stable): SortFunc is "not guaranteed to
// be stable", so equal-offset ordering must never be relied on - the ReadTime
// tiebreak leaves no equal elements to order. CompactFunc "keeps the first
// one" of each equal run and zeroes the dropped tail for the GC.

// Test_DedupeKeepsLaterReadTwin: the deterministic unit pin, both insertion
// orders. The survivor of a same-offset run must always be the later-read
// twin, whatever order the completions arrived in.
func Test_DedupeKeepsLaterReadTwin(t *testing.T) {
	commitFn, _ := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	base := time.Now()

	for _, staleFirst := range []bool{true, false} {
		committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
		committer.mu.Lock()
		tracker := committer.offsetsByPartition.PartitionMap[partition]
		tracker.GapBuffer = tracker.GapBuffer[:0]

		stale := makeOrphanItem(pool, partition, 12, 11, false)
		stale.Metrics.ReadTime = base
		real := makeOrphanItem(pool, partition, 12, 8, false)
		real.Metrics.ReadTime = base.Add(time.Second)

		if staleFirst {
			tracker.GapBuffer = append(tracker.GapBuffer, stale, real)
		} else {
			tracker.GapBuffer = append(tracker.GapBuffer, real, stale)
		}

		committer.sortGapBuffer(tracker)

		if len(tracker.GapBuffer) != 1 {
			committer.mu.Unlock()
			t.Fatalf("staleFirst=%v: dedupe should leave one twin, holds %d", staleFirst,
				len(tracker.GapBuffer))
		}
		if survivor := tracker.GapBuffer[0]; survivor.PreviousOffset != 8 {
			t.Errorf("staleFirst=%v: dedupe kept the STALE twin (predecessor stamp %d, want 8); "+
				"its stamp satisfies no successor proof and the partition stalls permanently",
				staleFirst, survivor.PreviousOffset)
		}
		committer.mu.Unlock()
	}
}

// Test_DedupeStaleTwinDoesNotStallChain: the end-to-end consequence. The
// stale twin arrives first (a late orphan), the current epoch's redelivery
// arrives second, and the chain continues behind them. Whichever twin the
// unstable sort would have favoured, the chain must commit; keeping the stale
// twin strands the offset forever with the buffer growing behind it.
func Test_DedupeStaleTwinDoesNotStallChain(t *testing.T) {
	commitFn, committedOffsets := capturingCommit()
	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	partition := int32(0)
	base := time.Now()

	committer.ResetCommittedOffsets(map[int32]int64{partition: 8})
	committer.MarkPartitionAssigned(partition)

	committer.mu.Lock()
	committer.processCommit(makeOrphanItem(pool, partition, 8, 7, true), base)
	committer.mu.Unlock()
	if err := committer.CommitOffsets(); err != nil {
		t.Fatalf("CommitOffsets: %v", err)
	}

	committer.mu.Lock()
	// the stale twin: an orphan from the previous epoch, stamped when 9-11
	// still existed, read earlier
	stale := makeOrphanItem(pool, partition, 12, 11, false)
	stale.Metrics.ReadTime = base
	committer.processCommit(stale, base)
	// the current epoch's redelivery of 12: records 9-11 compacted since, so
	// it is stamped against the committed record 8, and read later
	real := makeOrphanItem(pool, partition, 12, 8, false)
	real.Metrics.ReadTime = base.Add(time.Second)
	committer.processCommit(real, base)
	// the chain continues behind the twins
	next := makeOrphanItem(pool, partition, 13, 12, false)
	next.Metrics.ReadTime = base.Add(2 * time.Second)
	committer.processCommit(next, base)
	committer.flushGapBuffers(base)
	committer.mu.Unlock()

	for tick := 1; tick <= 2; tick++ {
		if err := committer.CommitOffsets(); err != nil {
			t.Fatalf("CommitOffsets (tick #%d): %v", tick, err)
		}
	}

	got := committedOffsets()
	if len(got) == 0 || got[len(got)-1] != 13 {
		t.Errorf("stale-twin stall: chain never committed through the duplicate "+
			"(committed set: %v, want tail 13)", got)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	tracker := committer.offsetsByPartition.PartitionMap[partition]
	if len(tracker.GapBuffer) != 0 {
		t.Errorf("gap buffer should be empty, holds %d item(s)", len(tracker.GapBuffer))
	}
}
