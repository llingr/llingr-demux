// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// Test_DrainCommitter_HandshakeCommitsBeforeReturn pins DrainCommitter's
// contract against the live ingest goroutine: everything enqueued before the
// drain is committed by the time it returns, whichever side of the handshake
// wins (backlog found -> drained signal wait; channel already empty -> grace
// period + mutex barrier). 200 iterations squeeze the enqueue/drain race from
// both sides.
func Test_DrainCommitter_HandshakeCommitsBeforeReturn(t *testing.T) {
	var mu sync.Mutex
	committedNext := make(map[int32]int64)
	commitFn := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range msgs {
			next := message.Offset + 1
			if previous, ok := committedNext[message.Partition]; ok && next < previous {
				t.Errorf("commit monotonicity violated: partition %d committed offset moved backwards: %d -> %d",
					message.Partition, previous-1, message.Offset)
				continue
			}
			committedNext[message.Partition] = next
		}
		return msgs, nil
	}

	committer, pool, cancel := newOrphanTestCommitter(t, commitFn, nil)
	defer cancel()

	iterations := 200
	if testing.Short() {
		iterations = 50
	}
	const batchSize = 25

	for iteration := 0; iteration < iterations; iteration++ {
		partition := int32(iteration) // fresh partition per iteration
		committer.MarkPartitionAssigned(partition)

		base := int64(iteration % 7) // streams need not start at 0
		for k := 0; k < batchSize; k++ {
			offset := base + int64(k)
			committer.CollectAndCommit(makeOrphanItem(pool, partition, offset, offset-1, k == 0))
		}

		if err := committer.DrainCommitter(time.NewTimer(5 * time.Second)); err != nil {
			t.Fatalf("iteration %d: DrainCommitter returned: %v", iteration, err)
		}

		want := base + batchSize
		mu.Lock()
		got := committedNext[partition]
		mu.Unlock()
		if got != want {
			t.Fatalf("iteration %d: DrainCommitter returned before the batch committed: next=%d, want %d",
				iteration, got, want)
		}

		committer.mu.Lock()
		tracker := committer.offsetsByPartition.PartitionMap[partition]
		if tracker.CommittedPlusOne != want {
			t.Errorf("iteration %d: CommittedPlusOne=%d after drain, want %d",
				iteration, tracker.CommittedPlusOne, want)
		}
		committer.mu.Unlock()
	}
}
