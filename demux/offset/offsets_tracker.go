// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// OffsetsTracker manages offset commit 'high-watermark' for a partition,
// caching out-of-order and/or non-contiguous messages in the GapBuffer
type OffsetsTracker[T any] struct {
	Ready           *ports.WorkItem[T]  // the next known WorkItem to commit
	Assignment      nexus.RebalanceType // status after last rebalance event: Assigned or Revoked
	MinOffsetSeen   int64
	MaxOffsetSeen   int64
	GapBuffer       []*ports.WorkItem[T] // cached out of order and/or non-contiguous offsets
	NeedsGapAdvance bool                 // deferred gap buffer sort+walk flag, flushed at batch end

	// CommittedPlusOne is the next offset expected after the last broker commit. Set from
	// RebalanceInfo during partition assign. Guards against stale messages from abandoned
	// workers after a drain timeout during rebalance.
	CommittedPlusOne int64 // see DESIGN.md for full explanation

	// LastCommittedOffset is the offset of the last record COMMITTED THIS EPOCH,
	// or -1 when none has been. It is the only anchor predecessor linkage may
	// promote against in the flush walk: CommittedPlusOne is a position, not a
	// record (a reset-derived baseline's offset-1 need not exist after
	// compaction), so deriving the anchor arithmetically from it would promote
	// stale items on coincidence.
	LastCommittedOffset int64
}

// HasPendingCommits returns true if there are
// uncommitted message(s) awaiting commit processing
func (oc *OffsetsTracker[T]) HasPendingCommits() bool {
	return oc.Ready != nil || len(oc.GapBuffer) > 0
}
