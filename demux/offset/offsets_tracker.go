// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

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
}

// HasPendingCommits returns true if there are
// uncommitted message(s) awaiting commit processing
func (oc *OffsetsTracker[T]) HasPendingCommits() bool {
	return oc.Ready != nil || len(oc.GapBuffer) > 0
}
