// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"fmt"
	"time"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// repairSuccessor repairs predecessor linkage broken by log compaction across
// ownership epochs. A super-rare edge case, quarantined here so the ingest
// path never pays for it: this runs at commit cadence, only for a partition
// that looks stalled (Ready empty, items buffered, no flush pending), as the
// repair of last resort after the flush walk has had its chance.
//
// The walk advances on exact evidence only: an offset at the baseline, or a
// predecessor stamp naming the Ready record or the record committed this
// epoch. Within one ownership epoch that is always sufficient. Across epochs
// it can break. Example:
//
//	The broker holds records 7, 8, 12 (9-11 removed by compaction after a
//	previous assignment of this partition read them). This epoch commits 8.
//	A work item for offset 11 from that previous epoch, stamped with its
//	then-true predecessor 10, completes late (a worker stuck since before the
//	rebalance) and buffers. The epoch's real successor, offset 12 stamped
//	with predecessor 8, buffers behind it in sorted order. The walk stops at
//	11 (nothing names it), 12 is never reached, and no later arrival can ever
//	flag the tracker again: commits stop for the partition, every completion
//	joins the buffer, and the buffer grows without bound.
//
// The repair requires a record committed THIS epoch (LastCommittedOffset) as
// its anchor: with nothing committed yet there is no record to reason from (a
// reset-derived baseline is a position, not a record), and the baseline
// record itself may be validly in flight - promoting anything would lose it.
// One scan finds the LOWEST-offset item that provably holds the record
// immediately after the anchor. Proofs:
//
//  1. its offset is the baseline (CommittedPlusOne): the completion of the
//     exact record the broker position names, whatever its stamp claims (it
//     may be synthetic). Candidates below the baseline are never considered
//     and the baseline is never below committed+1, so this also carries the
//     offset-adjacency proof: nothing can exist between committed and
//     committed+1. A buffered First qualifies here and ONLY here: its
//     arrival already raised the baseline to its own offset, so a First
//     found ABOVE the baseline was overridden by a later, higher authority
//     (an assign that re-based the partition below it, or an operator
//     resetting the group offset backwards) and the offsets in between may
//     hold valid in-flight work - its flag is deliberately not honoured.
//  2. its predecessor stamp is at or below the committed record: delivery
//     evidence that the interval (stamp, offset) held no records when the
//     item was fetched, whichever epoch fetched it - records append in
//     offset order and compaction/retention only remove, so the interval
//     holds none now. At or below rather than exact, because committing one
//     repaired item moves the anchor past the stamp the next one carries.
//
// Promoting the provable successor skips nothing that exists, and by the same
// proof every buffered item below it sits inside an interval that holds no
// records: provably stale, pruned as orphaned. An item that merely awaits an
// in-flight predecessor satisfies no proof and is left untouched - its stamp
// names an offset above the committed record, so it can never be mistaken for
// a successor however many ticks fire. With no provable successor at all this
// is a no-op: a genuine hole, and the committer must stall until it fills
// (lifting past it would be loss, not duplicates).
func (oc *Committer[T]) repairSuccessor(partition int32, tracker *OffsetsTracker[T]) {
	switch {
	case tracker.Ready != nil:
		// a commit candidate is held: the tick will commit it, no blockage
		return
	case len(tracker.GapBuffer) == 0:
		// nothing buffered: nothing to repair
		return
	case tracker.NeedsGapAdvance:
		// a flush is pending: the exact walk has first right of refusal
		return
	case tracker.LastCommittedOffset < 0:
		// nothing committed this epoch: no record to reason from, and the
		// baseline record itself may be validly in flight
		return
	}

	committed := tracker.LastCommittedOffset
	var successor *ports.WorkItem[T]
	for _, g := range tracker.GapBuffer {
		switch {
		case g.Message.Offset < tracker.CommittedPlusOne:
			// below the baseline: never a successor (committing it would move
			// the broker position backwards); pruned below if one is found
		case successor != nil && g.Message.Offset >= successor.Message.Offset:
			// a lower-offset successor is already chosen
		case g.Message.Offset == tracker.CommittedPlusOne:
			// positional proof: the completion of the exact record the broker
			// position names, whatever its stamp claims
			successor = g
		case g.PreviousOffset >= 0 && g.PreviousOffset <= committed:
			// stamp proof: the interval (stamp, offset) held no records when g
			// was fetched, so nothing exists between the committed record and g
			successor = g
		}
	}
	if successor == nil {
		return
	}

	now := time.Now()
	pruned := 0
	kept := tracker.GapBuffer[:0]
	for _, g := range tracker.GapBuffer {
		switch {
		case g == successor:
			// promoted below
		case g.Message.Offset < successor.Message.Offset:
			nexus.SetOrphaned(&g.Metrics.Traits)
			g.Metrics.WatermarkAdvanceTime = now
			oc.returnMessageAndCollectMetrics(g)
			pruned++
		default:
			kept = append(kept, g)
		}
	}
	tracker.GapBuffer = kept

	tracker.Ready = successor
	successor.Metrics.WatermarkAdvanceTime = now
	// anything still buffered may chain onto the repaired record: let the
	// next flush walk it with the normal exact rules
	tracker.NeedsGapAdvance = len(tracker.GapBuffer) > 0

	oc.logger.Warn(oc.ctx, fmt.Sprintf(
		"repaired successor on partition %d: offset %d (predecessor stamp %d) provably follows "+
			"the last committed record %d - the records in between were removed by log compaction "+
			"or retention, or the completion was stamped in a previous ownership epoch; pruned %d "+
			"provably stale buffered item(s); without repair the partition would stall and its "+
			"gap buffer would grow without bound",
		partition, successor.Message.Offset, successor.PreviousOffset, committed, pruned))
}
