// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package ports

// CommitterPort abstracts offset.Committer[T] for testability.
//
// Satisfied by: *offset.Committer[T]
//
// CollectAndCommit is on the hot processing path - the Demux constructor lifts
// this method to a function variable passed to workers, avoiding interface
// dispatch overhead in the per-message commit flow.
type CommitterPort[T any] interface {
	// CollectAndCommit receives processed work items for offset tracking.
	// Called by workers after ProcessMessage completes (success or dead-letter).
	// Non-blocking: timestamps arrival and forwards to streaming watermark algorithm.
	CollectAndCommit(workItem *WorkItem[T])
}
