// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package ports

import "time"

// ProcessorPort abstracts pipeline.Processor[T] for testability.
//
// Satisfied by: *pipeline.Processor[T]
//
// Process is on the hot polling path - consumers should lift the method
// to a local variable at function entry to avoid interface dispatch overhead.
// ResetPrevOffsets is cold path (rebalance assign only).
type ProcessorPort[T any] interface {
	// Process routes a message payload to a concurrent per-key worker.
	// This is a blocking call that waits until a worker accepts the message.
	// Returns error if acquire-worker timeout expires (circuit breaker trigger).
	Process(payload T, readTime time.Time) error

	// ResetPrevOffsets clears offset tracking for the given partitions.
	// Called during rebalance assign so the committer knows sequence starts.
	ResetPrevOffsets(partitions []int32)
}
