// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package ports

// DrainCoordinatorPort abstracts drain.Coordinator[T] for testability.
//
// Satisfied by: *drain.Coordinator[T]
//
// Used during rebalance and shutdown to ensure all in-flight work
// completes and offsets are committed before partition handoff.
// This is always cold path (rebalance/shutdown only).
type DrainCoordinatorPort interface {
	// Drain waits for all in-flight workers to complete processing,
	// then drains and commits pending offsets.
	// Returns error on timeout or commit failure, which triggers circuit breaker.
	Drain() error
}
