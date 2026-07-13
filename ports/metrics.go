// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package ports

// MetricsPort abstracts metrics.Collector[T] for testability.
//
// Satisfied by: *metrics.Collector[T]
//
// Collect is on the hot commit path - the Committer constructor lifts this
// method to a function variable, avoiding interface dispatch overhead in
// the per-message metrics flow.
type MetricsPort[T any] interface {
	// Collect captures work item metrics without blocking.
	// Called after watermark advancement when the work item lifecycle completes.
	// Safe for hot paths: drops metrics on overflow rather than blocking.
	Collect(workItem *WorkItem[T])
}
