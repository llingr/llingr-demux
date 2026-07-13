// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package throttle

import "github.com/llingr/llingr-nexus/nexus"

// NoOpRateLimiter is the default when no rate limiting
// is configured.
//
// This is the RECOMMENDED default for most applications:
//
//	Use config.ConcurrentKeys as the primary means to
//	manage throughput and resource utilization.
type NoOpRateLimiter[T any] struct{}

// Await no-op
func (r *NoOpRateLimiter[T]) Await(_ *nexus.Message[T]) {
	// no-op
}

// Stop no-op
func (r *NoOpRateLimiter[T]) Stop() {
	// no-op
}
