// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package throttle

// NewTicker creates a steady-rate limiter with no burst capacity.
// Tokens are spaced evenly at ratePerSec, providing predictable,
// uniform pacing - a token bucket with a burst of 1.
// For burst tolerance, use NewTokenBucket directly.
//
// ratePerSec must be between 1 and 5000.
func NewTicker[T any](ratePerSec int) RateLimiter[T] {
	return NewTokenBucket[T](ratePerSec, 1)
}
