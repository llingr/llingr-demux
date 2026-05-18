// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package throttle

import (
	"context"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// TokenBucketRateLimiter provides rate limiting
// with average rate and burst capacity.
//
// This optional feature is not normally required
// as it is almost always better to throttle by
// configuring concurrency, see: config.ConcurrentKeys
type TokenBucketRateLimiter[T any] struct {
	ticker *time.Ticker
	c      <-chan time.Time
	cancel context.CancelFunc
}

// NewTokenBucket creates a token bucket rate limiter that allows bursting
// up to 'burst' tokens immediately, then refills at ratePerSec.
// The bucket starts full.
//
// ratePerSec must be between 1 and 5000.
// burst must be >= 1.
func NewTokenBucket[T any](ratePerSec int, burst int) RateLimiter[T] {
	validateRate(ratePerSec)

	ctx, cancel := context.WithCancel(context.Background())

	tickInterval := time.Second / time.Duration(ratePerSec)
	ticker := time.NewTicker(tickInterval)

	c := createAndPreFillBurstChannel(burst)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case c <- <-ticker.C:
			}
		}
	}()

	return &TokenBucketRateLimiter[T]{
		ticker: ticker,
		c:      c,
		cancel: cancel,
	}
}

// Await blocks until a token is available, consuming it
func (r *TokenBucketRateLimiter[T]) Await(_ *nexus.Message[T]) {
	<-r.c
}

// Stop releases resources: once this has been
// called, the TokenBucketRateLimiter should no longer be used
func (r *TokenBucketRateLimiter[T]) Stop() {
	r.cancel()
	r.ticker.Stop()
}

// createAndPreFillBurstChannel for token-bucket rate-limiter
func createAndPreFillBurstChannel(burst int) chan time.Time {
	if burst < 1 {
		panic("burst must be >= 1 for TokenBucket")
	}
	c := make(chan time.Time, burst)
	now := time.Now()
	for {
		select {
		case c <- now:
		default:
			return c
		}
	}
}
