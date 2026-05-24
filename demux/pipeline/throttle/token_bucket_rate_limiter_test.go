// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package throttle

import (
	"runtime"
	"testing"
	"time"
)

func TestNewTokenBucket_ReturnsValidLimiter(t *testing.T) {
	const (
		ratePerSec = 1000
		burst      = 10
	)
	rateLimiter := NewTokenBucket[string](ratePerSec, burst)
	defer rateLimiter.Stop()

	var limiter = rateLimiter.(*TokenBucketRateLimiter[string])
	if limiter.ticker == nil {
		t.Fatal("expected ticker to be set")
	}
	if limiter.c == nil {
		t.Fatal("expected c channel to be set")
	}
	if limiter.cancel == nil {
		t.Fatal("expected cancel to be set")
	}
}

func TestNewTokenBucket_ImplementsRateLimiterInterface(t *testing.T) {
	var _ RateLimiter[string] = NewTokenBucket[string](1000, 5)
}

func TestNewTokenBucket_BurstAvailableImmediately(t *testing.T) {
	const (
		ratePerSec = 1
		burst      = 100
	)
	limiter := NewTokenBucket[string](ratePerSec, burst)
	defer limiter.Stop()

	start := time.Now()

	// should be able to consume all burst tokens immediately
	for i := 0; i < burst; i++ {
		done := make(chan struct{})
		go func() {
			limiter.Await(nil)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("burst token %d not immediately available", i)
		}
	}

	elapsed := time.Since(start)
	if elapsed > 5*time.Millisecond {
		t.Errorf("burst tokens should be immediate, took %v", elapsed)
	}
}

func TestNewTokenBucket_RefillsAfterBurstConsumed(t *testing.T) {
	const (
		ratePerSec = 100
		burst      = 2
	)

	limiter := NewTokenBucket[string](ratePerSec, burst)
	defer limiter.Stop()

	// consume burst
	for i := 0; i < burst; i++ {
		limiter.Await(nil)
	}

	// Next token should take ~10ms (one tick interval)
	start := time.Now()
	limiter.Await(nil)
	elapsed := time.Since(start)

	if elapsed < 5*time.Millisecond {
		t.Errorf("refill too fast: %v", elapsed)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("refill too slow: %v", elapsed)
	}
}

func TestNewTokenBucket_BurstOfOne(t *testing.T) {
	const (
		ratePerSec = 100
		burst      = 1
	)
	limiter := NewTokenBucket[string](ratePerSec, burst)
	defer limiter.Stop()

	// single burst token available immediately
	done := make(chan struct{})
	go func() {
		limiter.Await(nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Millisecond):
		t.Fatal("burst token should be immediately available")
	}

	// next requires waiting for tick
	start := time.Now()
	limiter.Await(nil)
	elapsed := time.Since(start)

	if elapsed < 5*time.Millisecond {
		t.Errorf("should wait for refill, only took %v", elapsed)
	}
}

func TestNewTokenBucket_PanicsWhenRatePerSecTooLow(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for ratePerSec < 1")
		}
	}()

	NewTokenBucket[string](0, 5)
}

func TestNewTokenBucket_PanicsWhenRatePerSecTooHigh(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for ratePerSec > 5000")
		}
	}()

	NewTokenBucket[string](5001, 5)
}

func TestNewTokenBucket_PanicsWhenBurstTooLow(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for burst < 1")
		}
	}()

	NewTokenBucket[string](100, 0)
}

func TestTokenBucket_Stop(t *testing.T) {
	limiter := NewTokenBucket[string](1000, 5)

	limiter.Stop()
	runtime.Gosched()
	// give goroutine time to exit
	time.Sleep(5 * time.Millisecond)

	// drain remaining burst tokens
	for i := 0; i < 5; i++ {
		done := make(chan struct{})
		go func() {
			limiter.Await(nil)
			close(done)
		}()
		select {
		case <-done:
		default:
			break
		}
	}

	runtime.Gosched()
	// give token-bucket time to fill if
	// it's still running - it shouldn't be
	time.Sleep(10 * time.Millisecond)

	// after stop and drain, Await should block indefinitely (no new tokens)
	awaitDone := make(chan struct{})
	go func() {
		limiter.Await(nil)
		close(awaitDone)
	}()

	select {
	case <-awaitDone:
		t.Fatal("expected no more tokens after Stop")
	case <-time.After(20 * time.Millisecond):
		// expected - Await blocks because no new tokens are produced
	}
}

func TestTokenBucket_GoroutineExitsOnStop(t *testing.T) {
	// Use fast rate so ticks happen frequently
	limiter := NewTokenBucket[string](1000, 3) // 1ms between ticks, burst of 3

	// Don't consume any tokens - bucket stays full (3 pre-filled)
	// Goroutine will try to send ticks but bucket is full

	// Let some ticks happen while bucket is full
	time.Sleep(10 * time.Millisecond)

	// Stop - goroutine should exit
	limiter.Stop()

	// Give goroutine time to exit
	time.Sleep(5 * time.Millisecond)

	// Drain all available tokens: 3 burst + up to 1 in-flight from goroutine.
	// The goroutine may have received a tick value from ticker.C before Stop()
	// and is blocked trying to send it to the full channel. Draining frees a
	// slot, letting that in-flight value through, then the goroutine exits.
	for i := 0; i < 4; i++ {
		done := make(chan struct{})
		go func() {
			limiter.Await(nil)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Millisecond):
			// no more tokens available
		}
	}

	// Verify no more tokens are produced (Await should block)
	awaitDone := make(chan struct{})
	go func() {
		limiter.Await(nil)
		close(awaitDone)
	}()

	select {
	case <-awaitDone:
		t.Fatal("goroutine should have exited, no new tokens expected")
	case <-time.After(20 * time.Millisecond):
		// expected - goroutine exited, no new tokens
	}
}

func TestCreateAndPreFillBurstChannel(t *testing.T) {
	tests := []struct {
		name  string
		burst int
	}{
		{"burst of 1", 1},
		{"burst of 5", 5},
		{"burst of 100", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := createAndPreFillBurstChannel(tt.burst)

			// verify capacity equals burst
			if cap(c) != tt.burst {
				t.Errorf("expected capacity %d, got %d", tt.burst, cap(c))
			}

			// verify channel is completely filled
			if len(c) != tt.burst {
				t.Errorf("expected %d tokens pre-filled, got %d", tt.burst, len(c))
			}

			// verify we can read exactly burst tokens without blocking
			for i := 0; i < tt.burst; i++ {
				select {
				case <-c:
				default:
					t.Fatalf("expected token %d to be available", i)
				}
			}

			// verify channel is now empty
			select {
			case <-c:
				t.Fatal("expected channel to be empty after draining")
			default:
				// expected
			}
		})
	}
}

func TestCreateAndPreFillBurstChannel_PanicsWhenBurstTooLow(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for burst < 1")
		}
	}()

	createAndPreFillBurstChannel(0)
}
