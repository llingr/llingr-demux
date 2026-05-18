// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package throttle

import (
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

func TestNoOpRateLimiter_ImplementsRateLimiterInterface(t *testing.T) {
	var _ RateLimiter[string] = &NoOpRateLimiter[string]{}
}

func TestNoOpRateLimiter_StopDoesNotPanic(t *testing.T) {
	limiter := &NoOpRateLimiter[string]{}
	limiter.Stop()
	limiter.Stop() // idempotent
}

func TestNoOpRateLimiter_AwaitReturnsImmediately(t *testing.T) {
	limiter := &NoOpRateLimiter[string]{}

	const messages = 1000
	start := time.Now()
	for i := 0; i < messages; i++ {
		limiter.Await(&nexus.Message[string]{
			Partition: int32(i % 12),
			Offset:    int64(i),
			Key:       "key",
		})
	}
	elapsed := time.Since(start)

	perMessage := elapsed / messages
	if perMessage >= time.Microsecond {
		t.Errorf("expected < 1µs per message, got %v (%v total for %d messages)",
			perMessage, elapsed, messages)
	}
}

func TestNoOpRateLimiter_AwaitNilMessage(t *testing.T) {
	limiter := &NoOpRateLimiter[string]{}
	limiter.Await(nil) // must not panic
}

func TestNoOpRateLimiter_AwaitAfterStop(t *testing.T) {
	limiter := &NoOpRateLimiter[string]{}
	limiter.Stop()
	limiter.Await(&nexus.Message[string]{Key: "key"}) // must not panic
}

func TestNoOpRateLimiter_ConcurrentAwait(t *testing.T) {
	limiter := &NoOpRateLimiter[string]{}

	const goroutines = 100
	const messagesPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(partition int32) {
			defer wg.Done()
			for i := 0; i < messagesPerGoroutine; i++ {
				limiter.Await(&nexus.Message[string]{
					Partition: partition,
					Offset:    int64(i),
					Key:       "key",
				})
			}
		}(int32(g))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent Await did not complete within 1s")
	}
}

func TestNoOpRateLimiter_ZeroAllocations(t *testing.T) {
	limiter := &NoOpRateLimiter[string]{}
	msg := &nexus.Message[string]{Key: "key"}

	allocs := testing.AllocsPerRun(1000, func() {
		limiter.Await(msg)
	})
	if allocs != 0 {
		t.Errorf("expected 0 allocations, got %v", allocs)
	}
}
