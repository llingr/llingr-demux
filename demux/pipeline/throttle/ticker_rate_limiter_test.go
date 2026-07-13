// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package throttle

import (
	"testing"
	"time"
)

func TestNewTicker_ReturnsValidLimiter(t *testing.T) {
	limiter := NewTicker[string](1000)
	defer limiter.Stop()

	// Await should return within ~1ms (rate 1000/s)
	done := make(chan struct{})
	go func() {
		limiter.Await(nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Millisecond):
		t.Fatal("Await did not return in time")
	}
}

func TestNewTicker_TokensArriveAtExpectedRate(t *testing.T) {
	rate := 100 // 100 per second = 10ms interval
	limiter := NewTicker[string](rate)
	defer limiter.Stop()

	start := time.Now()
	limiter.Await(nil)
	limiter.Await(nil)
	elapsed := time.Since(start)

	// Should take approximately 10ms (one interval) but less than 30ms
	// Note: ticker starts at NewTicker(), not at start := time.Now(),
	// so we allow a small tolerance for the setup time between them
	if elapsed < 9*time.Millisecond {
		t.Errorf("tokens arrived too fast: %v", elapsed)
	}
	if elapsed > 30*time.Millisecond {
		t.Errorf("tokens arrived too slow: %v", elapsed)
	}
}

func TestNewTicker_PanicsWhenRatePerSecTooLow(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for ratePerSec < 1")
		}
	}()

	NewTicker[string](0)
}

func TestNewTicker_PanicsWhenRatePerSecTooHigh(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for ratePerSec > 5000")
		}
	}()

	NewTicker[string](5001)
}

func TestNewTicker_BoundaryRates(t *testing.T) {
	t.Run("MinRate", func(t *testing.T) {
		limiter := NewTicker[string](1)
		defer limiter.Stop()
		// Should not panic, 1 is valid
	})

	t.Run("MaxRate", func(t *testing.T) {
		limiter := NewTicker[string](5000)
		defer limiter.Stop()
		// Should not panic, 5000 is valid
	})
}

func TestTicker_Stop(t *testing.T) {
	limiter := NewTicker[string](1000)
	limiter.Stop()
	// Verify no panic and method completes
}
