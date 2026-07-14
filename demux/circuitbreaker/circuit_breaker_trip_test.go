// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/tests/mocklogger"
)

// A trigger arriving from INSIDE the winner's own logging (a host log handler
// calling back into the engine) must be a no-op, not a deadlock: the election
// is decided before any log runs, so the re-entrant call returns immediately.
// Reaching the assertions at all proves there is no self-deadlock.
func Test_TriggerEmergencyShutdown_ReentrantFromLogHandler_NoDeadlock(t *testing.T) {
	ctx := context.Background()

	var cb *CircuitBreaker
	reentered := false
	logger := &orderTrackingLogger{
		onError: func(_ string) {
			// the log handler re-enters the trigger, as a host handler might
			if !reentered {
				reentered = true
				cb.TriggerEmergencyShutdown(errors.New("re-entrant"))
			}
		},
	}
	cb = New(ctx, logger)

	done := make(chan struct{})
	go func() {
		cb.TriggerEmergencyShutdown(errors.New("first"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("re-entrant trigger deadlocked")
	}

	if !reentered {
		t.Fatal("test premise broken: log handler never re-entered")
	}

	// exactly one trip: one message, and the reason is the FIRST call's
	msg := <-cb.Triggered()
	expected := fmt.Sprintf(signalMessage, "first")
	if msg != expected {
		t.Errorf("message mismatch:\ngot:  %q\nwant: %q", msg, expected)
	}
}

// The latch is open until the trip and closed after it; reading it consumes
// nothing, so any number of observers see it, before or after the trip.
func Test_Tripped_LatchObservableByManyReaders(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	select {
	case <-cb.Tripped():
		t.Fatal("latch closed before any trip")
	default:
	}
	if cb.Reason() != nil {
		t.Fatalf("Reason() = %v before any trip, want nil", cb.Reason())
	}

	tripErr := errors.New("boom")
	cb.TriggerEmergencyShutdown(tripErr)

	for i := 0; i < 3; i++ {
		select {
		case <-cb.Tripped():
			// non-consuming: every read observes the closed latch
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("observer %d did not see the closed latch", i)
		}
	}
	if !errors.Is(cb.Reason(), tripErr) {
		t.Errorf("Reason() = %v, want the tripping error", cb.Reason())
	}
}

// Concurrent trips elect exactly one winner: one latch close, one payload
// message, one log set, and the recorded reason belongs to the winner.
// Run under -race to exercise the election and reason publication.
func Test_TriggerEmergencyShutdown_ConcurrentTrips_OneWinner(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			cb.TriggerEmergencyShutdown(fmt.Errorf("error %d", n))
		}(i)
	}
	wg.Wait()

	messageCount := 0
	var msg string
	for m := range cb.Triggered() {
		messageCount++
		msg = m
	}
	if messageCount != 1 {
		t.Fatalf("expected exactly 1 message, got %d", messageCount)
	}

	// the recorded reason and the payload message agree on the winner
	expected := fmt.Sprintf(signalMessage, cb.Reason())
	if msg != expected {
		t.Errorf("payload/reason mismatch:\ngot:  %q\nwant: %q", msg, expected)
	}
	if len(logger.Errors()) != 1 {
		t.Errorf("expected 1 error log, got %d", len(logger.Errors()))
	}
}

// The reason must be readable the instant the latch is observed closed: the
// winner writes it before close(tripped). A racing poller would catch a torn
// order (close before write) under the race detector.
func Test_Tripped_ReasonVisibleWhenLatchCloses(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	got := make(chan error, 1)
	go func() {
		<-cb.Tripped()
		got <- cb.Reason()
	}()

	tripErr := errors.New("ordering probe")
	cb.TriggerEmergencyShutdown(tripErr)

	select {
	case r := <-got:
		if !errors.Is(r, tripErr) {
			t.Errorf("Reason() = %v at latch close, want the trip error", r)
		}
	case <-time.After(time.Second):
		t.Fatal("latch never observed closed")
	}
}

// A nil reason is normalised, so downstream formatting (logs, the payload
// message, wrapped Subscribe errors) never renders a nil error.
func Test_TriggerEmergencyShutdown_NilReasonNormalised(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	cb.TriggerEmergencyShutdown(nil)

	if cb.Reason() == nil {
		t.Fatal("Reason() = nil after a nil-reason trip, want a normalised error")
	}
	msg := <-cb.Triggered()
	if strings.Contains(msg, "<nil>") {
		t.Errorf("payload rendered a nil error: %q", msg)
	}
}
