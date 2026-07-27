// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// These tests pin the retrySend stall valve: a handler that never returns
// leaves its worker unable to drain, fills the per-key channel, and parks the
// dispatching goroutine in the retrySend loop. The valve must (a) trip the
// circuit breaker once the message has been undeliverable beyond the acquire
// timeout, (b) exit the loop when the breaker is tripped, whoever tripped it,
// and (c) release the dispatcher's guard token on exit. The stalled worker's
// own token stays pinned: Go cannot cancel a goroutine parked in host code.

// stallHarness blocks the first message of stallKey forever (until release is
// closed), fills the worker channel, then parks a dispatcher in retrySend.
// Returns once the dispatcher goroutine is spinning.
type stallHarness struct {
	h            *demuxTestHarness
	dmx          *Demux[string]
	release      chan struct{} // closed by cleanup to unstall the handler
	sendReturned chan struct{} // closed when the spinning dispatch returns
	unstallOnce  sync.Once     // unstall runs mid-test AND from t.Cleanup
}

func newStallHarness(t *testing.T, acquireTimeout time.Duration, spinnerUsesOverflow bool) *stallHarness {
	t.Helper()

	h := newDemuxTestHarness()
	h.cfg.PerKeyBufferLen = 2 // small buffer: stall + 2 buffered + 1 spinning
	h.cfg.AcquireWorkerTimeoutCircuitBreaker = acquireTimeout

	release := make(chan struct{})
	handlerEntered := make(chan struct{})
	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		if msg.Offset == 0 {
			close(handlerEntered)
			<-release // stalled: host code the engine cannot interrupt
		}
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedOffsets[msg.Key] = append(h.processedOffsets[msg.Key], msg.Offset)
		return nil
	}

	dmx := h.createDemux()
	const key = "stall-key"

	// worker takes offset 0 and stalls, holding one guard token for the
	// duration of the test
	h.guard <- struct{}{}
	go dmx.SendToWorkerForProcessing(key, h.createWorkItem(key, 0, 0))
	<-handlerEntered

	// fill the 2-slot channel; pipelined sends release their tokens on success
	for i := int64(1); i <= 2; i++ {
		h.guard <- struct{}{}
		go dmx.SendToWorkerForProcessing(key, h.createWorkItem(key, 0, i))
	}
	// both fill-sends have completed once their tokens come back and only the
	// stalled worker's remains outstanding
	deadline := time.Now().Add(2 * time.Second)
	for len(h.guard) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("fill-sends never completed: %d guard tokens outstanding, want 1", len(h.guard))
		}
		time.Sleep(time.Millisecond)
	}

	// the dispatcher: channel is full, so this send enters retrySend and spins
	workItem := h.createWorkItem(key, 0, 3)
	if spinnerUsesOverflow {
		h.overflowGuard <- struct{}{}
		nexus.SetUsedOverflow(&workItem.Metrics.Traits)
	} else {
		h.guard <- struct{}{}
	}
	sendReturned := make(chan struct{})
	go func() {
		dmx.SendToWorkerForProcessing(key, workItem)
		close(sendReturned)
	}()

	s := &stallHarness{h: h, dmx: dmx, release: release, sendReturned: sendReturned}
	t.Cleanup(s.unstall)
	return s
}

// unstall releases the handler so the worker drains and, on the unfixed code
// path, the spinning dispatcher completes its send; nothing leaks beyond the
// test regardless of outcome.
func (s *stallHarness) unstall() {
	s.unstallOnce.Do(func() {
		close(s.release)
		s.dmx.DrainWorkers()
	})
}

func TestSendToWorkerForProcessing_StalledWorker_TimeoutTripsCircuitBreaker(t *testing.T) {
	const acquireTimeout = 150 * time.Millisecond // short for testing
	s := newStallHarness(t, acquireTimeout, false)

	// the valve: a message undeliverable beyond the acquire timeout must trip
	// the breaker; nothing else in this test can trip it
	select {
	case <-s.h.circuitBreaker.Tripped():
	case <-time.After(2 * time.Second):
		t.Fatalf("stalled worker never tripped the circuit breaker: retrySend has no valve "+
			"(guard tokens outstanding: %d - the dispatcher's is lost to the spin, the stalled worker's to the handler)",
			len(s.h.guard))
	}

	if reason := s.h.circuitBreaker.Reason(); reason == nil || !strings.Contains(reason.Error(), "stall") {
		t.Errorf("trip reason = %v, want a worker stall reason", reason)
	}

	// the exit: the dispatcher must observe the trip and return
	select {
	case <-s.sendReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher never returned after the trip: retrySend does not observe the circuit breaker")
	}

	// the token: released on exit; only the stalled worker's remains pinned
	if got := len(s.h.guard); got != 1 {
		t.Errorf("guard tokens outstanding = %d, want 1 (dispatcher must release its token on exit)", got)
	}

	// the abandoned message is never processed by this instance: redelivery
	// after rebalance owns it. Unstall and drain, then check what ran.
	s.unstall()
	s.h.processedMu.Lock()
	defer s.h.processedMu.Unlock()
	for _, off := range s.h.processedOffsets["stall-key"] {
		if off == 3 {
			t.Error("abandoned message (offset 3) was processed; it must be left to redelivery")
		}
	}
}

func TestSendToWorkerForProcessing_StalledWorker_SpinObservesExternalTrip(t *testing.T) {
	// acquire timeout far beyond the test: the valve cannot self-arm, so the
	// ONLY exit is observing a trip raised elsewhere (the silent-stall rescue)
	s := newStallHarness(t, time.Hour, true)

	// give the dispatcher a moment to be parked in the retry loop
	time.Sleep(30 * time.Millisecond)

	s.h.circuitBreaker.TriggerEmergencyShutdown(errors.New("external emergency: host liveness probe"))

	select {
	case <-s.sendReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("retrySend survived an external emergency shutdown: the spin never observes the trip")
	}

	// the spinner held an overflow token: shared burst capacity must not stay
	// pinned by an exiting dispatcher
	if got := len(s.h.overflowGuard); got != 0 {
		t.Errorf("overflow tokens outstanding = %d, want 0 (dispatcher must release its overflow token on exit)", got)
	}
	if got := len(s.h.guard); got != 1 {
		t.Errorf("guard tokens outstanding = %d, want 1 (only the stalled worker's)", got)
	}
}
