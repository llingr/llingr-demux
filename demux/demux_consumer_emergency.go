// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package demux

import (
	"errors"
)

// EmergencyShutdown trips the circuit breaker: processing is cancelled and
// the registered shutdown callback receives the reason, exactly once across
// graceful and emergency exits. Safe from any goroutine, in any lifecycle
// state (before, during, or after Subscribe), repeatedly and re-entrantly;
// it never blocks beyond the trip's synchronous logging. It does NOT drain
// or commit in-flight work; callers wanting a graceful exit use Shutdown.
//
// This method (and not TriggerEmergencyShutdown, which predates it) is the
// one adapters and applications should assert for: its presence marks an
// engine whose emergency path guarantees the exactly-once notification
// above regardless of when the trip lands.
func (dxc *Consumer[T]) EmergencyShutdown(reason error) {
	dxc.circuitBreaker.TriggerEmergencyShutdown(reason)
}

// watchEmergency delivers the shutdown callback for emergency exits. Parked
// from Build (not from Subscribe), so a trip in ANY window - before Subscribe,
// while awaiting assignment, mid-drain - reaches the host; the payload
// channel is buffered, so a trip that precedes this goroutine's first poll
// is held, not lost. Exits on the graceful token instead, once a completed
// Shutdown has delivered the callback itself.
func (dxc *Consumer[T]) watchEmergency() {
	select {
	case msg := <-dxc.circuitBreaker.Triggered():
		if msg == "" {
			return
		}
		dxc.emergencyExit(errors.New(msg))

	case <-dxc.gracefulDone:
		// graceful shutdown completed without a trip; nothing to deliver
	}
}

// emergencyExit runs on the observer goroutine after a trip: it stops the
// rate limiter (idempotent alongside Shutdown's own call) and delivers the
// shutdown callback if the graceful path has not already claimed delivery.
// Broker release stays with its owners: an adapter that trips the breaker
// releases its own client; the drain path releases through unsubscribe.
func (dxc *Consumer[T]) emergencyExit(reason error) {
	dxc.stopRateLimit()

	// record the reason first: if no callback exists yet, a later Shutdown
	// delivers it (trippedReason) instead of reporting a graceful exit
	dxc.emergencyReason.Store(&reason)

	callback := dxc.shutdownCallback.Load()
	if callback == nil {
		// nothing registered and Subscribe never installed the default:
		// the trip has already surfaced through the Subscribe/Shutdown error
		// paths, and the recorded reason covers a later Shutdown delivery
		return
	}
	if dxc.claimNotify() {
		(*callback)(dxc.ctx, reason)
	}
}

// trippedReason returns the recorded emergency reason, or nil when no trip
// has been delivered to this consumer. Shutdown uses it so a callback
// registered only after a trip still hears the emergency, not a graceful nil.
func (dxc *Consumer[T]) trippedReason() error {
	if reason := dxc.emergencyReason.Load(); reason != nil {
		return *reason
	}
	return nil
}

// claimNotify elects the single deliverer of the shutdown callback. The
// cap-1 slot never blocks: the first claimant fills it, every later claim
// (a graceful completion racing an emergency, or the reverse) is refused.
// A consumer constructed without the builder has no slot and stays ungated,
// preserving the direct-invocation behaviour it always had.
func (dxc *Consumer[T]) claimNotify() bool {
	if dxc.notify == nil {
		return true
	}
	select {
	case dxc.notify <- struct{}{}:
		return true
	default:
		return false
	}
}

// signalGracefulDone releases the watchEmergency observer after a shutdown
// that completed without a trip. A cap-1 token send that never blocks and
// tolerates repeats; the observer consumes at most one.
func (dxc *Consumer[T]) signalGracefulDone() {
	if dxc.gracefulDone == nil {
		return
	}
	select {
	case dxc.gracefulDone <- struct{}{}:
	default:
	}
}
