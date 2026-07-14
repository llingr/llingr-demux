// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package circuitbreaker provides emergency shutdown coordination for when severe application
// or infrastructure failures threaten message processing reliability.
//
// The [CircuitBreaker] triggers when the system enters an irrecoverable state. This
// is typically when dead-letter writes fail (a proxy indicator that infrastructure is
// very likely to be broken) or when application deadlocks occur - less immediate but
// necessary to prevent systems silently locking up indefinitely.
//
// This is a safety valve based on the idea that it is better to fail fast and let the broker
// reassign partitions to healthy consumers than to continue operating in a degraded state. When
// systems are healthy, duplicates are rare; however, if the circuit-breaker trips, duplicates
// may occur during recovery.
package circuitbreaker

import (
	"context"
	"errors"
	"fmt"

	"github.com/llingr/llingr-nexus/nexus"
)

// CircuitBreaker coordinates emergency shutdown when external issues
// in runtime infrastructure or app errors (e.g. failed dead-letters)
// threaten system reliability.
type CircuitBreaker struct {
	mainCtx           context.Context        // cancels processing + hot path wrapped in DemuxAndProcess
	mainCtxDone       func() <-chan struct{} // cached Done() for hot path performance
	mainCancelFunc    context.CancelFunc
	logger            nexus.Logger
	ctx               context.Context // for logging (won't be cancelled)
	emergencyShutdown chan string     // advise host app
	// trip elects the single caller that drives the shutdown: the first send
	// fills the buffer and every later call (concurrent OR re-entrant on the
	// same goroutine) takes the select default and returns immediately. A
	// sync.Once cannot provide this: Do holds a mutex across f, so a trigger
	// re-entering from inside the winner's own logging (a host log handler
	// calling back into the engine) would self-deadlock.
	trip chan error
	// tripped is a latch, closed exactly once by the winner AFTER reason is
	// set and processing is cancelled. Unlike the single-consume
	// emergencyShutdown payload channel, a closed latch is observable by any
	// number of readers at any later time.
	tripped chan struct{}
	reason  error // written by the winner before close(tripped)
}

// New circuit breaker that coordinates emergency
// shutdown across all messages in worker pipelines
//
//nolint:contextcheck // mainCtx is intentionally independent - circuit breaker cancels workers without affecting globalCtx
func New(globalCtx context.Context, logger nexus.Logger) *CircuitBreaker {
	mainCtx, mainCancelFunc := context.WithCancel(context.Background())

	return &CircuitBreaker{
		mainCtx:           mainCtx, // for circuit-breaker
		mainCtxDone:       mainCtx.Done,
		mainCancelFunc:    mainCancelFunc, // attached to mainCtx
		ctx:               globalCtx,      // must not be cancelled
		logger:            logger,
		emergencyShutdown: make(chan string, 1),
		trip:              make(chan error, 1),
		tripped:           make(chan struct{}),
	}
}

// MainCtxDone returns a channel that closes when the circuit breaker triggers.
func (cb *CircuitBreaker) MainCtxDone() <-chan struct{} {
	return cb.mainCtxDone()
}

const (
	prefix            = "circuit-breaker: "
	shutdownInitiated = prefix + "protective shutdown initiated, reason: %s"
	contextsCancelled = prefix + "ProcessMessage contexts cancelled, stopping polling"
	signalMessage     = prefix + "triggered and completed protective shutdown, reason: %s"
	shutdownComplete  = prefix + "shutdown complete"
)

// TriggerEmergencyShutdown initiates protective shutdown to arrest cascading
// failures, mitigate resource exhaustion and protect overall system stability.
//
// Safe to call from any goroutine, in any lifecycle state, repeatedly and
// re-entrantly (including from inside a log handler the winner's own logging
// reaches); only the first call has effect, and no call ever blocks beyond
// the winner's synchronous logging. Processing is cancelled before anything
// can observe the trip, so the trip is in effect when the winning call
// returns even if a log handler stalls.
func (cb *CircuitBreaker) TriggerEmergencyShutdown(reason error) {
	if reason == nil {
		reason = errors.New("unspecified emergency shutdown")
	}

	select {
	case cb.trip <- reason:
		// won the election: the only caller that runs the body below
	default:
		return
	}

	cb.reason = reason

	// stop polling loop and prevent new messages from being processed
	cb.mainCancelFunc()
	close(cb.tripped)

	cb.logger.Error(cb.ctx, fmt.Sprintf(shutdownInitiated, reason))
	cb.logger.Warn(cb.ctx, contextsCancelled)

	// application must register callback,
	// simpler implementations might use os.Exit(1)
	cb.emergencyShutdown <- fmt.Sprintf(signalMessage, reason)
	close(cb.emergencyShutdown)
	cb.logger.Info(cb.ctx, shutdownComplete)
}

// Triggered callback channel advises if
// and why the circuit-breaker was closed
func (cb *CircuitBreaker) Triggered() <-chan string {
	return cb.emergencyShutdown
}

// Tripped returns a latch that is closed once the circuit breaker has
// triggered. Unlike Triggered, reading it consumes nothing, so any number
// of observers may select on it, before or after the trip.
func (cb *CircuitBreaker) Tripped() <-chan struct{} {
	return cb.tripped
}

// Reason returns the error that tripped the circuit breaker, or nil while
// it has not tripped. The value is stable once Tripped is closed.
func (cb *CircuitBreaker) Reason() error {
	select {
	case <-cb.tripped:
		return cb.reason
	default:
		return nil
	}
}
