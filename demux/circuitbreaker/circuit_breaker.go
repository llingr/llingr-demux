// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

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
	"fmt"
	"sync"

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
	once              sync.Once
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
		once:              sync.Once{},
		ctx:               globalCtx, // must not be cancelled
		logger:            logger,
		emergencyShutdown: make(chan string, 1),
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
// failures, mitigate resource exhaustion and protect overall system stability
func (cb *CircuitBreaker) TriggerEmergencyShutdown(reason error) {
	cb.once.Do(func() {
		cb.logger.Error(cb.ctx, fmt.Sprintf(shutdownInitiated, reason))

		// stop polling loop and prevent new messages from being processed
		cb.mainCancelFunc()
		cb.logger.Warn(cb.ctx, contextsCancelled)

		// application must register callback,
		// simpler implementations might use os.Exit(1)
		cb.emergencyShutdown <- fmt.Sprintf(signalMessage, reason)
		close(cb.emergencyShutdown)
		cb.logger.Info(cb.ctx, shutdownComplete)
	})
}

// Triggered callback channel advises if
// and why the circuit-breaker was closed
func (cb *CircuitBreaker) Triggered() <-chan string {
	return cb.emergencyShutdown
}
