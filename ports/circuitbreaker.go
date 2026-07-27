// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package ports

// CircuitBreakerPort abstracts circuitbreaker.CircuitBreaker for testability.
//
// Satisfied by: *circuitbreaker.CircuitBreaker
//
// Coordinates emergency shutdown when infrastructure or application errors
// threaten system reliability. All methods are cold path (error/shutdown only).
type CircuitBreakerPort interface {
	// MainCtxDone returns a channel that is closed when the circuit breaker
	// trips. It is used to stop the polling loop and message dispatch prior
	// to exit.
	MainCtxDone() <-chan struct{}

	// TriggerEmergencyShutdown cancels the main context, stops polling
	// and dispatch, then signals Triggered. In-flight ProcessMessage calls
	// may run to completion. Must be idempotent and re-entrant: never block.
	TriggerEmergencyShutdown(reason error)

	// Triggered returns a channel that receives the shutdown reason string.
	// The channel is closed after the reason is sent.
	// Useful for tests to verify circuit breaker was triggered and why.
	Triggered() <-chan string
}
