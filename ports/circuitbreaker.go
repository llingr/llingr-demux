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
	// MainCtxDone returns a channel that's closed when the circuit breaker trips.
	// Used to cancel in-flight processing and stop the polling loop.
	// Called once during construction; the returned channel is stored.
	MainCtxDone() <-chan struct{}

	// TriggerEmergencyShutdown initiates protective shutdown.
	// Cancels all in-flight ProcessMessage calls via context cancellation
	// and signals the host application via the Triggered channel.
	// Safe to call multiple times; only the first call has effect (sync.Once).
	TriggerEmergencyShutdown(reason error)

	// Triggered returns a channel that receives the shutdown reason string.
	// The channel is closed after the reason is sent.
	// Useful for tests to verify circuit breaker was triggered and why.
	Triggered() <-chan string
}
