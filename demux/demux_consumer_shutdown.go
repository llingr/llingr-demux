// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package demux

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// defaultShutdownDelay before sending interrupt - package var allows test override
const defaultShutdownDelay = 15 * time.Second

var shutdownDelay = defaultShutdownDelay // long enough for majority of log shippers

const (
	shutdownInvoked           = "shutdown invoked for topicName: %s, draining workers and committing offsets"
	gracefulShutdownComplete  = "shutdown complete, topicName: %s"
	noCallbackRegistered      = "no shutdown callback was registered for consumer on topicName: %s, sending interrupt in %s"
	emergencyShutdownReason   = "emergency shutdown: %v"
	sendingInterrupt          = "sending interrupt signal to self"
	callbackAlreadyRegistered = "shutdown callback already registered for topicName: %s (%T), " +
		"overwriting - call RegisterShutdownCallback before Subscribe to avoid race conditions"
)

// RegisterShutdownCallback sets a custom handler for shutdown events.
// If not called, a default handler is used which:
//   - graceful (reason nil): logs completion
//   - emergency (reason non-nil): waits 15s for log agents, then sends os.Interrupt
func (dxc *Consumer[T]) RegisterShutdownCallback(callback nexus.ShutdownCallback) {
	if !dxc.shutdownCallback.CompareAndSwap(nil, &callback) {
		// already set - overwrite but warn
		old := dxc.shutdownCallback.Swap(&callback)
		dxc.logger.Warn(dxc.ctx, fmt.Sprintf(callbackAlreadyRegistered, dxc.topicName, old))
	}
}

// Shutdown initiates graceful shutdown - delegating to subscription.Unsubscribe
// which stops polling, drains workers, commits offsets, unsubscribes from broker.
// Blocks until complete. Uses the context provided to the builder via WithContext().
func (dxc *Consumer[T]) Shutdown() error {
	dxc.logger.Info(dxc.ctx, fmt.Sprintf(shutdownInvoked, dxc.topicName))

	dxc.stopRateLimit()

	callback := dxc.shutdownCallback.Load()
	if callback == nil {
		dxc.logger.Warn(dxc.ctx, "never subscribed to topic, shutdown complete!")
		return nil
	}

	dxc.logger.Info(dxc.ctx, "calling Unsubscribe()")

	// Only invoke callback if clean shutdown.
	// If err != nil, circuit-breaker triggered → listener handles callback.
	if err := dxc.Unsubscribe(); err != nil {
		return err
	}

	// final stats packet drain
	if dxc.bandwidthAggregator != nil {
		dxc.bandwidthAggregator.Stop()
	}

	// deliver last buffered work items
	dxc.metricsCollector.Stop()

	dxc.logger.Info(dxc.ctx, fmt.Sprintf("invoking shutdownCallback for topicName: %s", dxc.topicName))
	(*callback)(dxc.ctx, nil) // nil reason = graceful
	return nil
}

// defaultShutdownCallback is used when no callback is registered.
// For graceful shutdown (reason nil): logs completion.
// For emergency shutdown (reason non-nil): waits 15s for log agents, then sends interrupt.
func (dxc *Consumer[T]) defaultShutdownCallback(_ context.Context, reason error) {
	if reason == nil {
		dxc.logger.Info(dxc.ctx, fmt.Sprintf(gracefulShutdownComplete, dxc.topicName))
		return
	}

	dxc.logger.Warn(dxc.ctx, fmt.Sprintf(noCallbackRegistered, dxc.topicName, shutdownDelay))
	dxc.logger.Warn(dxc.ctx, fmt.Sprintf(emergencyShutdownReason, reason))

	time.Sleep(shutdownDelay)

	dxc.logger.Info(dxc.ctx, sendingInterrupt)
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		errS := p.Signal(os.Interrupt)
		if errS != nil {
			const interruptFailedMessage = "interrupt call failed - %v (topicName: %s), calling os.Exit(1)"
			dxc.logger.Error(dxc.ctx, fmt.Sprintf(interruptFailedMessage, errS, dxc.topicName))
			time.Sleep(shutdownDelay)
			os.Exit(1)
		}
	} else {
		const interruptFailedMessage = "os.FindProcess failed - %v (topicName: %s), calling os.Exit(1)"
		dxc.logger.Error(dxc.ctx, fmt.Sprintf(interruptFailedMessage, err, dxc.topicName))
		time.Sleep(shutdownDelay)
		os.Exit(1)
	}
}
