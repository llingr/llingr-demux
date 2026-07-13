// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package demux provides a high-throughput message consumer that decouples partition count
// from parallelism, eliminating head-of-line blocking through per-key worker demultiplexing.
// Traditional consumers tie parallelism to partition count, where increasing throughput requires
// more partitions - this leads to larger broker clusters and operational complexity (i.e. more
// cost).
//
// Processing model:
//
//	subscription.Poll → pipeline.Demux (fan-out) → Workers → offset.Committer (fan-in) → Commit
//
// Key guarantees:
//
//	Per-key ordering, at-least-once delivery, zero message loss, graceful rebalances, including
//	during shutdown. A circuit breaker can be triggered if there are significant processing
//	issues: the philosophy is to hand off to other consumers, neither dropping messages nor
//	locking up indefinitely.
//
// The entry point is [NewBuilder], which creates a [ConsumerBuilder] for fluent configuration.
// Pass the builder to a broker adapter (e.g., llingr-adapters-kafka) which calls Build
// to wire together the pipeline components and returns a [Consumer].
package demux

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-demux/demux/bandwidth"
	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-demux/demux/offset"
	"github.com/llingr/llingr-demux/demux/pipeline"
	"github.com/llingr/llingr-demux/demux/subscription"
	"github.com/llingr/llingr-nexus/nexus"
)

// Consumer orchestrates the message flow:
// subscription.PollAndForward → pipeline.SendToWorkerForProcessing → offset.CollectAndCommit
//
// Its processing model removes head-of-line blocking and improves broker efficiency,
// see README.md for an overview
//
// Implements nexus.AdaptedConsumer[T] (which embeds nexus.Consumer[T]).
// The adapter stores this internally for TriggerRebalance, while returning
// the narrower nexus.Consumer[T] interface to the host application.
type Consumer[T any] struct {
	ctx                 context.Context                        // for control plane (only)
	subscription        *subscription.Subscription[T]          // broker POLL loop -> *pipeline.Processor
	pipelineProcessor   *pipeline.Processor[T]                 // *nexus.WorkItem -> nexus.ProcessMessage -> offset.Committer
	offsetCommitter     *offset.Committer[T]                   // *nexus.Message Offset -> Broker COMMIT
	metricsCollector    *metrics.Collector[T]                  // *nexus.Metrics -> sink (e.g. Prometheus)
	bandwidthAggregator *bandwidth.Aggregator                  // bandwidth telemetry side-channel (nil when unconfigured)
	demuxConfig         *config.DemuxConfig                    // defaults are sensible, but overridable
	circuitBreaker      *circuitbreaker.CircuitBreaker         // emergency shutdown coordinator
	shutdownCallback    atomic.Pointer[nexus.ShutdownCallback] // called on exit (graceful or emergency)
	recorder            *snapshot.Recorder                     // point-in-time engine state snapshots
	topicName           string                                 // for logging
	logger              nexus.Logger                           // operational logging, exposed to adapters via Logger()
	stopRateLimit       func()                                 // stops rate limiter on shutdown (no-op if unconfigured)
}

const (
	failedToSubscribe         = "failed to subscribe to topic: %s - %w"
	startingPollingLoop       = "starting %T poll loop for topic: %s"
	startedConsuming          = "started %T for topic: %s"
	awaitAssignmentTimeout    = "timeout after %s waiting for partition assignments, unsubscribing"
	unsubscribeDrainTimeout   = "timeout: %s exceeded draining prior to unsubscribe, triggering circuit breaker"
	defaultCallbackRegistered = "registering defaultShutdownCallback (os.Interrupt after %s) for topicName: %s - " +
		"use RegisterShutdownCallback() for more control"
)

// Subscribe to a topic, start the polling loop, and await assigned.
// Topic name is provided at construction time via the adapter.
func (dxc *Consumer[T]) Subscribe() error {
	topicName := dxc.topicName

	// register default shutdown callback if none provided
	defaultCallback := nexus.ShutdownCallback(dxc.defaultShutdownCallback)
	if dxc.shutdownCallback.CompareAndSwap(nil, &defaultCallback) {
		dxc.logger.Info(dxc.ctx, fmt.Sprintf(defaultCallbackRegistered, shutdownDelay, topicName))
	}

	// start collecting metrics (SinkContext set at Build() time)
	dxc.metricsCollector.StartCollectingMetrics()

	// start bandwidth telemetry aggregator if configured
	if dxc.bandwidthAggregator != nil {
		dxc.bandwidthAggregator.Start()
	}

	// subscription polls for messages and coordinates rebalances and shutdowns.
	if err := dxc.subscription.Subscribe(); err != nil {
		return fmt.Errorf(failedToSubscribe, topicName, err)
	}

	dxc.logger.Info(dxc.ctx, fmt.Sprintf(startingPollingLoop, dxc, topicName))
	go dxc.subscription.PollAndForward(dxc.demuxConfig.PollTimeout)

	select {
	case <-dxc.subscription.AwaitAssigned():
		dxc.logger.Info(dxc.ctx, fmt.Sprintf(startedConsuming, dxc, topicName))

		// listen for circuit breaker and invoke shutdown callback
		go func() {
			if reason := <-dxc.circuitBreaker.Triggered(); reason != "" {
				shutdownCallback := *dxc.shutdownCallback.Load()
				shutdownCallback(dxc.ctx, errors.New(reason))
			}
		}()

	case <-time.After(dxc.demuxConfig.AwaitAssignmentsTimeout):
		timeoutDuration := dxc.demuxConfig.AwaitAssignmentsTimeout
		dxc.logger.Error(dxc.ctx, fmt.Sprintf(awaitAssignmentTimeout, timeoutDuration))

		select {
		case <-dxc.subscription.Unsubscribe():
			// no-op (clean unsubscribe)

		case <-time.After(timeoutDuration):
			// unsubscribe stuck, force terminate
			dxc.defaultShutdownCallback(dxc.ctx, errors.New("unsubscribe timeout during startup"))
		}
		return fmt.Errorf(awaitAssignmentTimeout, timeoutDuration)
	}

	return nil
}

// Unsubscribe stops and drains the pipeline, then calls unsubscribes from broker.
// If workers drain timeout is exceeded, this triggers circuit-breaker emergency shutdown.
func (dxc *Consumer[T]) Unsubscribe() error {
	select {
	case err := <-dxc.subscription.Unsubscribe():
		// Normal shutdown completed
		if err != nil {
			dxc.logger.Warn(dxc.ctx, fmt.Sprintf("broker unsubscribe error: %v", err))
		}
		return nil
	case <-time.After(dxc.demuxConfig.DrainTimeout):
		// DrainWorkers timeout exceeded - trigger emergency shutdown
		timeoutMessage := fmt.Sprintf(unsubscribeDrainTimeout, dxc.demuxConfig.DrainTimeout.String())
		dxc.logger.Error(dxc.ctx, timeoutMessage)
		dxc.circuitBreaker.TriggerEmergencyShutdown(errors.New(timeoutMessage))
		return errors.New(timeoutMessage)
	}
}

// TriggerRebalance handles partition assignment/revocation from the broker.
//
//nolint:wrapcheck // adapter-facing API delegates to subscription - errors include ack failures, unsupported types etc.
func (dxc *Consumer[T]) TriggerRebalance(rebalanceType nexus.RebalanceType, rebalanceInfo []nexus.RebalanceInfo) error {
	return dxc.subscription.HandleRebalance(rebalanceType, rebalanceInfo)
}

// TriggerEmergencyShutdown provides external access to circuit breaker
// for host applications that detect infrastructure failures
func (dxc *Consumer[T]) TriggerEmergencyShutdown(reason error) {
	dxc.circuitBreaker.TriggerEmergencyShutdown(reason)
}

// MetricsStats returns a point-in-time view of metrics collection counters.
func (dxc *Consumer[T]) MetricsStats() metrics.Stats {
	return dxc.metricsCollector.Stats()
}

// Context returns the control-plane context for the consumer lifecycle.
// Adapters use this for broker API calls and as the base context
// injected into each message's Envelope.Ctx.
func (dxc *Consumer[T]) Context() context.Context {
	return dxc.ctx
}

// Logger returns the operational logger for adapter-level logging.
func (dxc *Consumer[T]) Logger() nexus.Logger {
	return dxc.logger
}

// TakeSnapshot returns a point-in-time view of the consumer's state.
// Safe to call from any goroutine.
func (dxc *Consumer[T]) TakeSnapshot() snapshot.Snapshot {
	return dxc.recorder.TakeSnapshot()
}

// SnapshotHandler returns an http.HandlerFunc that serves a JSON snapshot.
// Attach to any router or framework that accepts http.HandlerFunc.
func (dxc *Consumer[T]) SnapshotHandler() http.HandlerFunc {
	return snapshot.NewHandler(dxc.TakeSnapshot)
}
