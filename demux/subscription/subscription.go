// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package subscription manages the broker connection lifecycle: subscribing, polling,
// rebalance handling, and coordinated shutdown.
//
// The [Subscription] runs a polling loop that fetches messages and forwards them to the
// pipeline processor. It handles partition rebalances by coordinating drain operations -
// ensuring all in-flight messages complete and offsets commit before acknowledging
// the rebalance to the broker.
//
// Rebalance handling distinguishes between synchronous callbacks (where polling is already
// paused) and asynchronous events (where the loop must be explicitly paused). This enables
// fast rebalances: the time to commit after the last message is microseconds of internal
// work plus broker round-trip.
//
// On assign, the subscription resets the committer's CommittedPlusOne from broker state
// and marks partitions as owned. On revoke, it drains workers, commits pending offsets,
// then marks partitions as unowned - closing race windows where orphaned work items from
// drain timeout could corrupt committed positions.
package subscription

import (
	"context"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Subscription is responsible for the primary link with the Message Broker, polling messages
// and forwarding these into the pipeline.Processor.  It also handles partition rebalances,
// coordinating commit drains before assign/revoke completion and (in a similar process)
// controls shutdown and unsubscribe.
type Subscription[T any] struct {
	mainCtxDone           <-chan struct{} // main circuit-breaker
	poll                  nexus.Poll[T]
	processor             ports.ProcessorPort[T]
	pausePollingTimeout   time.Duration
	signalAssigned        chan struct{} // for startup
	pausePolling          chan struct{} // for partition rebalance
	resumePolling         chan struct{} // after partition rebalance
	stopPolling           chan struct{} // for shutdown
	drainCoordinator      ports.DrainCoordinatorPort
	logger                nexus.Logger
	circuitBreaker        ports.CircuitBreakerPort
	subscribe             nexus.Subscribe
	unsubscribe           nexus.Unsubscribe
	ackRebalance          func(nexus.RebalanceType, []nexus.RebalanceInfo) error
	brokerQuery           func(nexus.QueryRequest) (nexus.QueryResponse, error)
	resetCommittedOffsets func(map[int32]int64) // updates committer on assign
	markPartitionAssigned func(int32)           // tracks partition ownership for orphaned WorkItem protection
	markPartitionRevoked  func(int32)           // tracks partition ownership for orphaned WorkItem protection
	drainTimeout          time.Duration         // during shutdown
	topicName             string                // for logging, updated by Subscribe
	ctx                   context.Context       // global ctrl-plane context
}

// New a Subscription which polls Kafka messages forwarding to
// the pipeline.Processor for de-multiplexing and callback
func New[T any](ctx context.Context, demuxConfig config.DemuxConfig,
	circuitBreaker ports.CircuitBreakerPort, processor ports.ProcessorPort[T],
	poll nexus.Poll[T], subscribe nexus.Subscribe, unsubscribe nexus.Unsubscribe,
	ackRebalance func(nexus.RebalanceType, []nexus.RebalanceInfo) error,
	brokerQuery func(nexus.QueryRequest) (nexus.QueryResponse, error),
	drainCoordinator ports.DrainCoordinatorPort, resetCommittedOffsets func(map[int32]int64),
	markPartitionAssigned func(int32), markPartitionRevoked func(int32),
	topicName string, logger nexus.Logger) *Subscription[T] {

	return &Subscription[T]{
		mainCtxDone:           circuitBreaker.MainCtxDone(), // cancels all processing for exit
		processor:             processor,
		poll:                  poll,
		pausePollingTimeout:   demuxConfig.RebalancePausePollingTimeout,
		signalAssigned:        make(chan struct{}, 1), // buffered avoids (rare) startup timing issue
		pausePolling:          make(chan struct{}),    // for partition rebalance
		resumePolling:         make(chan struct{}),    // after partition rebalance
		stopPolling:           make(chan struct{}),    // for shutdown
		drainCoordinator:      drainCoordinator,
		logger:                logger,
		circuitBreaker:        circuitBreaker,
		subscribe:             subscribe,
		unsubscribe:           unsubscribe,
		ackRebalance:          ackRebalance,
		brokerQuery:           brokerQuery,
		resetCommittedOffsets: resetCommittedOffsets,
		markPartitionAssigned: markPartitionAssigned,
		markPartitionRevoked:  markPartitionRevoked,
		drainTimeout:          demuxConfig.DrainTimeout,
		topicName:             topicName,
		ctx:                   ctx, // global ctrl-plane context
	}
}

// Subscribe called by the demux.DemuxConsumer to register
// the new client with the broker.  This method does not start
// the polling loop - the demux.DemuxConsumer will call this.
// Topic name is set at construction time via New().
func (s *Subscription[T]) Subscribe() error {
	return s.subscribe()
}

// AwaitAssigned signals pipeline processing can start
func (s *Subscription[T]) AwaitAssigned() <-chan struct{} {
	return s.signalAssigned
}

// Unsubscribe signals polling loop should exit, then drains the processing pipeline
func (s *Subscription[T]) Unsubscribe() <-chan error {
	ch := make(chan error, 1)
	go func() {
		defer func() {
			ch <- s.unsubscribe()
			close(ch)
		}()

		s.drain(ShutdownStopPollingBeforeDrain)
	}()

	return ch
}
