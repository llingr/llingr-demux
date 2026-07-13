// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package drain orchestrates graceful pipeline shutdown during rebalances and consumer exit.
//
// The [Coordinator] ensures zero message loss by waiting for all in-flight workers to
// complete processing, then draining the committer's pending offsets before acknowledging
// the rebalance. This achieves exactly-once semantics in healthy systems.
//
// Drain timeout (default 20s) bounds the wait. If workers don't complete in time - typically
// due to slow external systems - the circuit breaker triggers, allowing the broker to
// reassign partitions to other consumers rather than blocking the entire consumer group.
package drain

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/offset"
	"github.com/llingr/llingr-demux/demux/pipeline"
	"github.com/llingr/llingr-nexus/nexus"
)

// workerDrainer drains in-flight messages from workers.
type workerDrainer interface {
	DrainWorkers()
}

// offsetDrainer drains and commits pending offsets.
type offsetDrainer interface {
	DrainCommitter(timeoutTimer *time.Timer) error
	CommitOffsets() error
}

// emergencyShutdown triggers circuit breaker on fatal errors.
type emergencyShutdown interface {
	TriggerEmergencyShutdown(reason error)
}

// Coordinator ensures zero message loss during rebalancing
// by draining in-flight work from workers and committing
// offsets before partition handoff.
//
// Because this waits for all workers to complete processing
// and all offsets are committed before acknowledging the rebalance,
// exactly-once semantics in a healthy system is maintained.
type Coordinator[T any] struct {
	ctx            context.Context
	demux          workerDrainer
	committer      offsetDrainer
	circuitBreaker emergencyShutdown
	drainTimeout   time.Duration
	logger         nexus.Logger
	mu             sync.Mutex // serializes Drain: a revoke's drain can overlap a shutdown's
}

// NewDrainCoordinator creates a coordinator for graceful pipeline shutdown.
func NewDrainCoordinator[T any](
	ctx context.Context,
	demux *pipeline.Demux[T],
	committer *offset.Committer[T],
	circuitBreaker *circuitbreaker.CircuitBreaker,
	demuxConfig config.DemuxConfig,
	logger nexus.Logger) *Coordinator[T] {

	return &Coordinator[T]{
		demux:          demux,
		committer:      committer,
		circuitBreaker: circuitBreaker,
		drainTimeout:   demuxConfig.DrainTimeout,
		logger:         logger,
	}
}

// Drain waits for all in-flight messages to complete and commit.
//
// Serialized: the revoke and shutdown paths can both drain concurrently
// (adapter callback goroutine vs app goroutine). Overlapping drains would
// double-drain workers and race for the committer's single drained token,
// turning a graceful stop into a spurious drain timeout. A queued drain
// acquires the lock before its timer starts, keeping its full budget.
func (c *Coordinator[T]) Drain() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	timeoutTimer := time.NewTimer(c.drainTimeout)
	defer func() {
		timeoutTimer.Stop()
		if err != nil {
			// emergency shutdown sets offsets
			// up to whatever can be committed
			c.circuitBreaker.TriggerEmergencyShutdown(err)
		}
	}()

	// pipelined messages in workers
	err = c.drainWorkers(timeoutTimer)
	if err != nil {
		return
	}

	// commit any outstanding offsets
	err = c.drainCommitter(timeoutTimer)
	if err != nil {
		return
	}

	return
}

// drainWorkers waits for all pipelined
// messages to complete processing
func (c *Coordinator[T]) drainWorkers(timeoutTimer *time.Timer) error {
	const (
		drainingWorkers        = "draining messages from partitions, timeout: %s"
		drainedWorkers         = "took: %s to drain workers"
		timeoutDrainingWorkers = "timeout after %s draining workers"
	)

	c.logger.Info(c.ctx, fmt.Sprintf(drainingWorkers, c.drainTimeout))

	startTime := time.Now()
	drainDone := make(chan struct{}, 1)
	go func() {
		defer close(drainDone)
		c.demux.DrainWorkers()
	}()

	select {
	case <-drainDone:
		took := time.Since(startTime).Truncate(time.Microsecond)
		c.logger.Info(c.ctx, fmt.Sprintf(drainedWorkers, took))
		return nil

	case <-timeoutTimer.C:
		took := time.Since(startTime).Truncate(time.Microsecond)
		timeoutError := fmt.Sprintf(timeoutDrainingWorkers, took)
		c.logger.Error(c.ctx, timeoutError)
		return errors.New(timeoutError)
	}
}

// drainCommitter waits for the pre-commit
// buffers to clear when wall offsets have
// been sent to the broker.
//
//nolint:wrapcheck // private shutdown helper - DrainCommitter returns descriptive timeout/context errors
func (c *Coordinator[T]) drainCommitter(timeoutTimer *time.Timer) error {
	err := c.committer.DrainCommitter(timeoutTimer)
	if err != nil {
		c.logger.Error(c.ctx, fmt.Sprintf("failed to drain committer - %v", err))
		return err
	}
	return nil
}

// ImmediateCommit forces an immediate offset commit outside the normal tick cycle.
//
//nolint:wrapcheck // single delegation to CommitOffsets - broker errors include partition and offset context
func (c *Coordinator[T]) ImmediateCommit() error {
	return c.committer.CommitOffsets()
}
